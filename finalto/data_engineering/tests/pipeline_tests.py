"""
pipeline_tests.py
────────────────────────────────────────────────────────────────
Yelp Data Platform — Pipeline Data Quality Test Suite
Purpose : Standalone tests that run AFTER each pipeline layer
          to assert data completeness, freshness, and integrity.
Trigger : Airflow BashOperator or PythonOperator after each DAG layer
          dag_bronze → [pipeline_tests.py --layer bronze]
          dag_silver → [pipeline_tests.py --layer silver]

These tests are separate from dbt tests (which cover Gold).
Bronze and Silver are PySpark territory — these Python tests cover them.

Run manually:
  spark-submit pipeline_tests.py --layer bronze --env prod
  spark-submit pipeline_tests.py --layer silver --env prod --date 2026-04-26

Exit codes:
  0 — all tests passed
  1 — one or more tests failed (Airflow marks task as FAILED)
────────────────────────────────────────────────────────────────
"""

import argparse
import logging
import sys
from datetime import date, datetime, timedelta
from dataclasses import dataclass, field
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

# ─── Logging ─────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("pipeline_tests")

# ─── S3 Paths ────────────────────────────────────────────────────
PATHS = {
    "dev": {
        "bronze": "s3://yelp-platform-dev/bronze",
        "silver": "s3://yelp-platform-dev/silver",
        "audit":  "s3://yelp-platform-dev/audit/pipeline_audit",
        "test_results": "s3://yelp-platform-dev/audit/test_results",
    },
    "prod": {
        "bronze": "s3://yelp-platform/bronze",
        "silver": "s3://yelp-platform/silver",
        "audit":  "s3://yelp-platform/audit/pipeline_audit",
        "test_results": "s3://yelp-platform/audit/test_results",
    },
}

# ─── Test Configuration ───────────────────────────────────────────

# Minimum expected row counts per layer per entity
MIN_ROW_COUNTS = {
    "bronze": {
        "business": 100_000,
        "reviews":  500_000,
        "users":    100_000,
        "checkins":  50_000,
        "tips":      50_000,
    },
    "silver": {
        "business":           100_000,
        "reviews":            500_000,
        "users":              100_000,
        "checkin_events":     200_000,   # exploded — more rows than Bronze
        "tips":                50_000,
        "business_categories": 200_000,  # exploded — more rows than Bronze business
    },
}

# Freshness thresholds per layer (hours)
# How old can the latest record be before we flag it as stale?
FRESHNESS_THRESHOLDS = {
    "bronze": 26,   # daily pipeline + 2h buffer
    "silver": 28,   # runs after Bronze so slightly later threshold
}

# Critical columns that must have zero nulls
ZERO_NULL_COLUMNS = {
    "bronze": {
        "business": ["business_id"],
        "reviews":  ["review_id", "business_id", "user_id"],
        "users":    ["user_id"],
        "checkins": ["business_id"],
        "tips":     ["business_id", "user_id"],
    },
    "silver": {
        "business":           ["business_id"],
        "reviews":            ["review_id", "business_id", "user_id", "review_date"],
        "users":              ["user_id", "is_elite"],
        "checkin_events":     ["checkin_id", "business_id", "checkin_ts"],
        "tips":               ["business_id", "user_id"],
        "business_categories":["business_id", "category"],
    },
}

# Date columns for freshness checks (entity → column name)
FRESHNESS_COLS = {
    "bronze": {
        "reviews": "date",
        "users":   "yelping_since",
        "tips":    "date",
    },
    "silver": {
        "reviews":        "review_date",
        "users":          "yelping_since",
        "tips":           "tip_date",
        "checkin_events": "checkin_ts",
    },
}

# Date formats for parsing freshness columns
DATE_FORMATS = {
    "date":         "yyyy-MM-dd HH:mm:ss",
    "yelping_since":"yyyy-MM-dd HH:mm:ss",
    "review_date":  "yyyy-MM-dd HH:mm:ss",
    "tip_date":     "yyyy-MM-dd HH:mm:ss",
    "checkin_ts":   None,   # already a TimestampType in Silver
}


# ─── Test Result ──────────────────────────────────────────────────

@dataclass
class TestResult:
    test_name:  str
    entity:     str
    layer:      str
    passed:     bool
    message:    str
    value:      Optional[float] = None
    threshold:  Optional[float] = None


# ─── Spark Session ────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("yelp-pipeline-tests")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


# ─── Individual Test Functions ────────────────────────────────────

def test_row_count_minimum(
    df: DataFrame,
    entity: str,
    layer: str,
    minimum: int,
) -> TestResult:
    """
    Assert row count is above the minimum expected threshold.
    A count below minimum usually means the source was truncated,
    the path is wrong, or a filter was over-applied.
    """
    count = df.count()
    passed = count >= minimum
    return TestResult(
        test_name  = "row_count_minimum",
        entity     = entity,
        layer      = layer,
        passed     = passed,
        message    = (
            f"Row count {count:,} >= minimum {minimum:,} ✓"
            if passed else
            f"FAIL — Row count {count:,} is below minimum {minimum:,}"
        ),
        value     = float(count),
        threshold = float(minimum),
    )


def test_zero_nulls(
    df: DataFrame,
    entity: str,
    layer: str,
    col_name: str,
) -> TestResult:
    """
    Assert a critical column has zero null values.
    Primary keys and foreign keys must never be null.
    """
    if col_name not in df.columns:
        return TestResult(
            test_name = "zero_nulls",
            entity    = entity,
            layer     = layer,
            passed    = True,
            message   = f"Column '{col_name}' not in DataFrame — skipped",
        )

    null_count = df.filter(F.col(col_name).isNull()).count()
    passed     = null_count == 0
    return TestResult(
        test_name = "zero_nulls",
        entity    = entity,
        layer     = layer,
        passed    = passed,
        message   = (
            f"Column '{col_name}': zero nulls ✓"
            if passed else
            f"FAIL — Column '{col_name}': {null_count:,} null values found"
        ),
        value     = float(null_count),
        threshold = 0.0,
    )


def test_no_duplicate_keys(
    df: DataFrame,
    entity: str,
    layer: str,
    key_col: str,
) -> TestResult:
    """
    Assert no duplicate values exist on the natural key column.
    Duplicate PKs mean deduplication failed or the source has issues.
    """
    if key_col not in df.columns:
        return TestResult(
            test_name = "no_duplicate_keys",
            entity    = entity,
            layer     = layer,
            passed    = True,
            message   = f"Key column '{key_col}' not found — skipped",
        )

    total    = df.count()
    distinct = df.select(key_col).distinct().count()
    dupe_count = total - distinct
    passed     = dupe_count == 0

    return TestResult(
        test_name = "no_duplicate_keys",
        entity    = entity,
        layer     = layer,
        passed    = passed,
        message   = (
            f"Key '{key_col}': no duplicates ✓"
            if passed else
            f"FAIL — Key '{key_col}': {dupe_count:,} duplicate rows found"
        ),
        value     = float(dupe_count),
        threshold = 0.0,
    )


def test_star_rating_range(
    df: DataFrame,
    entity: str,
    layer: str,
) -> TestResult:
    """
    Assert all star ratings are within the valid range (1–5).
    Out-of-range stars indicate a schema mismatch or data corruption.
    Only applies to reviews entity.
    """
    stars_col = "stars" if layer == "bronze" else "stars"
    if stars_col not in df.columns:
        return TestResult(
            test_name = "star_rating_range",
            entity    = entity,
            layer     = layer,
            passed    = True,
            message   = f"No stars column — skipped",
        )

    invalid_count = df.filter(
        ~F.col(stars_col).between(1, 5)
    ).count()
    passed = invalid_count == 0

    return TestResult(
        test_name = "star_rating_range",
        entity    = entity,
        layer     = layer,
        passed    = passed,
        message   = (
            "Star ratings all within 1–5 ✓"
            if passed else
            f"FAIL — {invalid_count:,} rows with stars outside 1–5 range"
        ),
        value     = float(invalid_count),
        threshold = 0.0,
    )


def test_freshness(
    df: DataFrame,
    entity: str,
    layer: str,
    date_col: str,
    date_format: Optional[str],
    threshold_hours: int,
) -> TestResult:
    """
    Assert the latest record in the table is not older than
    threshold_hours. Stale data means the source did not update
    or the pipeline processed the wrong date range.

    This is one of the most important tests — silently stale data
    in a BI dashboard is worse than a pipeline failure because
    stakeholders may act on outdated numbers.
    """
    if date_col not in df.columns:
        return TestResult(
            test_name = "freshness",
            entity    = entity,
            layer     = layer,
            passed    = True,
            message   = f"No date column '{date_col}' — skipped",
        )

    if date_format:
        ts_col = F.to_timestamp(F.col(date_col), date_format)
    else:
        ts_col = F.col(date_col)   # already a timestamp

    latest_ts = (
        df.select(ts_col.alias("ts"))
        .agg(F.max("ts").alias("latest"))
        .collect()[0]["latest"]
    )

    if latest_ts is None:
        return TestResult(
            test_name = "freshness",
            entity    = entity,
            layer     = layer,
            passed    = False,
            message   = f"FAIL — Could not determine latest timestamp in '{date_col}'",
        )

    age_hours = (datetime.utcnow() - latest_ts).total_seconds() / 3600
    passed    = age_hours <= threshold_hours

    return TestResult(
        test_name = "freshness",
        entity    = entity,
        layer     = layer,
        passed    = passed,
        message   = (
            f"Freshness OK — latest record {age_hours:.1f}h old "
            f"(threshold: {threshold_hours}h) ✓"
            if passed else
            f"FAIL — Latest record is {age_hours:.1f}h old "
            f"(threshold: {threshold_hours}h). Data may be stale."
        ),
        value     = round(age_hours, 2),
        threshold = float(threshold_hours),
    )


def test_lookback_partitions_present(
    spark: SparkSession,
    table_path: str,
    entity: str,
    layer: str,
    lookback_dates: list[str],
) -> TestResult:
    """
    Assert that every date in the lookback window has a partition
    present in the Delta table. A missing partition means the ingestion
    job failed silently for that date — late-arriving data would be lost.
    """
    try:
        df = spark.read.format("delta").load(table_path)

        if "ingestion_date" not in df.columns:
            return TestResult(
                test_name = "lookback_partitions_present",
                entity    = entity,
                layer     = layer,
                passed    = True,
                message   = "No ingestion_date partition column — skipped",
            )

        present_dates = set(
            row["ingestion_date"]
            for row in df.select("ingestion_date").distinct().collect()
        )

        missing = [d for d in lookback_dates if d not in present_dates]
        passed  = len(missing) == 0

        return TestResult(
            test_name = "lookback_partitions_present",
            entity    = entity,
            layer     = layer,
            passed    = passed,
            message   = (
                f"All lookback partitions present: {lookback_dates} ✓"
                if passed else
                f"FAIL — Missing partitions for dates: {missing}. "
                f"Late-arriving data may have been lost."
            ),
        )

    except Exception as e:
        return TestResult(
            test_name = "lookback_partitions_present",
            entity    = entity,
            layer     = layer,
            passed    = False,
            message   = f"FAIL — Could not read table at {table_path}: {e}",
        )


def test_bronze_silver_row_ratio(
    spark: SparkSession,
    bronze_path: str,
    silver_path: str,
    entity: str,
    silver_entity: str,
    max_loss_pct: float = 5.0,
) -> TestResult:
    """
    Assert Silver row count has not lost more than max_loss_pct% of Bronze.
    Only applies to entities where grain is the same (not checkins/categories
    which explode and naturally have more Silver rows than Bronze).
    """
    try:
        bronze_count = spark.read.format("delta").load(f"{bronze_path}/{entity}").count()
        silver_count = spark.read.format("delta").load(f"{silver_path}/{silver_entity}").count()

        loss_pct = (bronze_count - silver_count) / bronze_count * 100 if bronze_count > 0 else 0
        passed   = loss_pct <= max_loss_pct

        return TestResult(
            test_name = "bronze_silver_row_ratio",
            entity    = entity,
            layer     = "silver",
            passed    = passed,
            message   = (
                f"Bronze→Silver loss {loss_pct:.1f}% within threshold {max_loss_pct}% ✓"
                if passed else
                f"FAIL — Bronze→Silver row loss {loss_pct:.1f}% exceeds threshold "
                f"{max_loss_pct}%. Bronze: {bronze_count:,}, Silver: {silver_count:,}"
            ),
            value     = round(loss_pct, 2),
            threshold = max_loss_pct,
        )

    except Exception as e:
        return TestResult(
            test_name = "bronze_silver_row_ratio",
            entity    = entity,
            layer     = "silver",
            passed    = False,
            message   = f"FAIL — Could not compute ratio: {e}",
        )


# ─── Test Runner ─────────────────────────────────────────────────

def run_tests_for_layer(
    spark: SparkSession,
    layer: str,
    paths: dict,
    lookback_dates: list[str],
) -> list[TestResult]:
    """
    Run all configured tests for a given layer (bronze or silver).
    Returns a list of TestResult objects.
    """
    results: list[TestResult] = []

    layer_path   = paths[layer]
    entities     = MIN_ROW_COUNTS[layer]
    threshold_h  = FRESHNESS_THRESHOLDS[layer]

    for entity, minimum in entities.items():
        log.info(f"Running tests for [{layer}] [{entity}]")

        # Determine S3 path for this entity
        if layer == "bronze":
            table_path = f"{layer_path}/{entity}"
        else:
            # Silver entities have different folder names in some cases
            table_path = f"{layer_path}/{entity}"

        try:
            df = spark.read.format("delta").load(table_path)
        except Exception as e:
            results.append(TestResult(
                test_name = "table_readable",
                entity    = entity,
                layer     = layer,
                passed    = False,
                message   = f"FAIL — Could not read table at {table_path}: {e}",
            ))
            continue

        # ── Test 1: Minimum row count ──────────────────────────
        results.append(test_row_count_minimum(df, entity, layer, minimum))

        # ── Test 2: Zero nulls on critical columns ─────────────
        for col_name in ZERO_NULL_COLUMNS.get(layer, {}).get(entity, []):
            results.append(test_zero_nulls(df, entity, layer, col_name))

        # ── Test 3: No duplicate keys (PKs only) ───────────────
        pk_map = {
            "business": "business_id",
            "reviews":  "review_id",
            "users":    "user_id",
            "checkin_events": "checkin_id",
        }
        if entity in pk_map:
            results.append(
                test_no_duplicate_keys(df, entity, layer, pk_map[entity])
            )

        # ── Test 4: Star rating range (reviews only) ───────────
        if entity == "reviews":
            results.append(test_star_rating_range(df, entity, layer))

        # ── Test 5: Freshness ──────────────────────────────────
        freshness_col = FRESHNESS_COLS.get(layer, {}).get(entity)
        if freshness_col:
            date_fmt = DATE_FORMATS.get(freshness_col)
            results.append(
                test_freshness(df, entity, layer, freshness_col, date_fmt, threshold_h)
            )

        # ── Test 6: Lookback partitions present (bronze only) ──
        if layer == "bronze":
            results.append(
                test_lookback_partitions_present(
                    spark, table_path, entity, layer, lookback_dates
                )
            )

    # ── Test 7: Bronze → Silver row ratio (silver only) ────────
    if layer == "silver":
        for entity in ["business", "reviews", "users", "tips"]:
            results.append(
                test_bronze_silver_row_ratio(
                    spark,
                    paths["bronze"],
                    paths["silver"],
                    entity,
                    silver_entity=entity,
                )
            )

    return results


# ─── Results Writer ───────────────────────────────────────────────

def write_test_results(
    spark: SparkSession,
    results: list[TestResult],
    test_results_path: str,
) -> None:
    """Write test results to Delta table for trend tracking in BI."""
    rows = [
        {
            "test_name":   r.test_name,
            "entity":      r.entity,
            "layer":       r.layer,
            "passed":      r.passed,
            "message":     r.message,
            "value":       r.value,
            "threshold":   r.threshold,
            "run_date":    str(date.today()),
            "run_timestamp": datetime.utcnow().isoformat(),
        }
        for r in results
    ]

    df = spark.createDataFrame(rows)
    df.write.format("delta").mode("append").save(test_results_path)
    log.info(f"Test results written to {test_results_path}")


# ─── Summary Printer ─────────────────────────────────────────────

def print_summary(results: list[TestResult]) -> bool:
    """Print a test run summary. Returns True if all tests passed."""
    passed = [r for r in results if r.passed]
    failed = [r for r in results if not r.passed]

    log.info("=" * 60)
    log.info(f"TEST SUMMARY: {len(passed)} passed, {len(failed)} failed")
    log.info("=" * 60)

    if failed:
        log.error("FAILED TESTS:")
        for r in failed:
            log.error(f"  [{r.layer}][{r.entity}] {r.test_name}: {r.message}")

    if passed:
        log.info("PASSED TESTS:")
        for r in passed:
            log.info(f"  [{r.layer}][{r.entity}] {r.test_name}: {r.message}")

    log.info("=" * 60)
    return len(failed) == 0


# ─── Entry Point ─────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Yelp Pipeline Data Quality Tests")
    parser.add_argument(
        "--layer",
        choices=["bronze", "silver"],
        required=True,
        help="Which layer to test",
    )
    parser.add_argument(
        "--env",
        choices=["dev", "prod"],
        default="prod",
    )
    parser.add_argument(
        "--date",
        default=str(date.today()),
        help="Run date for lookback window calculation",
    )
    parser.add_argument(
        "--lookback",
        type=int,
        default=3,
        help="Lookback days — must match the ingestion job lookback",
    )
    args = parser.parse_args()

    paths = PATHS[args.env]

    # Compute lookback dates — same logic as ingestion scripts
    end_date   = datetime.strptime(args.date, "%Y-%m-%d").date()
    start_date = end_date - timedelta(days=args.lookback - 1)
    lookback_dates = [
        str(start_date + timedelta(days=i))
        for i in range(args.lookback)
    ]

    log.info(f"Layer          : {args.layer}")
    log.info(f"Environment    : {args.env}")
    log.info(f"Run date       : {args.date}")
    log.info(f"Lookback window: {lookback_dates}")

    spark   = create_spark_session()
    results = run_tests_for_layer(spark, args.layer, paths, lookback_dates)

    write_test_results(spark, results, paths["test_results"])

    all_passed = print_summary(results)

    spark.stop()

    # Non-zero exit code causes Airflow to mark the task as FAILED
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()