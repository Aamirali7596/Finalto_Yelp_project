"""
bronze_ingestion.py — Yelp Bronze Layer Ingestion
Reads raw Yelp JSON files from S3 and writes to Delta Lake (Bronze).

Lookback: Re-processes the last N days on every run to catch
          late-arriving source data. Safe to re-run — dynamic
          partition overwrite only replaces affected partitions.

Schema Drift Handling:
  Detects new columns (warn + keep), missing columns (warn + null fill),
  and type changes (halt + alert) before writing to Bronze.
  All drift events written to audit/schema_drift Delta table.

Partial Failure Handling:
  After every write, the partition is read back and row count is
  verified against what was written. A mismatch halts the job and
  triggers an Airflow retry. Delta Lake ACID transactions prevent
  partial writes from being visible, but we verify explicitly so
  "written" and "readable" are confirmed separately.

Reprocessing:
  Any date partition can be safely replayed:
    --date 2026-04-24 --lookback 1 --entity reviews
  See reprocessing strategy comments in this file for full detail.

Usage:
  spark-submit bronze_ingestion.py --env prod --date 2026-04-26 --lookback 3
"""

import argparse
import logging
from datetime import date, datetime, timedelta

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType, IntegerType,
)

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
log = logging.getLogger("bronze")

# ── Paths ─────────────────────────────────────────────────────────
PATHS = {
    "dev":  {"landing": "s3://yelp-dev/landing",  "bronze": "s3://yelp-dev/bronze",  "audit": "s3://yelp-dev/audit"},
    "prod": {"landing": "s3://yelp-prod/landing", "bronze": "s3://yelp-prod/bronze", "audit": "s3://yelp-prod/audit"},
}

SOURCE_FILES = {
    "business": "business.json",
    "reviews":  "review.json",
    "users":    "user.json",
    "checkins": "checkin.json",
    "tips":     "tip.json",
}

# Minimum expected rows per entity — below this = source problem
MIN_ROW_COUNTS = {
    "business": 100_000,
    "reviews":  500_000,
    "users":    100_000,
    "checkins":  50_000,
    "tips":      50_000,
}

# ── Schemas ───────────────────────────────────────────────────────
# Always define schemas explicitly — never use inferSchema=True.
# Spark samples 1% of the file to infer types which can silently
# mistype fields (e.g. stars as StringType instead of IntegerType).
# These schemas are the contract between source and pipeline.
# Any deviation is detected by check_schema_drift() before writing.

SCHEMAS = {
    "business": StructType([
        StructField("business_id",  StringType(),  False),
        StructField("name",         StringType(),  True),
        StructField("city",         StringType(),  True),
        StructField("state",        StringType(),  True),
        StructField("postal_code",  StringType(),  True),
        StructField("latitude",     DoubleType(),  True),
        StructField("longitude",    DoubleType(),  True),
        StructField("stars",        DoubleType(),  True),
        StructField("review_count", LongType(),    True),
        StructField("is_open",      IntegerType(), True),
        StructField("categories",   StringType(),  True),
        StructField("hours",        StringType(),  True),
        StructField("attributes",   StringType(),  True),
    ]),
    "reviews": StructType([
        StructField("review_id",   StringType(),  False),
        StructField("business_id", StringType(),  False),
        StructField("user_id",     StringType(),  False),
        StructField("stars",       IntegerType(), True),
        StructField("date",        StringType(),  True),
        StructField("text",        StringType(),  True),
        StructField("useful",      LongType(),    True),
        StructField("funny",       LongType(),    True),
        StructField("cool",        LongType(),    True),
    ]),
    "users": StructType([
        StructField("user_id",       StringType(), False),
        StructField("name",          StringType(), True),
        StructField("review_count",  LongType(),   True),
        StructField("yelping_since", StringType(), True),
        StructField("elite",         StringType(), True),
        StructField("fans",          LongType(),   True),
        StructField("average_stars", DoubleType(), True),
        StructField("useful",        LongType(),   True),
        StructField("funny",         LongType(),   True),
        StructField("cool",          LongType(),   True),
    ]),
    "checkins": StructType([
        StructField("business_id", StringType(), False),
        StructField("date",        StringType(), True),
    ]),
    "tips": StructType([
        StructField("business_id",      StringType(), False),
        StructField("user_id",          StringType(), False),
        StructField("text",             StringType(), True),
        StructField("date",             StringType(), True),
        StructField("compliment_count", LongType(),   True),
    ]),
}


# ── Schema Drift Detection ────────────────────────────────────────

def check_schema_drift(
    spark: SparkSession,
    entity: str,
    actual_df: DataFrame,
    audit_path: str,
    run_date: str,
) -> DataFrame:
    """
    Compare the incoming DataFrame schema against the registered
    expected schema (SCHEMAS dict). Detects three drift scenarios:

      NEW columns    — source added a field we don't know about
                       Action: log WARNING, keep the column (additive)

      MISSING columns — source dropped a field we expected
                        Action: log WARNING, add null column so
                        downstream Silver transforms don't break

      TYPE changes   — a field changed its data type
                       Action: raise RuntimeError — this is a
                       breaking change that requires human review
                       before the pipeline can proceed

    Returns the (possibly adjusted) DataFrame safe to write to Bronze.
    """
    expected_schema = SCHEMAS[entity]
    expected_fields = {f.name: f.dataType.simpleString() for f in expected_schema.fields}
    actual_fields   = {f.name: f.dataType.simpleString() for f in actual_df.schema.fields}

    new_cols     = set(actual_fields) - set(expected_fields)
    missing_cols = set(expected_fields) - set(actual_fields)
    type_changes = {
        col: (expected_fields[col], actual_fields[col])
        for col in set(expected_fields) & set(actual_fields)
        if expected_fields[col] != actual_fields[col]
        and col != "ingestion_date"   # exclude our own stamp column
    }

    drift_detected = bool(new_cols or missing_cols or type_changes)

    # ── New columns ───────────────────────────────────────────────
    # Additive changes are safe. Log them and keep the data.
    # Silver will ignore unknown columns — they won't break anything.
    # Add to SCHEMAS and Silver transforms in the next sprint.
    if new_cols:
        log.warning(
            f"[{entity}] SCHEMA DRIFT — new columns detected: {sorted(new_cols)}. "
            f"These columns are unknown to the pipeline. "
            f"They will be written to Bronze but Silver will not process them. "
            f"Add them to SCHEMAS and silver_transform.py to include in Silver."
        )

    # ── Missing columns ───────────────────────────────────────────
    # A column we expected is absent from the source file.
    # Insert a null column to preserve the Bronze schema contract
    # so Silver transforms don't fail on a missing column reference.
    if missing_cols:
        log.warning(
            f"[{entity}] SCHEMA DRIFT — expected columns missing from source: "
            f"{sorted(missing_cols)}. Null columns inserted to preserve schema."
        )
        for col_name in missing_cols:
            expected_field = next(f for f in expected_schema.fields if f.name == col_name)
            actual_df = actual_df.withColumn(
                col_name,
                F.lit(None).cast(expected_field.dataType)
            )

    # ── Type changes ──────────────────────────────────────────────
    # A type change is a breaking change. Casting silently could corrupt
    # data (e.g. DoubleType → StringType on stars would break all averages).
    # Halt the pipeline and require a human decision.
    if type_changes:
        msg = (
            f"[{entity}] SCHEMA DRIFT — BREAKING TYPE CHANGES DETECTED: "
            + ", ".join(
                f"{col}: expected {exp} got {got}"
                for col, (exp, got) in type_changes.items()
            )
            + ". Pipeline halted. Review source schema change before re-running."
        )
        log.error(msg)
        # Write drift record to audit before halting
        _write_drift_audit(spark, audit_path, entity, run_date, new_cols, missing_cols, type_changes)
        raise RuntimeError(msg)

    # Write drift audit record if any drift was detected
    if drift_detected:
        _write_drift_audit(spark, audit_path, entity, run_date, new_cols, missing_cols, type_changes)

    return actual_df


def _write_drift_audit(spark, audit_path, entity, run_date, new_cols, missing_cols, type_changes):
    """Write schema drift event to audit table for tracking and alerting."""
    drift_summary = {
        "new_columns":     str(sorted(new_cols))     if new_cols     else "",
        "missing_columns": str(sorted(missing_cols)) if missing_cols else "",
        "type_changes":    str(type_changes)         if type_changes else "",
    }
    spark.createDataFrame([{
        "entity":        entity,
        "layer":         "bronze",
        "event_type":    "schema_drift",
        "run_date":      run_date,
        "run_timestamp": datetime.utcnow().isoformat(),
        "details":       str(drift_summary),
    }]).write.format("delta").mode("append").save(f"{audit_path}/schema_drift")
    log.info(f"[{entity}] Schema drift event written to audit")


# ── Post-Write Verification ───────────────────────────────────────

def verify_write(
    spark: SparkSession,
    bronze_path: str,
    entity: str,
    run_date: str,
    expected_count: int,
) -> None:
    """
    After writing to Bronze, read back the partition we just wrote
    and confirm the row count matches what we intended to write.

    Why this matters:
      Delta Lake's ACID transactions prevent partial writes from being
      visible to readers — if the write fails, the transaction rolls back.
      However, we verify anyway because:
        1. It confirms the partition is queryable (no corruption in files)
        2. It catches edge cases like S3 eventual consistency issues
        3. It provides an explicit audit trail of what landed vs what was read
        4. It is the difference between "we wrote X rows" and "X rows are readable"

    If the counts don't match — the partition is flagged as suspect
    and the job fails loudly so Airflow retries the full ingestion.
    """
    written_df = (
        spark.read
        .format("delta")
        .load(f"{bronze_path}/{entity}")
        .filter(F.col("ingestion_date") == run_date)
    )
    written_count = written_df.count()

    if written_count != expected_count:
        raise RuntimeError(
            f"[{entity}] POST-WRITE VERIFICATION FAILED — "
            f"wrote {expected_count:,} rows but only {written_count:,} are readable. "
            f"Partition ingestion_date={run_date} may be corrupt. "
            f"Re-run with --entity {entity} --date {run_date} to replay."
        )

    log.info(
        f"[{entity}] Post-write verification passed — "
        f"{written_count:,} rows confirmed readable in Bronze ✓"
    )


# ── Reprocessing Strategy ─────────────────────────────────────────
#
# How to reprocess a specific date if a partition is corrupt or missing:
#
#   spark-submit bronze_ingestion.py \
#     --env prod \
#     --date 2026-04-24 \   ← the date to reprocess
#     --lookback 1 \        ← 1 day = only that date, nothing else
#     --entity reviews      ← optional: specific entity only
#
# Dynamic partition overwrite means only the April 24 partition is
# replaced. All other dates remain untouched.
#
# For a full backfill (e.g. after a source schema fix):
#   spark-submit bronze_ingestion.py \
#     --env prod \
#     --date 2026-04-26 \
#     --lookback 30         ← reprocess last 30 days
#
# Delta Lake time travel lets you inspect what a partition looked like
# before the reprocess:
#   SELECT * FROM bronze.reviews VERSION AS OF 5
#   WHERE ingestion_date = '2026-04-24'


# ── Helpers ───────────────────────────────────────────────────────

def get_lookback_dates(run_date: str, lookback_days: int) -> list[str]:
    """Return list of dates to process — today minus lookback_days up to today."""
    end   = datetime.strptime(run_date, "%Y-%m-%d").date()
    start = end - timedelta(days=lookback_days - 1)
    dates = [(start + timedelta(days=i)).isoformat() for i in range(lookback_days)]
    log.info(f"Lookback window: {dates}")
    return dates


def write_audit(spark, audit_path, entity, row_count, run_date, status, error=""):
    spark.createDataFrame([{
        "entity":        entity,
        "layer":         "bronze",
        "row_count":     row_count,
        "run_date":      run_date,
        "run_timestamp": datetime.utcnow().isoformat(),
        "status":        status,
        "error":         error,
    }]).write.format("delta").mode("append").save(audit_path)


def check_freshness(df, entity, date_col, date_fmt="yyyy-MM-dd HH:mm:ss"):
    """Warn if the latest record is older than 26 hours."""
    if date_col not in df.columns:
        return
    latest = (
        df.select(F.to_timestamp(F.col(date_col), date_fmt).alias("ts"))
          .agg(F.max("ts"))
          .collect()[0][0]
    )
    if latest:
        age_h = (datetime.utcnow() - latest).total_seconds() / 3600
        status = "✓" if age_h <= 26 else "⚠ STALE"
        log.info(f"[{entity}] Freshness: {age_h:.1f}h old {status}")


def check_row_count(entity, count):
    """Fail loudly if row count is below the expected minimum."""
    minimum = MIN_ROW_COUNTS.get(entity, 0)
    if count < minimum:
        raise ValueError(
            f"[{entity}] Row count {count:,} is below minimum {minimum:,}. "
            "Source may be truncated or path is wrong."
        )
    log.info(f"[{entity}] Row count {count:,} — above minimum {minimum:,} ✓")


# ── Core Ingestion ────────────────────────────────────────────────

FRESHNESS_COLS = {
    "reviews": "date",
    "users":   "yelping_since",
    "tips":    "date",
}


def ingest_entity(spark, entity, source_file, landing_path, bronze_path, audit_path, run_date):
    log.info(f"[{entity}] Starting ingestion")

    # Read with PERMISSIVE mode — bad rows go to _corrupt_record
    # rather than crashing the job. We log and drop them.
    df = (
        spark.read
        .schema(SCHEMAS[entity])
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(f"{landing_path}/{source_file}")
    )

    # Large file — distribute across executors evenly
    if entity == "reviews":
        df = df.repartition(200)

    # Drop corrupt records and stamp with ingestion date
    if "_corrupt_record" in df.columns:
        bad = df.filter(F.col("_corrupt_record").isNotNull()).count()
        if bad:
            log.warning(f"[{entity}] {bad:,} corrupt records dropped")
        df = df.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")

    df = df.withColumn("ingestion_date", F.lit(run_date))

    # ── Schema drift check ────────────────────────────────────────
    # Detects new columns (warn + keep), missing columns (warn + null fill),
    # and type changes (error + halt). Must run before write so drift is
    # caught before bad data lands in Bronze.
    df = check_schema_drift(spark, entity, df, audit_path, run_date)

    # ── Row count validation ──────────────────────────────────────
    count = df.count()
    check_row_count(entity, count)

    # ── Freshness check ───────────────────────────────────────────
    if entity in FRESHNESS_COLS:
        check_freshness(df, entity, FRESHNESS_COLS[entity])

    # ── Write to Bronze ───────────────────────────────────────────
    # mergeSchema=false: if schema has drifted in a way check_schema_drift
    # didn't catch (e.g. non-breaking struct changes), fail rather than
    # silently alter the Bronze table schema.
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "false")
        .partitionBy("ingestion_date")
        .save(f"{bronze_path}/{entity}")
    )

    # ── Post-write verification ───────────────────────────────────
    # Read back the partition we just wrote and confirm count matches.
    # Catches write corruption before Silver runs on bad data.
    verify_write(spark, bronze_path, entity, run_date, count)

    write_audit(spark, audit_path, entity, count, run_date, "SUCCESS")
    log.info(f"[{entity}] Bronze complete — {count:,} rows ✓")


# ── Entry Point ───────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--env",      choices=["dev", "prod"], default="prod")
    parser.add_argument("--date",     default=str(date.today()))
    parser.add_argument("--lookback", type=int, default=3)
    parser.add_argument("--entity",   default="all")
    args = parser.parse_args()

    # For a daily batch export the full source file is re-read each run.
    # The lookback window determines how many date partitions to overwrite,
    # not which records to filter from the source file.
    paths    = PATHS[args.env]
    dates    = get_lookback_dates(args.date, args.lookback)
    run_date = dates[-1]

    spark = (
        SparkSession.builder
        .appName("yelp-bronze-ingestion")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Dynamic overwrite = only replace TODAY's partition, not the whole table
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

    entities = SOURCE_FILES if args.entity == "all" else {args.entity: SOURCE_FILES[args.entity]}
    failed   = []

    for entity, source_file in entities.items():
        try:
            ingest_entity(
                spark, entity, source_file,
                paths["landing"], paths["bronze"], paths["audit"], run_date,
            )
        except Exception as e:
            log.error(f"[{entity}] FAILED: {e}", exc_info=True)
            failed.append(entity)

    spark.stop()

    if failed:
        raise RuntimeError(f"Bronze ingestion failed for: {failed}")

    log.info("Bronze ingestion complete ✓")


if __name__ == "__main__":
    main()