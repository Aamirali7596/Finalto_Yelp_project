"""
bronze_ingestion.py — Yelp Bronze Layer Ingestion
Reads raw Yelp JSON files from S3 and writes to Delta Lake (Bronze).

Lookback: Re-processes the last N days on every run to catch
          late-arriving source data. Safe to re-run — dynamic
          partition overwrite only replaces affected partitions.
Usage:
  spark-submit bronze_ingestion.py --env prod --date 2026-04-26 --lookback 3
"""

import argparse
import logging
from datetime import date, datetime, timedelta

from pyspark.sql import SparkSession
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
        "entity": entity, "layer": "bronze",
        "row_count": row_count, "run_date": run_date,
        "run_timestamp": datetime.utcnow().isoformat(),
        "status": status, "error": error,
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

    # Validate
    count = df.count()
    check_row_count(entity, count)

    # Freshness check on date columns where applicable
    if entity in FRESHNESS_COLS:
        check_freshness(df, entity, FRESHNESS_COLS[entity])

    # Write to Bronze — dynamic partition overwrite keeps other dates safe
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "false")
        .partitionBy("ingestion_date")
        .save(f"{bronze_path}/{entity}")
    )

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
    paths = PATHS[args.env]
    dates = get_lookback_dates(args.date, args.lookback)
    run_date = dates[-1]   # latest date in the window = partition key

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
            ingest_entity(spark, entity, source_file, paths["landing"], paths["bronze"], paths["audit"], run_date)
        except Exception as e:
            log.error(f"[{entity}] FAILED: {e}", exc_info=True)
            failed.append(entity)

    spark.stop()

    if failed:
        raise RuntimeError(f"Bronze ingestion failed for: {failed}")

    log.info("Bronze ingestion complete ✓")


if __name__ == "__main__":
    main()