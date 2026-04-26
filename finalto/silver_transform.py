"""
silver_transform.py — Yelp Silver Layer Transformation
Reads Bronze Delta tables and produces cleaned, typed Silver tables.

Lookback: Mirrors Bronze lookback — reads the same date window
          that Bronze re-processed so Silver stays in sync.

Checks per entity:
  - Row count vs Bronze (warn if > 5% loss)
  - Null rate on critical columns
  - Freshness of latest record
  - Regression vs previous run (reads audit table)

Usage:
  spark-submit silver_transform.py --env prod --date 2026-04-26 --lookback 3
"""

import argparse
import logging
from datetime import date, datetime, timedelta

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, BooleanType, IntegerType

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
log = logging.getLogger("silver")

PATHS = {
    "dev":  {"bronze": "s3://yelp-dev/bronze",  "silver": "s3://yelp-dev/silver",  "audit": "s3://yelp-dev/audit"},
    "prod": {"bronze": "s3://yelp-prod/bronze", "silver": "s3://yelp-prod/silver", "audit": "s3://yelp-prod/audit"},
}

MAX_ROW_LOSS_PCT = 5.0


# ── Helpers ───────────────────────────────────────────────────────

def get_lookback_dates(run_date, lookback_days):
    end   = datetime.strptime(run_date, "%Y-%m-%d").date()
    start = end - timedelta(days=lookback_days - 1)
    dates = [(start + timedelta(days=i)).isoformat() for i in range(lookback_days)]
    log.info(f"Silver lookback: {dates}")
    return dates


def read_bronze(spark, bronze_path, entity, lookback_dates):
    """Read Bronze filtered to the lookback window. Partition pruning fires automatically."""
    return (
        spark.read.format("delta")
        .load(f"{bronze_path}/{entity}")
        .filter(F.col("ingestion_date").isin(lookback_dates))
    )


def write_audit(spark, audit_path, entity, bronze_count, silver_count, status, error=""):
    loss_pct = round((bronze_count - silver_count) / bronze_count * 100, 2) if bronze_count else 0
    spark.createDataFrame([{
        "entity": entity, "layer": "silver",
        "bronze_count": bronze_count, "silver_count": silver_count,
        "loss_pct": loss_pct, "run_date": str(date.today()),
        "run_timestamp": datetime.utcnow().isoformat(),
        "status": status, "error": error,
    }]).write.format("delta").mode("append").save(audit_path)


def check_counts(bronze_df, silver_df, entity):
    """Assert Silver is non-empty. Warn if row loss exceeds threshold."""
    b = bronze_df.count()
    s = silver_df.count()
    assert s > 0, f"[{entity}] Silver is empty — check filters"
    loss = (b - s) / b * 100 if b else 0
    log.info(f"[{entity}] Bronze: {b:,} | Silver: {s:,} | Loss: {loss:.1f}%")
    if loss > MAX_ROW_LOSS_PCT:
        log.warning(f"[{entity}] Row loss {loss:.1f}% exceeds {MAX_ROW_LOSS_PCT}% — investigate")
    return b, s


def check_nulls(df, entity, critical_cols):
    """Warn if any critical column has nulls."""
    total = df.count()
    for col in critical_cols:
        if col not in df.columns:
            continue
        n = df.filter(F.col(col).isNull()).count()
        if n > 0:
            log.warning(f"[{entity}] '{col}': {n:,} nulls ({n/total:.2%}) — investigate")
        else:
            log.info(f"[{entity}] '{col}': zero nulls OK")


def check_freshness(df, entity, date_col):
    """Warn if latest record is older than 26 hours."""
    if date_col not in df.columns:
        return
    latest = df.agg(F.max(F.col(date_col))).collect()[0][0]
    if latest:
        age_h = (datetime.utcnow() - latest).total_seconds() / 3600
        flag = "OK" if age_h <= 26 else "STALE — investigate"
        log.info(f"[{entity}] Freshness: {age_h:.1f}h — {flag}")


def check_regression(spark, audit_path, entity, current):
    """Warn if Silver count dropped more than 10% vs previous run."""
    try:
        prev = (
            spark.read.format("delta").load(audit_path)
            .filter((F.col("entity") == entity) & (F.col("layer") == "silver") & (F.col("status") == "SUCCESS"))
            .orderBy(F.col("run_timestamp").desc()).limit(1).collect()
        )
        if prev:
            p = prev[0]["silver_count"]
            drop = (p - current) / p * 100 if p else 0
            if drop > 10:
                log.warning(f"[{entity}] Regression: count dropped {drop:.1f}% ({p:,} to {current:,})")
            else:
                log.info(f"[{entity}] Regression OK (change: {current - p:+,})")
    except Exception as e:
        log.warning(f"[{entity}] Regression check skipped: {e}")


# ── Transformations ───────────────────────────────────────────────

def transform_business(spark, bronze_path, silver_path, audit_path, lookback_dates):
    log.info("[business] Starting Silver transform")
    bronze = read_bronze(spark, bronze_path, "business", lookback_dates)

    silver = (
        bronze
        .withColumn("stars",        F.col("stars").cast(DoubleType()))
        .withColumn("review_count", F.col("review_count").cast(IntegerType()))
        .withColumn("is_open",      F.col("is_open").cast(BooleanType()))
        .withColumn("city",  F.coalesce(F.trim(F.col("city")),  F.lit("UNKNOWN")))
        .withColumn("state", F.coalesce(F.upper(F.col("state")), F.lit("UNKNOWN")))
        .filter(F.col("business_id").isNotNull())
        .dropDuplicates(["business_id"])
        .drop("ingestion_date")
    )

    check_nulls(silver, "business", ["business_id"])
    b, s = check_counts(bronze, silver, "business")
    silver.write.format("delta").mode("overwrite").save(f"{silver_path}/business")

    # Explode categories string into bridge table
    # "Restaurants, Italian, Pizza" becomes 3 rows: one per category
    (
        bronze.select("business_id", "categories")
        .filter(F.col("categories").isNotNull())
        .withColumn("category", F.explode(F.split(F.trim(F.col("categories")), r",\s*")))
        .withColumn("category", F.trim(F.col("category")))
        .filter(F.col("category") != "")
        .dropDuplicates(["business_id", "category"])
        .write.format("delta").mode("overwrite")
        .save(f"{silver_path}/business_categories")
    )

    write_audit(spark, audit_path, "business", b, s, "SUCCESS")
    log.info("[business] Silver complete")


def transform_reviews(spark, bronze_path, silver_path, audit_path, lookback_dates):
    log.info("[reviews] Starting Silver transform")
    bronze = read_bronze(spark, bronze_path, "reviews", lookback_dates)

    silver = (
        bronze
        .withColumn("review_date", F.to_timestamp(F.col("date"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("stars",       F.col("stars").cast(DoubleType()))
        .filter(F.col("review_id").isNotNull())
        .filter(F.col("review_date").isNotNull())
        .filter(F.col("stars").between(1.0, 5.0))   # drop invalid ratings
        .withColumn("year",  F.year("review_date"))
        .withColumn("month", F.month("review_date"))
        .dropDuplicates(["review_id"])               # lookback can create dupes across partitions
        .drop("date", "ingestion_date")
    )

    check_nulls(silver, "reviews", ["review_id", "business_id", "user_id"])
    b, s = check_counts(bronze, silver, "reviews")
    check_freshness(silver, "reviews", "review_date")
    check_regression(spark, audit_path, "reviews", s)

    silver.write.format("delta").mode("overwrite").partitionBy("year", "month").save(f"{silver_path}/reviews")
    write_audit(spark, audit_path, "reviews", b, s, "SUCCESS")
    log.info("[reviews] Silver complete")


def transform_users(spark, bronze_path, silver_path, audit_path, lookback_dates):
    log.info("[users] Starting Silver transform")
    bronze = read_bronze(spark, bronze_path, "users", lookback_dates)

    silver = (
        bronze
        .withColumn("yelping_since", F.to_timestamp(F.col("yelping_since"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("average_stars", F.col("average_stars").cast(DoubleType()))
        # elite field is a comma-separated year string e.g. "2015,2016,2017"
        # Non-null and non-empty means the user has been Elite at some point
        .withColumn("is_elite",
            F.when(F.col("elite").isNotNull() & (F.trim(F.col("elite")) != ""), F.lit(True))
             .otherwise(F.lit(False)))
        .withColumn("elite_years_count",
            F.when(F.col("elite").isNotNull() & (F.trim(F.col("elite")) != ""),
                   F.size(F.split(F.col("elite"), ",")))
             .otherwise(F.lit(0)))
        .filter(F.col("user_id").isNotNull())
        .dropDuplicates(["user_id"])
        .drop("elite", "friends", "ingestion_date")
    )

    check_nulls(silver, "users", ["user_id", "is_elite"])
    b, s = check_counts(bronze, silver, "users")
    check_regression(spark, audit_path, "users", s)

    silver.write.format("delta").mode("overwrite").save(f"{silver_path}/users")
    write_audit(spark, audit_path, "users", b, s, "SUCCESS")
    log.info("[users] Silver complete")


def transform_checkins(spark, bronze_path, silver_path, audit_path, lookback_dates):
    log.info("[checkins] Starting Silver transform")
    bronze = read_bronze(spark, bronze_path, "checkins", lookback_dates)

    # Source: 1 row per business, date = "ts1, ts2, ts3, ..."
    # Target: 1 row per checkin event with its own timestamp
    silver = (
        bronze
        .filter(F.col("business_id").isNotNull() & F.col("date").isNotNull())
        .withColumn("checkin_ts_str", F.explode(F.split(F.col("date"), r",\s*")))
        .withColumn("checkin_ts",     F.to_timestamp(F.trim(F.col("checkin_ts_str")), "yyyy-MM-dd HH:mm:ss"))
        .filter(F.col("checkin_ts").isNotNull())
        .withColumn("checkin_hour",        F.hour("checkin_ts"))
        .withColumn("checkin_day_of_week", F.date_format("checkin_ts", "EEEE"))
        .withColumn("year",                F.year("checkin_ts"))
        .withColumn("month",               F.month("checkin_ts"))
        # Surrogate key: md5(business_id + timestamp) — unique and deterministic
        .withColumn("checkin_id", F.md5(F.concat_ws("||", F.col("business_id"), F.col("checkin_ts").cast("string"))))
        .select("checkin_id", "business_id", "checkin_ts", "checkin_hour", "checkin_day_of_week", "year", "month")
        .dropDuplicates(["checkin_id"])
    )

    b_count = bronze.count()
    s_count = silver.count()
    assert s_count > 0, "[checkins] Silver output is empty"
    log.info(f"[checkins] Bronze businesses: {b_count:,} | Silver events: {s_count:,}")
    check_freshness(silver, "checkins", "checkin_ts")

    silver.write.format("delta").mode("overwrite").partitionBy("year", "month").save(f"{silver_path}/checkin_events")
    write_audit(spark, audit_path, "checkins", b_count, s_count, "SUCCESS")
    log.info("[checkins] Silver complete")


def transform_tips(spark, bronze_path, silver_path, audit_path, lookback_dates):
    log.info("[tips] Starting Silver transform")
    bronze = read_bronze(spark, bronze_path, "tips", lookback_dates)

    silver = (
        bronze
        .withColumn("tip_date", F.to_timestamp(F.col("date"), "yyyy-MM-dd HH:mm:ss"))
        .filter(F.col("business_id").isNotNull() & F.col("user_id").isNotNull() & F.col("tip_date").isNotNull())
        .withColumn("year",  F.year("tip_date"))
        .withColumn("month", F.month("tip_date"))
        .dropDuplicates(["business_id", "user_id", "tip_date"])
        .drop("date", "ingestion_date")
    )

    b, s = check_counts(bronze, silver, "tips")
    silver.write.format("delta").mode("overwrite").partitionBy("year", "month").save(f"{silver_path}/tips")
    write_audit(spark, audit_path, "tips", b, s, "SUCCESS")
    log.info("[tips] Silver complete")


# ── Entry Point ───────────────────────────────────────────────────

TRANSFORMATIONS = {
    "business": transform_business,
    "reviews":  transform_reviews,
    "users":    transform_users,
    "checkins": transform_checkins,
    "tips":     transform_tips,
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--env",      choices=["dev", "prod"], default="prod")
    parser.add_argument("--date",     default=str(date.today()))
    parser.add_argument("--lookback", type=int, default=3)
    parser.add_argument("--entity",   default="all")
    args = parser.parse_args()

    paths = PATHS[args.env]
    dates = get_lookback_dates(args.date, args.lookback)

    spark = (
        SparkSession.builder
        .appName("yelp-silver-transform")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    entities = TRANSFORMATIONS if args.entity == "all" else {args.entity: TRANSFORMATIONS[args.entity]}
    failed   = []

    for entity, fn in entities.items():
        try:
            fn(spark, paths["bronze"], paths["silver"], paths["audit"], dates)
        except Exception as e:
            log.error(f"[{entity}] FAILED: {e}", exc_info=True)
            failed.append(entity)

    spark.stop()

    if failed:
        raise RuntimeError(f"Silver failed for: {failed}")

    log.info("Silver transformation complete")


if __name__ == "__main__":
    main()