# Data Engineering
## Yelp Data Engineering Platform — Ingestion, Transformation, Modelling & Scalability
**Author:** Aamir | **Version:** 1.0 | **Last Updated:** April 2026

---

## Table of Contents

1. [Data Ingestion](#1-data-ingestion)
2. [Data Modelling — Star Schema](#2-data-modelling--star-schema)
3. [Data Transformation](#3-data-transformation)
4. [Scalability & Performance](#4-scalability--performance)

> **Related Documents**
> - Architecture decisions → `docs/architecture.md`
> - Data flow → `docs/data_flow.md`
> - Field-level lineage → `docs/data_lineage.md`
> - PySpark code → `ingestion/bronze_ingestion.py`, `transform/silver_transform.py`
> - dbt models → `dbt/models/`

---

## 1. Data Ingestion

### 1.1 Design Overview

Each of the five Yelp JSON files is treated as a separate data source. Each has its own ingestion job, its own explicit schema definition, and its own Bronze Delta table. They share a common ingestion pattern but are never mixed in a single job.

**Ingestion stack:**
- Compute: PySpark on Amazon EMR (ephemeral cluster per run)
- Storage: Delta Lake on Amazon S3
- Orchestration: Apache Airflow (MWAA) — `dag_bronze_ingestion`
- Schema: Explicit `StructType` definitions in `models/schema_definitions.py`

---

### 1.2 Airflow Ingestion Flow — Step by Step

```
Step 1  Airflow dag_bronze_ingestion triggers at 06:00 UTC
Step 2  EmrCreateJobFlowOperator provisions EMR cluster (m5.xlarge master, 3x m5.2xlarge core)
Step 3  EmrAddStepsOperator submits PySpark job per entity (5 parallel steps)
Step 4  PySpark reads raw JSON from S3 landing bucket
Step 5  Explicit StructType schema applied — no inferSchema
Step 6  Data written to Bronze Delta table, partitioned by ingestion_date
Step 7  EmrJobFlowSensor waits for all steps to complete
Step 8  EMR cluster auto-terminates (TerminationProtection = False)
Step 9  Airflow marks dag_bronze_ingestion SUCCESS
Step 10 ExternalTaskSensor in dag_silver_transform releases — Silver pipeline begins
```

**Why ephemeral clusters?**
A cluster that runs 24/7 waiting for a daily job costs ~$500/month in idle compute. An ephemeral cluster that lives for 15 minutes costs ~$0.50 per run. At this workload, ephemeral is the only sensible choice.

---

### 1.3 Schema Definitions

All schemas are defined explicitly in `models/schema_definitions.py`. Never use `inferSchema=True` on production pipelines — Spark samples 1% of the file to infer types, which means it can silently mistype a field if the sample doesn't contain a representative value.

```python
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType, IntegerType, BooleanType
)

business_schema = StructType([
    StructField("business_id",  StringType(),  nullable=False),
    StructField("name",         StringType(),  nullable=True),
    StructField("address",      StringType(),  nullable=True),
    StructField("city",         StringType(),  nullable=True),
    StructField("state",        StringType(),  nullable=True),
    StructField("postal_code",  StringType(),  nullable=True),
    StructField("latitude",     DoubleType(),  nullable=True),
    StructField("longitude",    DoubleType(),  nullable=True),
    StructField("stars",        DoubleType(),  nullable=True),
    StructField("review_count", LongType(),    nullable=True),
    StructField("is_open",      IntegerType(), nullable=True),
    StructField("categories",   StringType(),  nullable=True),
])

review_schema = StructType([
    StructField("review_id",   StringType(),  nullable=False),
    StructField("business_id", StringType(),  nullable=False),
    StructField("user_id",     StringType(),  nullable=False),
    StructField("stars",       IntegerType(), nullable=True),
    StructField("date",        StringType(),  nullable=True),
    StructField("text",        StringType(),  nullable=True),
    StructField("useful",      LongType(),    nullable=True),
    StructField("funny",       LongType(),    nullable=True),
    StructField("cool",        LongType(),    nullable=True),
])

user_schema = StructType([
    StructField("user_id",       StringType(),  nullable=False),
    StructField("name",          StringType(),  nullable=True),
    StructField("review_count",  LongType(),    nullable=True),
    StructField("yelping_since", StringType(),  nullable=True),
    StructField("useful",        LongType(),    nullable=True),
    StructField("funny",         LongType(),    nullable=True),
    StructField("cool",          LongType(),    nullable=True),
    StructField("elite",         StringType(),  nullable=True),
    StructField("fans",          LongType(),    nullable=True),
    StructField("average_stars", DoubleType(),  nullable=True),
])

checkin_schema = StructType([
    StructField("business_id", StringType(), nullable=False),
    StructField("date",        StringType(), nullable=True),
])

tip_schema = StructType([
    StructField("business_id",     StringType(),  nullable=False),
    StructField("user_id",         StringType(),  nullable=False),
    StructField("text",            StringType(),  nullable=True),
    StructField("date",            StringType(),  nullable=True),
    StructField("compliment_count",LongType(),    nullable=True),
])
```

---

### 1.4 Bronze Ingestion — PySpark

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_date
from models.schema_definitions import review_schema
from datetime import date

spark = SparkSession.builder \
    .appName("yelp-bronze-ingestion") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

LANDING_PATH = "s3://yelp-platform/landing"
BRONZE_PATH  = "s3://yelp-platform/bronze"
INGESTION_DATE = str(date.today())

def ingest_entity(entity_name: str, schema, source_file: str):
    df = spark.read \
        .schema(schema) \
        .json(f"{LANDING_PATH}/{source_file}")

    df = df.withColumn("ingestion_date", lit(INGESTION_DATE))

    row_count = df.count()
    print(f"[{entity_name}] rows read: {row_count:,}")
    assert row_count > 0, f"Empty source file: {source_file}"

    df.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("ingestion_date") \
        .save(f"{BRONZE_PATH}/{entity_name}")

    print(f"[{entity_name}] written to Bronze successfully")

ingest_entity("reviews",  review_schema,   "review.json")
ingest_entity("business", business_schema, "business.json")
ingest_entity("users",    user_schema,     "user.json")
ingest_entity("checkins", checkin_schema,  "checkin.json")
ingest_entity("tips",     tip_schema,      "tip.json")
```

**Key design decisions:**
- `partitionOverwriteMode = dynamic` ensures re-running for one `ingestion_date` overwrites only that partition — all historical partitions are safe
- `lit(current_date())` stamps every row with the ingestion date for audit purposes
- Row count assertion before write catches empty files early — fails loudly rather than writing an empty partition

---

### 1.5 Handling review.json at Scale

`review.json` at ~5.5GB is the dominant file. It needs special handling:

```python
reviews_df = spark.read.schema(review_schema).json(f"{LANDING_PATH}/review.json")

# Repartition to align with shuffle.partitions default
# avoids the 1-partition problem on large JSON reads
reviews_df = reviews_df.repartition(200)

# Write partitioned by year + month for downstream query pruning
from pyspark.sql.functions import year, month, to_timestamp

reviews_df = reviews_df \
    .withColumn("review_ts",    to_timestamp("date", "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("year",         year("review_ts")) \
    .withColumn("month",        month("review_ts")) \
    .withColumn("ingestion_date", lit(INGESTION_DATE))

reviews_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("ingestion_date") \
    .save(f"{BRONZE_PATH}/reviews")
```

---

## 2. Data Modelling — Star Schema

### 2.1 Design Rationale

The Gold layer uses a **Kimball star schema** — fact tables surrounded by dimension tables, joined on surrogate keys. This is the right choice for a BI team because:

- BI tools (Looker, Tableau) are optimised for star schema queries
- Analysts write simple `JOIN fact ON dim` queries — no complex multi-hop joins
- Aggregations on fact tables are fast because dimensions are pre-joined and pre-cleaned
- Pre-computed dimension attributes (e.g. `is_elite`, `primary_category`) eliminate runtime derivation in dashboards

---

### 2.2 Fact Tables

#### fact_reviews
Grain: one row per review. The primary analytical fact table.

```
fact_reviews
├── review_id           STRING    NOT NULL  (PK)
├── business_id         STRING    NOT NULL  (FK → dim_business)
├── user_id             STRING    NOT NULL  (FK → dim_user)
├── date_id             INTEGER   NOT NULL  (FK → dim_date)
├── review_stars        DOUBLE    NOT NULL
├── useful_votes        LONG
├── funny_votes         LONG
├── cool_votes          LONG
└── review_length_chars INTEGER

Materialisation : incremental (append new rows per run)
Partition       : year / month
Z-order         : business_id, user_id
```

#### fact_checkins
Grain: one row per checkin event (exploded from the source date blob).

```
fact_checkins
├── checkin_id          STRING    NOT NULL  (PK — surrogate, md5 of biz_id + ts)
├── business_id         STRING    NOT NULL  (FK → dim_business)
├── date_id             INTEGER   NOT NULL  (FK → dim_date)
├── checkin_hour        INTEGER             (0–23)
└── checkin_day_of_week STRING              (Monday … Sunday)

Materialisation : incremental
Partition       : year / month
Z-order         : business_id
```

---

### 2.3 Dimension Tables

#### dim_business
```
dim_business
├── business_id         STRING    NOT NULL  (PK)
├── name                STRING
├── city                STRING
├── state               STRING
├── stars               DOUBLE              (overall rating)
├── review_count        LONG
├── is_open             BOOLEAN
├── primary_category    STRING              (first category from list)
├── latitude            DOUBLE
└── longitude           DOUBLE

Materialisation : table (full refresh — SCD Type 1)
```

#### dim_user
```
dim_user
├── user_id             STRING    NOT NULL  (PK)
├── name                STRING              (PII — flagged)
├── review_count        LONG
├── average_stars       DOUBLE
├── is_elite            BOOLEAN             (derived from elite year list)
├── elite_years_count   INTEGER             (number of years elite)
├── fan_count           LONG
└── yelping_since_year  INTEGER             (year user joined Yelp)

Materialisation : table (full refresh — SCD Type 1)
```

#### dim_date
```
dim_date
├── date_id             INTEGER   NOT NULL  (PK — surrogate)
├── full_date           DATE      NOT NULL
├── year                INTEGER
├── month               INTEGER
├── quarter             INTEGER
├── day_of_week         INTEGER             (1=Sunday … 7=Saturday)
├── day_name            STRING              (Monday … Sunday)
├── is_weekend          BOOLEAN
└── week_of_year        INTEGER

Materialisation : table (static date spine — rebuilt once)
Source          : dbt_utils.date_spine macro — not derived from source data
```

---

### 2.4 BI Reporting Scenarios

The star schema is designed to answer these five scenarios out of the box — no additional joins or transformations needed by the BI team.

**Scenario 1 — Top-rated businesses by city and category**
```sql
select
    b.city,
    b.primary_category,
    b.name,
    avg(f.review_stars)    as avg_rating,
    count(f.review_id)     as total_reviews
from gold.fact_reviews f
join gold.dim_business b on f.business_id = b.business_id
join gold.dim_date d     on f.date_id     = d.date_id
where d.year = 2023
group by b.city, b.primary_category, b.name
order by avg_rating desc;
```

**Scenario 2 — Elite vs non-elite reviewer behaviour**
```sql
select
    u.is_elite,
    avg(f.review_stars)    as avg_stars_given,
    avg(f.useful_votes)    as avg_useful_votes,
    avg(f.review_length_chars) as avg_review_length,
    count(f.review_id)     as total_reviews
from gold.fact_reviews f
join gold.dim_user u on f.user_id = u.user_id
group by u.is_elite;
```

**Scenario 3 — Rising star businesses (Section 3 — SQL question)**
```sql
with recent as (
    select
        f.business_id,
        avg(f.review_stars)  as avg_stars_recent,
        count(f.review_id)   as review_count_recent
    from gold.fact_reviews f
    join gold.dim_date d on f.date_id = d.date_id
    where d.full_date >= dateadd(year, -1, current_date)
    group by f.business_id
),
historical as (
    select
        f.business_id,
        avg(f.review_stars)  as avg_stars_historical
    from gold.fact_reviews f
    join gold.dim_date d on f.date_id = d.date_id
    where d.full_date < dateadd(year, -1, current_date)
    group by f.business_id
)
select
    b.business_id,
    b.name,
    round(r.avg_stars_recent, 2)       as avg_stars_recent,
    round(h.avg_stars_historical, 2)   as avg_stars_historical,
    round(r.avg_stars_recent
        - h.avg_stars_historical, 2)   as rating_improvement
from recent r
join historical h     on r.business_id = h.business_id
join gold.dim_business b on r.business_id = b.business_id
where r.review_count_recent >= 10
  and r.avg_stars_recent >= h.avg_stars_historical + 1.0
order by rating_improvement desc;
```

**Scenario 4 — Peak check-in patterns by business type**
```sql
select
    b.primary_category,
    c.checkin_day_of_week,
    c.checkin_hour,
    count(c.checkin_id)    as total_checkins
from gold.fact_checkins c
join gold.dim_business b on c.business_id = b.business_id
group by b.primary_category, c.checkin_day_of_week, c.checkin_hour
order by total_checkins desc;
```

**Scenario 5 — Review volume trend over time**
```sql
select
    d.year,
    d.month,
    count(f.review_id)     as total_reviews,
    avg(f.review_stars)    as avg_rating
from gold.fact_reviews f
join gold.dim_date d on f.date_id = d.date_id
group by d.year, d.month
order by d.year, d.month;
```

---

## 3. Data Transformation

### 3.1 Silver Transformation — PySpark

Silver is where the raw data is cleaned, typed, and conformed. Each entity has its own transformation function. All Silver jobs run on the same EMR cluster, triggered by `dag_silver_transform`.

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, TimestampType, BooleanType, IntegerType

spark = SparkSession.builder \
    .appName("yelp-silver-transform") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

BRONZE_PATH = "s3://yelp-platform/bronze"
SILVER_PATH = "s3://yelp-platform/silver"
```

#### Reviews — Silver Transform

```python
def transform_reviews_silver():
    bronze_reviews = spark.read.format("delta").load(f"{BRONZE_PATH}/reviews")

    silver_reviews = bronze_reviews \
        .withColumn(
            "review_date",
            F.to_timestamp(F.col("date"), "yyyy-MM-dd HH:mm:ss")
        ) \
        .withColumn("stars", F.col("stars").cast(DoubleType())) \
        .withColumn("year",  F.year("review_date")) \
        .withColumn("month", F.month("review_date")) \
        .filter(F.col("review_id").isNotNull()) \
        .filter(F.col("stars").between(1.0, 5.0)) \
        .dropDuplicates(["review_id"]) \
        .drop("date", "ingestion_date")

    assert_row_counts(bronze_reviews, silver_reviews, "reviews")

    silver_reviews.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "false") \
        .partitionBy("year", "month") \
        .save(f"{SILVER_PATH}/reviews")

    print(f"[reviews] Silver complete — {silver_reviews.count():,} rows")
```

#### Business — Silver Transform (with category explode)

```python
def transform_business_silver():
    bronze_biz = spark.read.format("delta").load(f"{BRONZE_PATH}/business")

    silver_biz = bronze_biz \
        .withColumn("stars",       F.col("stars").cast(DoubleType())) \
        .withColumn("is_open",     F.col("is_open").cast(BooleanType())) \
        .withColumn("city",        F.coalesce(F.trim(F.col("city")),  F.lit("UNKNOWN"))) \
        .withColumn("state",       F.coalesce(F.upper(F.col("state")), F.lit("UNKNOWN"))) \
        .filter(F.col("business_id").isNotNull()) \
        .dropDuplicates(["business_id"]) \
        .drop("ingestion_date")

    silver_biz.write \
        .format("delta") \
        .mode("overwrite") \
        .save(f"{SILVER_PATH}/business")

    # Explode categories into bridge table
    biz_categories = bronze_biz \
        .select("business_id", "categories") \
        .filter(F.col("categories").isNotNull()) \
        .withColumn(
            "category",
            F.explode(F.split(F.trim(F.col("categories")), ",\\s*"))
        ) \
        .withColumn("category", F.trim(F.col("category"))) \
        .filter(F.col("category") != "") \
        .select("business_id", "category") \
        .dropDuplicates(["business_id", "category"])

    biz_categories.write \
        .format("delta") \
        .mode("overwrite") \
        .save(f"{SILVER_PATH}/business_categories")

    print(f"[business] Silver complete — {silver_biz.count():,} rows")
    print(f"[business_categories] {biz_categories.count():,} rows")
```

#### Users — Silver Transform (elite parsing)

```python
def transform_users_silver():
    bronze_users = spark.read.format("delta").load(f"{BRONZE_PATH}/users")

    silver_users = bronze_users \
        .withColumn(
            "yelping_since",
            F.to_timestamp(F.col("yelping_since"), "yyyy-MM-dd HH:mm:ss")
        ) \
        .withColumn(
            "is_elite",
            F.when(
                F.col("elite").isNotNull() & (F.trim(F.col("elite")) != ""),
                F.lit(True)
            ).otherwise(F.lit(False))
        ) \
        .withColumn(
            "elite_years_count",
            F.when(
                F.col("elite").isNotNull() & (F.trim(F.col("elite")) != ""),
                F.size(F.split(F.col("elite"), ","))
            ).otherwise(F.lit(0))
        ) \
        .withColumn("average_stars", F.col("average_stars").cast(DoubleType())) \
        .filter(F.col("user_id").isNotNull()) \
        .dropDuplicates(["user_id"]) \
        .drop("elite", "ingestion_date")

    silver_users.write \
        .format("delta") \
        .mode("overwrite") \
        .save(f"{SILVER_PATH}/users")

    print(f"[users] Silver complete — {silver_users.count():,} rows")
```

#### Checkins — Silver Transform (date blob explode)

```python
def transform_checkins_silver():
    bronze_checkins = spark.read.format("delta").load(f"{BRONZE_PATH}/checkins")

    # The date field is a single string of comma-separated timestamps
    # e.g. "2016-04-26 19:49:16, 2016-08-30 18:36:57, 2016-10-15 02:27:06"
    silver_checkins = bronze_checkins \
        .filter(F.col("business_id").isNotNull()) \
        .filter(F.col("date").isNotNull()) \
        .withColumn(
            "checkin_ts_str",
            F.explode(F.split(F.col("date"), ",\\s*"))
        ) \
        .withColumn(
            "checkin_ts",
            F.to_timestamp(F.trim(F.col("checkin_ts_str")), "yyyy-MM-dd HH:mm:ss")
        ) \
        .filter(F.col("checkin_ts").isNotNull()) \
        .withColumn("year",  F.year("checkin_ts")) \
        .withColumn("month", F.month("checkin_ts")) \
        .withColumn(
            "checkin_id",
            F.md5(F.concat_ws("||", F.col("business_id"), F.col("checkin_ts").cast("string")))
        ) \
        .select("checkin_id", "business_id", "checkin_ts", "year", "month") \
        .dropDuplicates(["checkin_id"])

    silver_checkins.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .save(f"{SILVER_PATH}/checkin_events")

    print(f"[checkins] Silver complete — {silver_checkins.count():,} rows")
```

---

### 3.2 Data Quality Assertions

Every Silver transformation includes row count assertions before writing. These are written to a `pipeline_audit` Delta table for trend tracking.

```python
def assert_row_counts(bronze_df, silver_df, entity: str):
    bronze_count = bronze_df.count()
    silver_count = silver_df.count()

    assert silver_count > 0, \
        f"[{entity}] Silver output is empty — pipeline failure"

    loss_pct = (bronze_count - silver_count) / bronze_count * 100
    print(f"[{entity}] Bronze: {bronze_count:,} | Silver: {silver_count:,} | Loss: {loss_pct:.1f}%")

    if loss_pct > 5:
        print(f"[{entity}] WARNING: >5% row loss — investigate duplicates or null filters")

    audit_row = spark.createDataFrame([{
        "entity":        entity,
        "bronze_count":  bronze_count,
        "silver_count":  silver_count,
        "loss_pct":      round(loss_pct, 2),
        "run_date":      str(date.today())
    }])
    audit_row.write.format("delta").mode("append") \
        .save("s3://yelp-platform/audit/pipeline_audit")
```

---

### 3.3 Gold Layer — dbt Models

Gold is built entirely by dbt on Databricks. Below are the key model definitions.

#### Staging — stg_reviews.sql

```sql
with source as (
    select * from {{ source('silver', 'reviews') }}
)
select
    review_id,
    business_id,
    user_id,
    cast(stars        as double)     as review_stars,
    review_date                      as reviewed_at,
    useful                           as useful_votes,
    funny                            as funny_votes,
    cool                             as cool_votes,
    length(text)                     as review_length_chars,
    year,
    month
from source
where review_id is not null
  and review_stars between 1.0 and 5.0
```

#### Intermediate — int_rising_star_candidates.sql

```sql
with recent as (
    select
        business_id,
        avg(review_stars)   as avg_stars_recent,
        count(review_id)    as review_count_recent
    from {{ ref('stg_reviews') }}
    where reviewed_at >= dateadd(year, -1, current_date)
    group by business_id
),
historical as (
    select
        business_id,
        avg(review_stars)   as avg_stars_historical
    from {{ ref('stg_reviews') }}
    where reviewed_at < dateadd(year, -1, current_date)
    group by business_id
)
select
    r.business_id,
    r.avg_stars_recent,
    r.review_count_recent,
    h.avg_stars_historical,
    round(r.avg_stars_recent - h.avg_stars_historical, 2) as rating_improvement
from recent r
inner join historical h on r.business_id = h.business_id
where r.review_count_recent     >= 10
  and r.avg_stars_recent        >= h.avg_stars_historical + 1.0
```

#### Mart — fact_reviews.sql

```sql
{{
    config(
        materialized = 'incremental',
        unique_key   = 'review_id',
        partition_by = {'field': 'year', 'data_type': 'int'},
        post_hook    = "OPTIMIZE {{ this }} ZORDER BY (business_id, user_id)"
    )
}}

select
    r.review_id,
    r.business_id,
    r.user_id,
    d.date_id,
    r.review_stars,
    r.useful_votes,
    r.funny_votes,
    r.cool_votes,
    r.review_length_chars,
    r.year,
    r.month
from {{ ref('stg_reviews') }} r
join {{ ref('dim_date') }}    d
    on cast(r.reviewed_at as date) = d.full_date

{% if is_incremental() %}
    where r.reviewed_at > (select max(reviewed_at) from {{ this }})
{% endif %}
```

#### dbt schema.yml — Tests

```yaml
version: 2

models:
  - name: fact_reviews
    description: "One row per review — primary analytical fact table"
    columns:
      - name: review_id
        description: "Unique review identifier"
        tests:
          - unique
          - not_null
      - name: business_id
        tests:
          - not_null
          - relationships:
              to: ref('dim_business')
              field: business_id
      - name: user_id
        tests:
          - not_null
          - relationships:
              to: ref('dim_user')
              field: user_id
      - name: review_stars
        tests:
          - not_null
          - accepted_values:
              values: [1.0, 2.0, 3.0, 4.0, 5.0]

  - name: dim_business
    columns:
      - name: business_id
        tests:
          - unique
          - not_null

  - name: dim_user
    columns:
      - name: user_id
        tests:
          - unique
          - not_null

  - name: dim_date
    columns:
      - name: date_id
        tests:
          - unique
          - not_null
      - name: full_date
        tests:
          - unique
          - not_null
```

---

## 4. Scalability & Performance

### 4.1 Partitioning Strategy

Partition keys are chosen based on how BI queries filter data. A well-chosen partition key means Spark reads only the relevant files rather than scanning the entire table.

| Table | Partition Key | Why |
|---|---|---|
| Bronze (all entities) | `ingestion_date` | Idempotent re-runs — overwrite only today's partition |
| Silver reviews | `year`, `month` | BI queries almost always include a date range filter |
| Silver checkin_events | `year`, `month` | Same pattern — time-series event data |
| Gold fact_reviews | `year`, `month` | Enables partition pruning on the largest table |
| Gold fact_checkins | `year`, `month` | Same |

**Partition pruning in practice:**
A query filtering `WHERE year = 2023 AND month = 6` on `fact_reviews` scans only the `year=2023/month=6` partition — roughly 1/60th of the full table. Without partitioning, Spark scans every file.

---

### 4.2 Z-Ordering (Delta Lake)

Z-ordering co-locates related data within Delta files. After Z-ordering by `business_id`, all rows for a given business are physically grouped in the same files. A query filtering `WHERE business_id = 'abc123'` now reads 2–5 files instead of 200.

```sql
-- Applied via dbt post_hook after each Gold table build
OPTIMIZE gold.fact_reviews  ZORDER BY (business_id, user_id);
OPTIMIZE gold.fact_checkins ZORDER BY (business_id);
OPTIMIZE gold.dim_business  ZORDER BY (city, state);
```

**When to run OPTIMIZE:** After every incremental load. The `post_hook` in the dbt model config handles this automatically.

---

### 4.3 Broadcast Joins

Dimension tables are small enough to fit in executor memory. Broadcasting them eliminates the shuffle that would otherwise occur when joining a large fact table to a small dimension table.

```python
from pyspark.sql.functions import broadcast

# Silver → intermediate join example
enriched = silver_reviews.join(
    broadcast(silver_business.select("business_id", "city", "state")),
    on="business_id",
    how="left"
)
```

**Default broadcast threshold:** 10MB. Dimension tables are well below this. In dbt on Databricks, broadcast hints can be added to SQL models:

```sql
select /*+ BROADCAST(dim_business) */
    f.review_id,
    b.city,
    b.primary_category
from {{ ref('stg_reviews') }} f
join {{ ref('dim_business') }} b on f.business_id = b.business_id
```

---

### 4.4 Peak vs Off-Peak Handling

| Condition | Scenario | Strategy |
|---|---|---|
| **Normal daily batch** | ~10GB processed once per day | Fixed 3-node EMR cluster; 200 shuffle partitions |
| **Peak load** | Historical backfill, all years at once | Scale EMR core nodes 3 → 10 via auto-scaling policy; increase `spark.sql.shuffle.partitions` to 800 |
| **Off-peak** | No jobs running | EMR auto-terminates; S3 is the only cost; MWAA scales to zero schedulers |
| **Streaming (future)** | Continuous Kafka ingest | Dedicated streaming cluster with auto-scaling; separate from batch cluster |

**EMR auto-scaling policy:**
```json
{
  "Constraints": {
    "MinCapacity": 3,
    "MaxCapacity": 10
  },
  "Rules": [
    {
      "Name": "ScaleOut",
      "Action": { "SimpleScalingPolicyConfiguration": { "ScalingAdjustment": 2 } },
      "Trigger": {
        "CloudWatchAlarmDefinition": {
          "MetricName": "YARNMemoryAvailablePercentage",
          "ComparisonOperator": "LESS_THAN",
          "Threshold": 20
        }
      }
    }
  ]
}
```

---

### 4.5 Monitoring & Benchmarking

#### Pipeline SLAs (Airflow)

| DAG | Expected Duration | SLA Alert Threshold |
|---|---|---|
| `dag_bronze_ingestion` | < 15 min | 20 min |
| `dag_silver_transform` | < 20 min | 30 min |
| `dag_gold_dbt` | < 25 min | 35 min |
| End-to-end | < 60 min | 90 min |

SLA misses trigger a PagerDuty alert via Airflow's `sla_miss_callback`.

#### Spark Job Metrics (CloudWatch + Spark UI)

| Metric | Threshold | Action if breached |
|---|---|---|
| Shuffle read bytes per task | > 500MB | Investigate data skew — salt the join key |
| Task duration P99 vs P50 | > 5x difference | Data skew — repartition before groupBy |
| GC time % per executor | > 10% | Increase executor memory (`spark.executor.memory`) |
| Stage input vs output rows | > 10% unexpected drop | Check Silver null filters — may be over-filtering |

#### Data Volume Tracking

Row counts per entity per run are written to `pipeline_audit` Delta table. A dbt test (`assert_rising_star_min_reviews.sql`) checks the rising star output is non-empty after each Gold build.

CloudWatch alarms on S3 bucket size:
- Alert if Bronze grows > 20% day-over-day (possible duplication bug)
- Alert if Gold shrinks > 5% day-over-day (possible incremental logic regression)

#### Delta Lake Maintenance

```sql
-- Scheduled weekly via Airflow maintenance DAG
VACUUM gold.fact_reviews  RETAIN 168 HOURS;   -- 7 days time travel
VACUUM gold.fact_checkins RETAIN 168 HOURS;
VACUUM silver.reviews     RETAIN 336 HOURS;   -- 14 days
```

`VACUUM` removes old Delta files that are no longer needed for time travel queries. Without it, storage costs grow unboundedly.

---

*End of Data Engineering Document*
*PySpark implementation → `ingestion/bronze_ingestion.py`, `transform/silver_transform.py`*
*dbt models → `dbt/models/staging/`, `dbt/models/intermediate/`, `dbt/models/marts/`*