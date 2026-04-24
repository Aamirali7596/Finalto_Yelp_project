# Architecture Design Document
## Yelp Dataset — Data Engineering Platform
**Author:** Aamir | **Version:** 2.0 | **Last Updated:** April 2026

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Dataset Overview](#2-dataset-overview)
3. [Architecture Principles](#3-architecture-principles)
4. [System Architecture](#4-system-architecture)
5. [Tech Stack & Justification](#5-tech-stack--justification)
6. [Medallion Architecture (Layered Data Design)](#6-medallion-architecture-layered-data-design)
7. [Data Ingestion Design](#7-data-ingestion-design)
8. [Data Transformation Design](#8-data-transformation-design)
9. [Gold Layer — dbt Modelling](#9-gold-layer--dbt-modelling)
10. [Data Modelling Strategy](#10-data-modelling-strategy)
11. [Orchestration](#11-orchestration)
12. [Scalability & Performance](#12-scalability--performance)
13. [Real-Time Extension (Kafka)](#13-real-time-extension-kafka)
14. [Monitoring & Observability](#14-monitoring--observability)
15. [Security & Governance](#15-security--governance)
16. [Trade-offs & Alternatives Considered](#16-trade-offs--alternatives-considered)

---

## 1. Executive Summary

This document describes the end-to-end data engineering architecture for ingesting, transforming, and serving the Yelp dataset to a Business Intelligence (BI) team. The platform is designed on AWS, built around a **Medallion (Bronze → Silver → Gold)** layered architecture, with **PySpark on Amazon EMR** as the compute engine for ingestion and transformation, **dbt (data build tool)** for Gold layer modelling and data quality, **Delta Lake on S3** as the storage layer, and **Apache Airflow** as the orchestration backbone.

The compute and transformation responsibilities are deliberately split by layer:
- **PySpark on EMR** — Bronze and Silver layers (heavy I/O, JSON parsing, schema enforcement, large-scale cleaning)
- **dbt on Databricks** — Gold layer (star schema modelling, business logic, lineage documentation, automated testing)

This mirrors the modern data stack pattern used by leading data teams and reflects the organisation's existing tooling.

The architecture is designed with three guiding priorities:

- **Reliability** — data pipelines must produce consistent, trustworthy outputs that BI consumers can depend on
- **Scalability** — the system must handle both the current batch Yelp dataset and a future real-time event stream without a fundamental redesign
- **Maintainability** — pipelines are modular, observable, and easy to extend by any engineer on the team

A real-time ingestion extension using **Apache Kafka + Spark Structured Streaming** is outlined as a forward-looking design to support the organisation's stated direction toward capturing live event data.

---

## 2. Dataset Overview

The Yelp dataset consists of five JSON source files, each representing a distinct entity:

| Entity | File | Approx. Size | Grain | Key Relationships |
|---|---|---|---|---|
| Business | `business.json` | ~120MB | One row per business | Root entity |
| Review | `review.json` | ~5.5GB | One row per review | → Business, → User |
| User | `user.json` | ~3.5GB | One row per user | → Review |
| Checkin | `checkin.json` | ~290MB | One row per business (dates denormalised) | → Business |
| Tip | `tip.json` | ~240MB | One row per tip | → Business, → User |

**Notable data quality considerations identified during exploration:**

- `categories` in `business.json` is a comma-separated string, not an array — requires splitting and exploding for category-level analysis
- `date` in `checkin.json` is a single denormalised string of comma-separated timestamps — requires parsing into individual rows
- `elite` in `user.json` is a comma-separated list of years — requires transformation to derive an `is_elite` boolean flag
- `review.json` at ~5.5GB is the dominant table and drives all partitioning and performance decisions
- Referential integrity is not enforced at source — `review.business_id` and `review.user_id` may reference deleted entities

---

## 3. Architecture Principles

These principles governed every design decision in this document. Where trade-offs were made, they are explained explicitly in [Section 15](#15-trade-offs--alternatives-considered).

### P1 — Separate Storage from Compute
Data is stored on **S3**, decoupled from the compute layer (**EMR**). This means clusters can be spun up, scaled, or terminated without any risk to data. It also means different compute engines (Athena for ad-hoc SQL, Redshift Spectrum for warehousing, SageMaker for ML) can query the same data without duplication.

### P2 — Schema-on-Write, Not Schema-on-Read
Schemas are defined and enforced explicitly at the Silver layer. BI teams receive typed, validated data — not raw JSON they must interpret themselves. This prevents silent data quality issues from propagating into dashboards.

### P3 — Idempotent Pipelines
Every pipeline job can be re-run safely. Bronze writes are `overwrite` by partition. Silver and Gold use Delta Lake's `MERGE` semantics to upsert rather than duplicate. Re-running any job for the same date partition produces the same result.

### P4 — Build for the BI Consumer
The Gold layer is designed around how analysts actually query data — pre-aggregated, denormalised where it helps performance, and named using business-friendly terminology. The BI team should never need to understand the raw source schema.

### P5 — Design for Real-Time from Day One
Even though the current workload is batch, S3 prefixes, Delta table structures, and partition strategies are chosen to be compatible with a streaming extension. Adding Kafka + Structured Streaming should not require a schema redesign.

---

## 4. System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES                                        │
│                                                                              │
│  [Yelp JSON Files]          [Future: Kafka Topics]                          │
│  business / review /        reviews-stream / checkin-stream                 │
│  user / checkin / tip       (real-time event feed)                          │
└────────────────┬────────────────────────┬───────────────────────────────────┘
                 │ batch                  │ streaming (future)
                 ▼                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                  INGESTION & TRANSFORMATION — PySpark on EMR                 │
│                                                                              │
│   BRONZE                             SILVER                                 │
│   - Read raw JSON                    - Enforce explicit schemas             │
│   - Apply source schema              - Cast types, handle nulls             │
│   - Land to Delta on S3              - Explode categories & checkins        │
│   - Partition by ingestion_date      - Deduplicate on natural keys          │
│   - Append-only, no transforms       - Partition by year / month            │
└────────────────────────────────────────┬────────────────────────────────────┘
                                         │ Silver Delta tables
                                         ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                     GOLD LAYER — dbt on Databricks                          │
│                                                                              │
│   dbt models read from Silver Delta tables (via Databricks Unity Catalog)   │
│                                                                              │
│   ┌──────────────────┐  ┌──────────────────┐  ┌───────────────────────┐    │
│   │  Staging models  │→ │ Intermediate      │→ │  Mart models          │    │
│   │  (stg_*)         │  │ (int_*)           │  │  (fact_* / dim_*)     │    │
│   │                  │  │                   │  │                       │    │
│   │ Rename columns   │  │ Business logic    │  │ fact_reviews          │    │
│   │ Light casting    │  │ Rising star calc  │  │ fact_checkins         │    │
│   │ Source freshness │  │ Elite user flags  │  │ dim_business          │    │
│   │ checks           │  │ Date spine joins  │  │ dim_user / dim_date   │    │
│   └──────────────────┘  └──────────────────┘  └───────────────────────┘    │
│                                                                              │
│   dbt tests: not_null, unique, relationships, accepted_values               │
│   dbt docs:  auto-generated lineage graph + column descriptions             │
└────────────────────────────────────────┬────────────────────────────────────┘
                                         │ Gold Delta tables
                                         ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        SERVING & CONSUMPTION LAYER                          │
│                                                                              │
│   Amazon Athena          Amazon Redshift Spectrum       BI Tools            │
│   (ad-hoc SQL on S3)     (warehoused Gold layer)        (Looker / Tableau / │
│                                                          QuickSight)         │
└─────────────────────────────────────────────────────────────────────────────┘
                                         │
                          ┌──────────────┴──────────────┐
                          │    ORCHESTRATION & OPS       │
                          │                              │
                          │   Apache Airflow (MWAA)      │
                          │   - EMR job operators        │
                          │   - DbtCloudRunJobOperator   │
                          │   - SLA monitoring           │
                          │   - Retry & alerting         │
                          │                              │
                          │   AWS CloudWatch             │
                          │   - EMR job metrics          │
                          │   - S3 storage metrics       │
                          └──────────────────────────────┘
```

---

## 5. Tech Stack & Justification

Every technology choice below has a specific reason. "Everyone uses it" is not a reason.

### 5.1 Amazon S3 — Object Storage

**Why S3:**
S3 is the foundation of the architecture because it decouples storage from compute entirely. Any compute engine — EMR, Athena, Redshift Spectrum, SageMaker — can read from the same S3 path without data movement or duplication. At the scale of the Yelp dataset (~10GB), S3 costs are negligible. At 10x or 100x scale, the cost advantage over a managed warehouse like Redshift (which charges for storage + compute together) becomes significant.

S3 also provides 11 nines of durability out of the box with no operational overhead.

**Alternative considered:** HDFS on EMR. Rejected because HDFS is coupled to the cluster — when the cluster terminates, data is at risk. S3 externalises storage permanently.

---

### 5.2 Delta Lake — Table Format

**Why Delta Lake over plain Parquet:**

This is the most important single decision in the architecture. Plain Parquet on S3 is read-only from a consistency standpoint — if a write job fails halfway, readers may see partial data. For a BI team, silent data corruption is the worst possible failure mode.

Delta Lake adds four capabilities that plain Parquet cannot provide:

| Capability | Why It Matters |
|---|---|
| **ACID transactions** | Concurrent reads and writes are safe. BI dashboards don't see partial pipeline runs. |
| **Schema enforcement** | A pipeline bug that produces the wrong column type fails loudly at write time, not silently in a Looker dashboard. |
| **Time travel** | `VERSION AS OF` or `TIMESTAMP AS OF` — can query any historical state of the data. Invaluable for debugging and auditing. |
| **Efficient upserts (MERGE)** | Handle late-arriving data and deduplication without full table rewrites. |

**Alternative considered:** Apache Iceberg. Also an excellent choice with similar ACID guarantees. Delta Lake was chosen because of its tighter Spark integration and the team's existing Databricks/EMR familiarity.

---

### 5.3 PySpark on Amazon EMR — Compute

**Why PySpark:**
The Yelp `review.json` file alone is ~5.5GB. Pandas would load this into a single machine's memory and either run slowly or OOM entirely. PySpark distributes the data across a cluster, processing partitions in parallel. For the transformation workload here — joins, aggregations, window functions across millions of rows — Spark's Catalyst optimiser and columnar execution engine are the right tool.

**Why EMR over Glue:**
AWS Glue is a managed Spark service but abstracts away too much control. EMR gives direct access to Spark configuration (executor memory, partition counts, broadcast thresholds), which is necessary for tuning the join-heavy Gold layer transformations. Glue's DynamicFrame abstraction also adds complexity without benefit when we're already using Delta Lake's DataFrame API.

**Why EMR over Databricks:**
Databricks is the superior Spark experience and would be the first choice in a greenfield environment. For this design, EMR was chosen to keep the entire stack within a single cloud provider (AWS) without a third-party vendor dependency. In a production setting, Databricks on AWS would be a strong alternative.

---

### 5.4 Apache Airflow (Amazon MWAA) — Orchestration

**Why Airflow:**
Airflow's DAG-based model maps naturally to the Medallion pipeline:
- `dag_bronze_ingestion` → runs on a schedule, triggers EMR job
- `dag_silver_transform` → triggered on Bronze completion (sensor)
- `dag_gold_transform` → triggered on Silver completion (sensor)

This creates a clear dependency chain with retry logic, SLA alerts, and a full audit log of every run. Airflow's EMR operators (`EmrCreateJobFlowOperator`, `EmrAddStepsOperator`, `EmrJobFlowSensor`) handle cluster lifecycle natively.

**Why MWAA (Managed Airflow) over self-hosted:**
Eliminates the operational burden of managing Airflow's scheduler, workers, and metadata database. For a data engineering team whose core job is pipelines, not infrastructure, managed Airflow is the right call.

---

### 5.5 dbt (data build tool) — Gold Layer Transformation & Modelling

**Why dbt for the Gold layer:**
dbt's core value proposition is treating SQL transformations as software — version controlled, tested, documented, and composable. For the Gold layer specifically, this matters enormously because Gold tables are the ones BI analysts query every day. Any bug in a Gold model lands directly in a dashboard.

dbt brings four capabilities that raw PySpark at the Gold layer cannot match:

| Capability | Why It Matters |
|---|---|
| **Built-in testing** | `not_null`, `unique`, `relationships`, `accepted_values` tests run automatically on every model build. A broken foreign key between `fact_reviews` and `dim_business` fails loudly before BI ever sees it. |
| **Auto-generated lineage docs** | dbt generates a full DAG visualisation showing exactly how each Gold table is built from Silver sources. Invaluable for debugging and onboarding new team members. |
| **Incremental materialisation** | `fact_reviews` is materialised as `incremental` — only new rows are processed on each run rather than rebuilding the entire table. For a table growing by millions of rows per day, this is non-negotiable for performance. |
| **Separation of concerns** | PySpark handles what it's good at (large-scale I/O, JSON parsing, distributed cleaning). dbt handles what it's good at (structured SQL transformations, business logic, documentation). Each tool does its job. |

**dbt model structure (3-layer within Gold):**

```
staging (stg_*)       → thin wrapper on Silver, rename columns, light casting
intermediate (int_*)  → business logic, complex joins, pre-aggregations
marts (fact_* / dim_*)→ final BI-consumable tables, star schema
```

**Why dbt on Databricks specifically:**
Databricks is the team's existing compute layer for non-EMR workloads. dbt's Databricks adapter runs models directly against Databricks SQL Warehouses, writing results back to Delta Lake tables in Unity Catalog. This means Gold tables produced by dbt are native Delta Lake tables — consistent with Bronze and Silver, queryable by Athena and Redshift Spectrum with no format changes.

**Alternative considered:** Running dbt on EMR via the Spark adapter. Rejected because the Databricks SQL adapter is more mature, better supported, and the Databricks SQL Warehouse provides superior query performance for the iterative model development workflow that dbt encourages.

---

### 5.6 Amazon Athena — Ad-hoc Query Layer

**Why Athena:**
Athena provides serverless SQL directly on S3 Delta tables with no infrastructure to manage. For the BI team to run exploratory queries or for data engineers to validate Silver/Gold outputs, Athena is zero-setup and pay-per-query. It reads Delta Lake's transaction log natively (via the Delta Lake connector for Athena).

---

## 6. Medallion Architecture (Layered Data Design)

The pipeline is organised into three layers, each with a distinct purpose and contract.

```
SOURCE JSON → [BRONZE] → [SILVER] → [GOLD] → BI TOOLS
```

### Bronze Layer — Raw Landing Zone
**Purpose:** Preserve the source data exactly as received. No transformations, no business logic.

**Contract:**
- Data is written as Delta tables, append-only
- Schema is inferred or loosely applied (strings for everything where uncertain)
- Every source record is preserved, including duplicates and nulls
- Partition by ingestion date (`ingestion_date=YYYY-MM-DD`)

**S3 path convention:**
```
s3://yelp-data-platform/bronze/
├── business/ingestion_date=2026-04-24/
├── reviews/ingestion_date=2026-04-24/
├── users/ingestion_date=2026-04-24/
├── checkins/ingestion_date=2026-04-24/
└── tips/ingestion_date=2026-04-24/
```

**Why keep raw data?**
If a Silver transformation has a bug, we can re-process from Bronze without re-ingesting from source. This is the data engineering equivalent of version control — Bronze is your `git` history.

---

### Silver Layer — Cleaned & Conformed
**Purpose:** Apply data quality rules, enforce types, flatten nested structures, deduplicate.

**Contract:**
- Explicit schemas enforced via Delta Lake schema enforcement
- All timestamps cast to `TimestampType`, all IDs to `StringType`, all numeric fields to `DoubleType` or `LongType`
- Nulls handled with documented rules (e.g. `stars` null → exclude row; `city` null → `"UNKNOWN"`)
- `categories` string exploded into `business_categories` bridge table
- `checkin.date` string parsed into individual `checkin_events` rows
- Deduplication applied on natural keys (e.g. `review_id`)
- Partition by event date (`year=YYYY/month=MM`)

**Transformations applied at Silver:**

| Source field | Issue | Silver treatment |
|---|---|---|
| `business.categories` | Comma-separated string | Split + explode → `dim_business_categories` |
| `checkin.date` | Comma-separated timestamps | Split + explode → one row per checkin event |
| `user.elite` | Comma-separated years string | Parse → `is_elite` boolean + `elite_years` array |
| `review.date` | String `"YYYY-MM-DD HH:MM:SS"` | Cast to `TimestampType` |
| All ID fields | String (correct) | Enforce not-null constraint |

---

### Gold Layer — BI-Ready Star Schema
**Purpose:** Serve the BI team. Pre-aggregated, denormalised, named for business users.

**Contract:**
- Kimball-style star schema: fact tables + dimension tables
- Fact tables partitioned by `year`/`month` for query performance
- Dimension tables are full snapshots (SCD Type 1 — overwrite on change)
- All joins between Gold tables use surrogate integer keys for performance
- Z-ordering applied on high-cardinality filter columns (`business_id`, `user_id`, `date`)

**Gold tables:** See [Section 9 — Data Modelling Strategy](#9-data-modelling-strategy).

---

## 7. Data Ingestion Design

### 7.1 Batch Ingestion Flow

```
1. Airflow DAG triggered (scheduled or manual)
2. EmrCreateJobFlowOperator spins up EMR cluster
3. PySpark job reads source JSON from S3 landing bucket
4. Explicit schema applied (StructType definitions)
5. Data written to Bronze Delta table (partitioned by ingestion_date)
6. EMR cluster auto-terminates
7. Airflow marks task SUCCESS, triggers Silver DAG sensor
```

### 7.2 Schema Enforcement at Ingestion

Schemas are defined explicitly in `models/schema_definitions.py` and imported by the ingestion job. This prevents Spark's schema inference from silently mistyping fields (a common source of downstream bugs — e.g. inferring `stars` as `LongType` when it should be `DoubleType`).

### 7.3 Handling Large Files

`review.json` at ~5.5GB requires careful handling:
- Read with `spark.read.schema(review_schema).json(path)` — never `.inferSchema()`
- Repartition to 200 partitions after read to align with Spark's default shuffle partition count
- Write partitioned by `year` and `month` to enable partition pruning in downstream jobs

### 7.4 Idempotency

Bronze writes use `.mode("overwrite").partitionOverwrite(True)` with `spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")`. This means re-running the ingestion for a specific date only overwrites that date's partition — all other partitions are untouched. Safe to re-run at any time.

---

## 8. Data Transformation Design

### 8.1 Silver Transformation Pipeline

```python
# High-level transformation flow per entity

# Business: flatten categories
business_silver = business_bronze
    .withColumn("category", explode(split(col("categories"), ", ")))
    .withColumn("stars", col("stars").cast(DoubleType()))
    .dropDuplicates(["business_id"])

# Reviews: parse dates, cast types
reviews_silver = reviews_bronze
    .withColumn("review_date", to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("stars", col("stars").cast(DoubleType()))
    .dropDuplicates(["review_id"])

# Checkins: parse denormalised date string into rows
checkins_silver = checkins_bronze
    .withColumn("checkin_date", explode(split(col("date"), ", ")))
    .withColumn("checkin_ts", to_timestamp(col("checkin_date"), "yyyy-MM-dd HH:mm:ss"))
    .drop("date")
```

### 8.2 Silver → Gold Handoff

Silver is the boundary between PySpark and dbt. Once Silver tables are written as clean, typed Delta tables on S3, the compute responsibility shifts entirely to dbt running on Databricks. PySpark does not touch the Gold layer.

The handoff contract is:
- Silver tables are registered in the **AWS Glue Data Catalog** (Databricks Unity Catalog can federate to this)
- dbt reads Silver tables as **sources** defined in `schema.yml`
- dbt writes Gold tables back to Delta Lake on S3 under `s3://yelp-data-platform/gold/`

---

## 9. Gold Layer — dbt Modelling

### 9.1 Why dbt Takes Over at Gold

PySpark is the right tool for Bronze and Silver: large-scale I/O, schema enforcement, JSON parsing, and distributed cleaning across gigabytes of raw data. But at Gold, the work changes character — it becomes structured SQL transformations, business logic, star schema joins, and BI-serving aggregations. This is exactly what dbt is built for.

Handing off to dbt at Gold gives us:
- Every Gold model is a plain `.sql` file — readable and reviewable by analysts, not just engineers
- Built-in data quality tests that run on every `dbt run`
- Auto-generated lineage documentation that shows exactly how `fact_reviews` traces back to `review.json`
- Incremental materialisation — only process new rows, not the entire table on every run

### 9.2 dbt Model Layers

dbt models within the Gold layer are organised into three sub-layers following dbt best practices:

**Staging (`stg_*`)** — Thin wrappers on Silver Delta tables. Rename columns to business-friendly names, apply final casts, add source freshness checks. No joins, no business logic.

```sql
-- stg_reviews.sql
select
    review_id,
    business_id,
    user_id,
    stars                                       as review_stars,
    cast(review_date as timestamp)              as reviewed_at,
    useful                                      as useful_votes,
    funny                                       as funny_votes,
    cool                                        as cool_votes,
    length(text)                                as review_length_chars
from {{ source('silver', 'reviews') }}
```

**Intermediate (`int_*`)** — Business logic lives here. Complex joins, pre-aggregations, derived flags. This is where the rising star calculation is built before it lands in the mart.

```sql
-- int_rising_star_candidates.sql
with recent as (
    select business_id,
           avg(review_stars)  as avg_stars_recent,
           count(*)           as review_count_recent
    from {{ ref('stg_reviews') }}
    where reviewed_at >= dateadd(year, -1, current_date)
    group by business_id
),
historical as (
    select business_id,
           avg(review_stars)  as avg_stars_historical
    from {{ ref('stg_reviews') }}
    where reviewed_at < dateadd(year, -1, current_date)
    group by business_id
)
select
    r.business_id,
    r.avg_stars_recent,
    r.review_count_recent,
    h.avg_stars_historical,
    (r.avg_stars_recent - h.avg_stars_historical) as rating_improvement
from recent r
inner join historical h on r.business_id = h.business_id
where r.review_count_recent >= 10
  and r.avg_stars_recent >= h.avg_stars_historical + 1.0
```

**Marts (`fact_*` / `dim_*`)** — Final BI-consumable tables. Materialised as Delta Lake tables, partitioned, Z-ordered, ready for Looker/Tableau.

### 9.3 dbt Materialisations

| Model | Materialisation | Reason |
|---|---|---|
| `stg_*` | `view` | Lightweight — no storage cost, always fresh |
| `int_*` | `ephemeral` or `view` | Intermediate logic, not queried directly by BI |
| `fact_reviews` | `incremental` | Millions of rows — only process new records per run |
| `fact_checkins` | `incremental` | Same — append-only event data |
| `dim_business` | `table` | Full refresh — SCD Type 1, relatively small |
| `dim_user` | `table` | Full refresh — user attributes change infrequently |
| `dim_date` | `table` | Static date spine — rebuilt once |

### 9.4 dbt Tests

Defined in `schema.yml`, these run automatically on every `dbt test`:

```yaml
models:
  - name: fact_reviews
    columns:
      - name: review_id
        tests:
          - unique
          - not_null
      - name: business_id
        tests:
          - not_null
          - relationships:
              to: ref('dim_business')
              field: business_id
      - name: review_stars
        tests:
          - accepted_values:
              values: [1, 2, 3, 4, 5]
```

A failed test blocks the downstream DAG — Gold tables never serve invalid data to BI.

### 9.5 Airflow Integration

dbt runs are triggered from Airflow using the `DbtCloudRunJobOperator` (if using dbt Cloud) or `BashOperator` with `dbt run` (if running dbt Core locally on the Databricks cluster):

```python
run_dbt_gold = BashOperator(
    task_id="run_dbt_gold_models",
    bash_command="dbt run --select gold --profiles-dir /path/to/profiles",
    dag=dag_gold
)

test_dbt_gold = BashOperator(
    task_id="test_dbt_gold_models",
    bash_command="dbt test --select gold --profiles-dir /path/to/profiles",
    dag=dag_gold
)

run_dbt_gold >> test_dbt_gold
```

`dbt test` always runs after `dbt run` — if tests fail, Airflow marks the task as failed and downstream BI is not served stale or broken data.

---

## 10. Data Modelling Strategy

The Gold layer implements a **Kimball star schema** optimised for BI query patterns, built entirely as dbt mart models.

### Fact Tables

**`fact_reviews`** — grain: one row per review
```
review_id (PK)     | business_id (FK) | user_id (FK)   | date_id (FK)
stars              | useful_votes     | funny_votes    | cool_votes
review_length_chars
```

**`fact_checkins`** — grain: one row per checkin event
```
checkin_id (PK)    | business_id (FK) | date_id (FK)
checkin_hour       | checkin_day_of_week
```

### Dimension Tables

**`dim_business`**
```
business_id (PK)   | name           | city           | state
stars              | review_count   | is_open        | primary_category
latitude           | longitude
```

**`dim_user`**
```
user_id (PK)       | name           | review_count   | avg_stars
is_elite           | fan_count      | yelping_since_year
```

**`dim_date`**
```
date_id (PK)       | full_date      | year           | month
quarter            | day_of_week    | is_weekend     | week_of_year
```

**`bridge_business_categories`** (handles many-to-many)
```
business_id        | category
```

### BI Reporting Scenarios This Enables

1. **Top-rated businesses by city and category** — `fact_reviews` → `dim_business` → `bridge_business_categories` → filter by `city`, `category`, aggregate `avg(stars)`
2. **Elite vs non-elite reviewer behaviour** — `fact_reviews` → `dim_user` → group by `is_elite`, compare `avg(stars)`, `avg(useful_votes)`
3. **Rising star businesses** — compare `avg(stars)` in `fact_reviews` for `date < 1yr ago` vs `date >= 1yr ago` per `business_id`
4. **Peak check-in patterns by business type** — `fact_checkins` → `dim_date` → `dim_business` → group by `checkin_hour`, `day_of_week`, `primary_category`
5. **Review volume trends over time** — `fact_reviews` → `dim_date` → group by `year`, `month` — identify growth periods and seasonal patterns

---

## 11. Orchestration

### Airflow DAG Structure

Three DAGs, one per Medallion layer, with explicit upstream dependencies:

```
dag_bronze_ingestion          (schedule: daily @06:00 UTC)
│
└── ingest_business
└── ingest_reviews            (largest job — 200 partitions)
└── ingest_users
└── ingest_checkins
└── ingest_tips

dag_silver_transform          (triggered by: dag_bronze_ingestion success)
│
└── transform_business_silver
└── transform_reviews_silver
└── transform_users_silver
└── transform_checkins_silver

dag_gold_dbt              (triggered by: dag_silver_transform success)
│
└── dbt_run_staging       (stg_* models — views on Silver)
└── dbt_run_intermediate  (int_* models — business logic)
└── dbt_run_marts         (fact_* and dim_* — final star schema)
└── dbt_test_gold         (runs all schema.yml tests — blocks on failure)
└── dbt_docs_generate     (refreshes lineage docs after successful run)
```

### EMR Cluster Strategy

- **Ephemeral clusters** — one cluster per DAG run, auto-terminates on completion
- **Master node:** `m5.xlarge` (4 vCPU, 16GB RAM)
- **Core nodes:** 3x `m5.2xlarge` (8 vCPU, 32GB RAM each) — scales to 10x under peak
- **Spot instances** for task nodes (non-critical, interruptible work) to reduce cost by ~70%

### dbt Compute (Databricks)

- dbt runs against a **Databricks SQL Warehouse** (serverless tier) — auto-scales to zero when idle
- No always-on cluster cost — Databricks SQL Warehouse is billed per DBU consumed during the dbt run
- `dbt run` and `dbt test` are sequential tasks in Airflow — test failure halts the DAG and alerts on-call

---

## 12. Scalability & Performance

### Partitioning Strategy

| Table | Partition Key | Rationale |
|---|---|---|
| Bronze (all) | `ingestion_date` | Enables idempotent re-runs per day |
| Silver reviews | `year`, `month` | BI queries almost always filter by date range |
| Gold fact_reviews | `year`, `month` | Same — partition pruning eliminates full scans |
| Gold fact_checkins | `year`, `month` | Same pattern |

### Z-Ordering (Delta Lake)

Z-ordering co-locates related data within Delta files, dramatically reducing the amount of data scanned for filtered queries:

```sql
OPTIMIZE gold.fact_reviews ZORDER BY (business_id, user_id);
OPTIMIZE gold.dim_business ZORDER BY (city, state);
```

After Z-ordering, a query filtering by `business_id` scans ~5% of files instead of 100%.

### Joins at Gold (dbt + Databricks SQL)

Since Gold is built by dbt running on Databricks SQL Warehouse, join optimisation is handled by Databricks' query optimiser. Dimension tables (`dim_business`, `dim_user`, `dim_date`) are small enough that Databricks automatically applies broadcast join hints. For explicit control, dbt models can include Databricks-specific hints:

```sql
-- fact_reviews.sql (dbt model)
select /*+ BROADCAST(dim_business) */
    r.review_id,
    r.review_stars,
    b.name           as business_name,
    b.city,
    u.is_elite,
    d.year,
    d.month
from {{ ref('stg_reviews') }}       r
join {{ ref('dim_business') }}      b on r.business_id = b.business_id
join {{ ref('dim_user') }}          u on r.user_id      = u.user_id
join {{ ref('dim_date') }}          d on r.date_id      = d.date_id
```

### Peak vs Off-Peak Handling

| Condition | Strategy |
|---|---|
| **Peak load** (historical backfill, large batch) | Scale EMR core nodes from 3 → 10 via auto-scaling policy; increase `spark.sql.shuffle.partitions` from 200 → 800 |
| **Normal daily batch** | Fixed 3-node cluster; standard partition count |
| **Off-peak** | EMR cluster auto-terminates; S3 is the only running cost; Airflow MWAA scales to zero workers |

### Performance Benchmarks (Target SLAs)

| Pipeline Stage | Expected Duration | Data Volume |
|---|---|---|
| Bronze ingestion (all 5 entities) | < 15 minutes | ~10GB |
| Silver transformation | < 20 minutes | ~10GB |
| Gold star schema build | < 25 minutes | ~10GB aggregated |
| **Total end-to-end** | **< 60 minutes** | |

---

## 13. Real-Time Extension (Kafka)

The organisation has indicated a strategic direction toward real-time data capture. The batch architecture described above is designed to accommodate this extension without schema or storage redesign.

### Proposed Real-Time Architecture

```
[Live Yelp Events]
       │
       ▼
[Amazon MSK (Managed Kafka)]
  Topics: yelp.reviews, yelp.checkins, yelp.tips
       │
       ▼
[Spark Structured Streaming on EMR]
  - Micro-batch interval: 30 seconds
  - Checkpoint location: s3://yelp-platform/checkpoints/
  - Exactly-once delivery via Delta Lake MERGE
       │
       ▼
[Bronze Delta Tables]  ← Same tables, same schema, same S3 paths
  - Streaming writes append to existing partitions
  - Batch and streaming data coexist transparently
       │
       ▼
[Silver + Gold]
  - Silver runs as a streaming job (continuous mode)
  - Gold runs as a micro-batch (5-minute intervals for BI freshness)
```

### Key Design Decisions for Real-Time

**Why MSK (Managed Kafka) over Kinesis:**
Kafka is the industry standard for event streaming and provides consumer group semantics, topic compaction, and replay capabilities that Kinesis lacks. MSK eliminates Kafka cluster management while retaining the full Kafka API. Given existing Kafka experience on the team, operational familiarity is also a factor.

**Why Delta Lake enables batch + streaming coexistence:**
Because Bronze Delta tables use the same schema and S3 paths for both batch and streaming writes, the Silver and Gold pipelines don't need to know whether data arrived via batch or stream. This is the critical architectural advantage of Delta Lake in a mixed workload environment — a property that plain Parquet cannot provide due to lack of ACID semantics.

**Exactly-once semantics:**
Spark Structured Streaming with Delta Lake provides end-to-end exactly-once guarantees via checkpoint files stored on S3. If the streaming job crashes and restarts, it resumes from the last committed checkpoint with no duplicate records written.

---

## 14. Monitoring & Observability

### Pipeline Monitoring (Airflow)

- SLA miss alerts configured per DAG — any job exceeding its SLA triggers a PagerDuty/Slack alert
- DAG run history provides full audit trail of every pipeline execution
- Task-level retry logic: 3 retries with exponential backoff before failure alert

### Spark Job Monitoring (Spark UI / CloudWatch)

Key metrics monitored per job:

| Metric | Why It Matters |
|---|---|
| **Shuffle read/write bytes** | High shuffle = expensive joins or too many partitions |
| **Task duration variance** | High variance = data skew on a partition key |
| **GC time % per executor** | > 10% GC time = memory pressure, increase executor memory |
| **Stage input/output rows** | Unexpected row count changes signal data quality issues |

### Data Quality Monitoring

At each Silver transformation, row count assertions are logged:

```python
bronze_count = bronze_df.count()
silver_count = silver_df.count()
assert silver_count <= bronze_count, f"Silver row count ({silver_count}) exceeds Bronze ({bronze_count}) — dedup failure"
assert silver_count > 0, "Silver output is empty — pipeline failure"
```

These counts are written to a `pipeline_audit` Delta table for trend tracking over time.

### Storage Monitoring (CloudWatch)

- S3 bucket size per layer (Bronze/Silver/Gold) tracked daily
- Alerts on unexpected size growth (> 20% day-over-day = possible data duplication)
- Delta Lake `VACUUM` scheduled weekly to remove stale files and control storage costs

---

## 15. Security & Governance

### Access Control

- **IAM roles** — EMR cluster assumes a role with read access to source bucket and read/write access to Bronze/Silver/Gold buckets. Principle of least privilege enforced.
- **S3 bucket policies** — BI tools (Athena, Looker) have read-only access to Gold layer only. No direct access to Bronze or Silver.
- **MWAA** — Airflow runs in a private VPC subnet with no public internet exposure.

### Data Governance

- **Delta Lake transaction log** provides a complete, immutable audit trail of every write operation — who wrote what, when, and from which job.
- **Column-level PII handling** — `user.name` and `review.text` are flagged as PII in the Silver schema metadata. Future implementation would apply tokenisation before Gold layer promotion.
- **Retention policy** — Bronze retained for 90 days (raw data recovery window). Silver retained for 1 year. Gold retained indefinitely.

---

## 16. Trade-offs & Alternatives Considered

| Decision | Alternative | Why Alternative Was Not Chosen |
|---|---|---|
| Delta Lake | Apache Iceberg | Both are excellent. Delta chosen for tighter Spark integration and simpler operational model on EMR. Iceberg would be preferred if multi-engine support (Flink, Trino) was a hard requirement. |
| EMR | AWS Glue | Glue abstracts too much Spark configuration. For a performance-sensitive pipeline with large shuffle operations, direct Spark control on EMR is preferable. |
| EMR (Bronze/Silver) + Databricks (Gold) | EMR end-to-end | Databricks is the superior developer experience for dbt and SQL-layer work. Splitting by layer plays to each platform's strengths — EMR for heavy PySpark I/O, Databricks for structured SQL modelling. |
| dbt (Gold layer) | PySpark for all layers | PySpark at Gold means engineers write DataFrame code for what is fundamentally a SQL transformation problem. dbt gives analysts readability, built-in testing, and lineage documentation that PySpark cannot match at the modelling layer. |
| Star Schema (Gold) | Data Vault | Data Vault offers better auditability and historisation. Star schema chosen because the BI team's primary need is query performance and ease of use, not full temporal historisation. |
| Airflow (MWAA) | AWS Step Functions | Step Functions integrates natively with AWS services but lacks Airflow's rich ecosystem of operators, sensors, and community plugins. For a data team already fluent in Airflow, MWAA is the right managed offering. |
| Batch-first with Kafka extension | Full Lambda Architecture | Lambda architecture (separate batch + speed layers) creates two code paths to maintain. The Delta Lake + Structured Streaming approach converges on a single unified pipeline (Kappa-adjacent) that serves both batch and real-time without duplication. |

---

*End of Architecture Document*