# Data Flow Diagram
## Yelp Data Engineering Platform — End-to-End Data Movement
**Author:** Aamir | **Version:** 1.0 | **Last Updated:** April 2026

---

> **Related Documents**
> - Architecture decisions → `docs/architecture.md`
> - Field-level lineage → `docs/data_lineage.md`
> - Entity relationships → `docs/erd.md`

---

## Overview

The data flow follows a strict top-down path through five stages. Data never moves backwards. Each stage has a single responsibility and a clear handoff contract to the next.

```
SOURCE → INGEST → BRONZE → SILVER → GOLD → SERVE
```

No stage can be skipped. Silver cannot read from Source directly. Gold cannot read from Bronze directly. This enforces clean separation of concerns and makes debugging straightforward — if something is wrong in Gold, you check Silver. If Silver looks wrong, you check Bronze.

---

## End-to-End Data Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                      SOURCE — S3 Landing Bucket                      │
│                                                                       │
│   business.json   review.json   user.json   checkin.json   tip.json  │
│   (~120MB)        (~5.5GB)      (~3.5GB)    (~290MB)       (~240MB)  │
└──────────────────────────────────┬──────────────────────────────────┘
                                   │ Airflow dag_bronze_ingestion triggers
                                   │ EmrCreateJobFlowOperator → EMR cluster up
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│               INGESTION — PySpark on Amazon EMR                       │
│                                                                       │
│  1. Read raw JSON files from S3 landing bucket                       │
│  2. Apply explicit StructType schemas (no inferSchema)               │
│  3. Write to Bronze Delta tables, partitioned by ingestion_date      │
│  4. EMR cluster auto-terminates                                      │
│  5. Airflow marks SUCCESS → triggers Silver sensor                   │
└──────────────────────────────────┬──────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│              BRONZE — Delta Lake on S3                                │
│                                                                       │
│  Purpose : Preserve raw source data exactly as received              │
│  Format  : Delta Lake (ACID, append-only)                            │
│  Partition: ingestion_date=YYYY-MM-DD                                │
│  Retained : 90 days                                                  │
│                                                                       │
│  Tables:                                                             │
│  ├── bronze.business                                                 │
│  ├── bronze.reviews                                                  │
│  ├── bronze.users                                                    │
│  ├── bronze.checkins                                                 │
│  └── bronze.tips                                                     │
│                                                                       │
│  S3 path: s3://yelp-platform/bronze/{entity}/ingestion_date={date}/  │
└──────────────────────────────────┬──────────────────────────────────┘
                                   │ Airflow dag_silver_transform triggers
                                   │ ExternalTaskSensor waits for Bronze SUCCESS
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│               TRANSFORMATION — PySpark on Amazon EMR                  │
│                                                                       │
│  Per entity:                                                         │
│  ├── Cast all types explicitly (string dates → TimestampType)        │
│  ├── Handle nulls per documented rules                               │
│  ├── Deduplicate on natural keys (review_id, business_id, user_id)   │
│  ├── Explode categories string → bridge_business_categories          │
│  ├── Explode checkin date blob → individual checkin_events rows      │
│  └── Parse user.elite string → is_elite boolean + elite_years_count  │
└──────────────────────────────────┬──────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│              SILVER — Delta Lake on S3                                │
│                                                                       │
│  Purpose : Cleaned, typed, conformed data ready for modelling        │
│  Format  : Delta Lake (MERGE semantics for upserts)                  │
│  Partition: year=YYYY/month=MM                                       │
│  Retained : 1 year                                                   │
│                                                                       │
│  Tables:                                                             │
│  ├── silver.business                                                 │
│  ├── silver.business_categories  ← exploded from categories string  │
│  ├── silver.reviews                                                  │
│  ├── silver.users                                                    │
│  ├── silver.checkin_events       ← exploded from date blob           │
│  └── silver.tips                                                     │
│                                                                       │
│  S3 path: s3://yelp-platform/silver/{entity}/year={y}/month={m}/    │
└──────────────────────────────────┬──────────────────────────────────┘
                                   │ Airflow dag_gold_dbt triggers
                                   │ ExternalTaskSensor waits for Silver SUCCESS
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│               MODELLING — dbt on Databricks SQL Warehouse             │
│                                                                       │
│  Three model sub-layers (Airflow runs them in sequence):             │
│                                                                       │
│  stg_* (staging)                                                     │
│  ├── stg_reviews      — rename cols, light casting, freshness check  │
│  ├── stg_business     — rename cols, select relevant fields          │
│  ├── stg_users        — rename cols, derive is_elite flag            │
│  └── stg_checkins     — rename cols, extract hour/day_of_week        │
│                                                                       │
│  int_* (intermediate)                                                │
│  ├── int_reviews_with_dates       — join to date spine               │
│  ├── int_business_categories      — flatten for primary_category     │
│  └── int_rising_star_candidates   — avg stars recent vs historical   │
│                                                                       │
│  mart (gold tables)                                                  │
│  ├── fact_reviews    — materialised: incremental                     │
│  ├── fact_checkins   — materialised: incremental                     │
│  ├── dim_business    — materialised: table (SCD Type 1)              │
│  ├── dim_user        — materialised: table (SCD Type 1)              │
│  └── dim_date        — materialised: table (static date spine)       │
│                                                                       │
│  After dbt run: dbt test runs all schema.yml tests.                  │
│  Failure blocks the DAG — BI never receives invalid data.            │
└──────────────────────────────────┬──────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│              GOLD — Delta Lake on S3                                  │
│                                                                       │
│  Purpose : BI-ready star schema, tested, documented                  │
│  Format  : Delta Lake, Z-ordered on business_id / user_id / date     │
│  Partition: year=YYYY/month=MM                                       │
│  Retained : Indefinite                                               │
│                                                                       │
│  Tables:                                                             │
│  ├── gold.fact_reviews                                               │
│  ├── gold.fact_checkins                                              │
│  ├── gold.dim_business                                               │
│  ├── gold.dim_user                                                   │
│  └── gold.dim_date                                                   │
│                                                                       │
│  S3 path: s3://yelp-platform/gold/{table}/year={y}/month={m}/       │
└──────────────────────────────────┬──────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│                          SERVING LAYER                                │
│                                                                       │
│  Amazon Athena         — ad-hoc SQL on Gold Delta tables             │
│  Looker / Tableau      — dashboards via Athena/JDBC connector        │
│  Data scientists / ML  — feature engineering from Gold tables        │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Airflow Orchestration — Trigger Chain

The three DAGs form a strict dependency chain. A failure at any stage stops the chain — Silver will never run if Bronze fails, Gold will never run if Silver fails.

```
dag_bronze_ingestion  (06:00 UTC daily)
  └── on SUCCESS → ExternalTaskSensor releases dag_silver_transform
        └── on SUCCESS → ExternalTaskSensor releases dag_gold_dbt
              └── dbt run → dbt test → dbt docs generate
```

**Retry behaviour:** Every task retries 3 times with exponential backoff (1min, 2min, 4min) before the DAG is marked FAILED and on-call is paged.

**Idempotency:** Every job is safe to re-run. Re-running Bronze for a given `ingestion_date` only overwrites that date's partition. Re-running Silver uses Delta MERGE — no duplicates. Re-running dbt models re-processes only new rows for incremental models.

---

## Future State — Real-Time Flow

When the Kafka extension is activated, the batch Bronze ingestion is supplemented by a streaming path. Silver and Gold are unchanged.

```
[Kafka MSK topics]
  yelp.reviews / yelp.checkins / yelp.tips
        │
        ▼
[Spark Structured Streaming — EMR]
  30-second micro-batch
  Checkpoint: s3://yelp-platform/checkpoints/
  Exactly-once via Delta MERGE
        │
        ▼
[Bronze Delta tables — same tables, same S3 paths as batch]
  Streaming writes coexist with batch writes transparently
        │
        ▼
[Silver → Gold — unchanged]
```

---

*End of Data Flow Document*