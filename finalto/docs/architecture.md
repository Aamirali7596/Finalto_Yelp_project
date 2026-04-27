# Architecture Design Document
## Yelp Data Engineering Platform
**Author:** Aamir | **Version:** 4.0 | **Last Updated:** April 2026

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Architecture Principles](#2-architecture-principles)
3. [System Architecture Overview](#3-system-architecture-overview)
4. [Tech Stack & Justification](#4-tech-stack--justification)
5. [Medallion Layer Responsibilities](#5-medallion-layer-responsibilities)
6. [Orchestration Design](#6-orchestration-design)
7. [Real-Time Extension](#7-real-time-extension)
8. [Trade-offs & Alternatives Considered](#8-trade-offs--alternatives-considered)

> **Related Documents**
> - Data Engineering (Ingestion, Transformation, Scalability) → `data_engineering/data_engineering.md`
> - SQL Design → `data_engineering/sql_design_task.md`
> - ERD Diagram → `docs/diagrams/erd_diagram.md`
> - Data Lineage → `docs/diagrams/data_lineage_diagram.md`
> - Data Flow Diagram → `docs/diagrams/data_flow_diagram.md`

---

## 1. Executive Summary

This document describes the architectural decisions behind the Yelp Data Engineering Platform. The platform ingests the Yelp dataset, transforms it through a four-layer architecture, and serves pre-joined reporting views to downstream BI tools.

**The stack in one line:**
> Raw JSON on S3 → PySpark on EMR (Bronze + Silver) → dbt on Databricks (Gold + Reporting) → Delta Lake on S3 → Athena / BI Tools — orchestrated end-to-end by Apache Airflow.

**Compute is split deliberately by layer:**

| Layer | Tool | Reason |
|---|---|---|
| Bronze + Silver | PySpark on EMR | Heavy I/O, JSON parsing, large-scale distributed cleaning |
| Gold | dbt on Databricks | Star schema modelling, business logic, incremental builds, testing |
| Reporting | dbt views on Databricks | Pre-joined BI-ready views — zero storage cost, always fresh |
| Serving | Amazon Athena | Serverless ad-hoc SQL directly on S3 Delta tables |

---

## 2. Architecture Principles

Every design decision in this platform traces back to one of these five principles.

**P1 — Decouple Storage from Compute**
All data lives on S3. Compute clusters (EMR, Databricks) are ephemeral — spun up on demand, terminated on completion. No data is at risk when a cluster goes down.

**P2 — Schema-on-Write**
Schemas are defined explicitly in code and enforced at write time. Silent type mismatches never reach the BI layer. A broken schema fails loudly in the pipeline, not quietly in a dashboard.

**P3 — Idempotent Pipelines**
Every job can be re-run safely for the same input date without duplicating data. Bronze uses dynamic partition overwrite. Silver and Gold use Delta Lake `MERGE` semantics.

**P4 — Build for the BI Consumer**
The Reporting layer is the final interface for analysts. Views are pre-joined, named in business terms, and always reflect the latest Gold data. BI teams never write their own joins against raw fact tables.

**P5 — Design for Real-Time from Day One**
The batch architecture is built so that adding Kafka + Spark Structured Streaming requires no schema or storage redesign. Delta Lake's unified batch/streaming semantics make this possible.

---

## 3. System Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES                                 │
│                                                                       │
│   Yelp JSON Files (S3 Landing)      Future: Kafka Topics (MSK)      │
│   business / review / user /        yelp.reviews / yelp.checkins    │
│   checkin / tip                     (real-time event stream)         │
└───────────────────┬──────────────────────────┬────────────────────────┘
                    │ batch                    │ streaming (future)
                    ▼                          ▼
┌──────────────────────────────────────────────────────────────────────┐
│              BRONZE + SILVER  —  PySpark on Amazon EMR               │
│                                                                       │
│  Bronze                            Silver                            │
│  ─────────────────────             ──────────────────────────        │
│  Read raw JSON from S3             Enforce explicit schemas          │
│  Apply StructType schemas          Cast types, handle nulls          │
│  Write to Delta (append-only)      Explode categories & checkins     │
│  Partition by ingestion_date       Deduplicate on natural keys       │
│  Lookback 3 days per run           Partition by year / month         │
└───────────────────────────────────────┬──────────────────────────────┘
                                        │ Silver Delta tables (S3)
                                        ▼
┌──────────────────────────────────────────────────────────────────────┐
│                   GOLD  —  dbt on Databricks                         │
│                                                                       │
│  Staging (stg_*)       Intermediate (int_*)       Marts              │
│  ─────────────────     ────────────────────       ───────────────    │
│  Rename columns        Business logic             fact_reviews       │
│  Source freshness      Rising star calc           fact_checkins      │
│  checks (views)        Elite user flags           dim_business       │
│                        Date spine joins           dim_user           │
│                        (ephemeral CTEs)           dim_date           │
│                                                                       │
│  dbt tests on every build → not_null, unique, relationships          │
│  dbt docs auto-generated  → full column-level lineage graph          │
└───────────────────────────────────────┬──────────────────────────────┘
                                        │ Gold Delta tables (S3)
                                        ▼
┌──────────────────────────────────────────────────────────────────────┐
│                REPORTING  —  dbt views on Databricks                 │
│                                                                       │
│  vw_rising_stars              Pre-joined · Always fresh              │
│  vw_top_businesses_by_city    Zero storage cost                      │
│  vw_review_trends             BI tools query these directly          │
│                               No raw fact table joins needed         │
└───────────────────────────────────────┬──────────────────────────────┘
                                        │
                                        ▼
┌──────────────────────────────────────────────────────────────────────┐
│                          SERVING LAYER                                │
│                                                                       │
│   Amazon Athena                    BI Tools                          │
│   Ad-hoc SQL on S3 Delta tables    Looker / Tableau / QuickSight     │
│   Serverless, pay-per-query        Reads via Athena / JDBC           │
└──────────────────────────────────────────────────────────────────────┘
                                        │
                       ┌────────────────┴─────────────────┐
                       │       ORCHESTRATION & OPS         │
                       │                                   │
                       │   Apache Airflow (MWAA)           │
                       │   ├─ dag_bronze  (EMR operators)  │
                       │   ├─ dag_silver  (EMR operators)  │
                       │   └─ dag_gold    (dbt operators)  │
                       │      incl. reporting views        │
                       │                                   │
                       │   AWS CloudWatch                  │
                       │   EMR metrics / S3 storage alerts │
                       └───────────────────────────────────┘
```

---

## 4. Tech Stack & Justification

### 4.1 Amazon S3 — Storage
The single storage layer for all Medallion tiers. Decouples storage from compute — any engine (EMR, Databricks, Athena) reads from the same S3 path with no data movement. 11 nines durability, zero operational overhead.

**Rejected:** HDFS on EMR — data is coupled to the cluster and lost on termination.

---

### 4.2 Delta Lake — Table Format
Sits on top of S3 Parquet files and adds the transaction guarantees plain Parquet cannot provide:

| Capability | Why It Matters |
|---|---|
| ACID transactions | BI never sees a partial pipeline write |
| Schema enforcement | Wrong types fail at write time, not in dashboards |
| Time travel | Query any historical state via `VERSION AS OF` |
| Efficient upserts | `MERGE` handles late-arriving data without full rewrites |

**Rejected:** Plain Parquet — no ACID guarantees.
**Considered:** Apache Iceberg — equally strong, Delta chosen for tighter Spark/Databricks integration.

---

### 4.3 PySpark on Amazon EMR — Bronze & Silver Compute
`review.json` alone is ~5.5GB. PySpark distributes processing across the cluster in parallel. EMR gives direct Spark configuration access — executor memory, partition counts, broadcast thresholds — needed for tuning large shuffle operations at Silver.

**Rejected:** AWS Glue — too much abstraction, DynamicFrame adds complexity with no benefit here.
**Considered:** Databricks for all layers — reserved for Gold/Reporting where SQL Warehouse is the better fit.

---

### 4.4 dbt on Databricks — Gold & Reporting
Gold and Reporting are SQL transformation and modelling problems. dbt is the right tool:

- Every model is a plain `.sql` file — readable by analysts, not just engineers
- Built-in `not_null`, `unique`, `relationships` tests run on every `dbt run`
- Auto-generated lineage graph from source JSON to final reporting view
- `fact_reviews` and `fact_checkins` materialised as `incremental` — only new rows per run
- Reporting views materialised as `view` — zero storage cost, always reflect latest Gold data

Runs on a **Databricks SQL Warehouse (serverless)** — zero idle cost, auto-scales.

**Rejected:** PySpark for Gold — DataFrame code for a SQL problem. Removes analyst accessibility.

---

### 4.5 Apache Airflow on MWAA — Orchestration
DAG-based model maps naturally to the four-layer pipeline. Native EMR operators handle cluster lifecycle. dbt runs triggered via `BashOperator` or `DbtCloudRunJobOperator`. Full retry logic, SLA alerts, and run history.

**Rejected:** AWS Step Functions — lacks Airflow's operator ecosystem and dbt integration.

---

### 4.6 Amazon Athena — Serving
Serverless SQL on S3 Delta tables. Zero infrastructure. Used for validation by engineers and as a query engine by BI tools. Reads Delta Lake transaction log natively via the Delta Lake connector.

---

## 5. Medallion Layer Responsibilities

| Concern | Bronze | Silver | Gold | Reporting |
|---|---|---|---|---|
| **Tool** | PySpark / EMR | PySpark / EMR | dbt / Databricks | dbt / Databricks |
| **Input** | Raw JSON (S3 landing) | Bronze Delta tables | Silver Delta tables | Gold Delta tables |
| **Output** | Delta tables (raw) | Delta tables (clean) | Delta tables (star schema) | SQL views (no storage) |
| **Schema** | Loosely applied | Explicit, enforced | Business-friendly, tested | Pre-joined, analyst-ready |
| **Transforms** | None | Typing, dedup, flatten | Joins, aggregations, logic | Pre-join facts + dims |
| **Materialisation** | Delta table | Delta table | Incremental / table | View (always fresh) |
| **Partition key** | `ingestion_date` | `year` / `month` | `year` / `month` | N/A — views |
| **Who reads it** | Silver only | Gold (dbt) only | Reporting views | BI tools / Athena |
| **Data quality** | None | Row count assertions | dbt schema tests | dbt schema tests |
| **Retention** | 90 days | 1 year | Indefinite | N/A — no storage |

---

## 6. Orchestration Design

Three Airflow DAGs with hard upstream dependencies. Silver cannot start until Bronze succeeds. Gold cannot start until Silver succeeds. Reporting views run after Gold mart tests pass.

```
dag_bronze_ingestion          ← schedule: daily 06:00 UTC
├── ingest_business
├── ingest_reviews             ← largest job, 200 shuffle partitions
├── ingest_users
├── ingest_checkins
└── ingest_tips
        │
        │  ExternalTaskSensor — waits for Bronze SUCCESS
        ▼
dag_silver_transform          ← triggered on Bronze completion
├── transform_reviews_silver
├── transform_business_silver
├── transform_users_silver
└── transform_checkins_silver
        │
        │  ExternalTaskSensor — waits for Silver SUCCESS
        ▼
dag_gold_dbt                  ← triggered on Silver completion
├── dbt_run_staging            ← stg_* views on Silver
├── dbt_run_intermediate       ← int_* business logic (ephemeral)
├── dbt_run_marts              ← fact_* and dim_* star schema
├── dbt_run_reporting          ← vw_* reporting views on Gold
├── dbt_test                   ← all schema.yml tests — fails DAG on error
└── dbt_docs_generate          ← refresh published lineage docs
```

**Key operational decisions:**
- EMR clusters are ephemeral — created at DAG start, auto-terminated on completion
- Databricks SQL Warehouse is serverless — zero idle cost between dbt runs
- Reporting views build after mart tests pass — BI never gets unvalidated data
- All tasks have 3 retries with exponential backoff before alerting on-call

---

## 7. Real-Time Extension

The organisation has signalled a move toward real-time data capture. The batch architecture above requires no schema or storage redesign to support streaming — this was a deliberate design choice from day one (Principle P5).

**How streaming slots in:**

```
[Live Yelp Events]
        │
        ▼
[Amazon MSK — Managed Kafka]
  Topics: yelp.reviews / yelp.checkins / yelp.tips
        │
        ▼
[Spark Structured Streaming on EMR]
  Micro-batch interval : 30 seconds
  Checkpoint location  : s3://yelp-platform/checkpoints/
  Delivery guarantee   : exactly-once via Delta Lake MERGE
        │
        ▼
[Bronze Delta Tables]
  Same schema. Same S3 paths. Batch and streaming coexist transparently.
        │
        ▼
[Silver → Gold → Reporting]
  Unchanged. dbt runs on whatever Silver contains — source agnostic.
  Reporting views reflect streaming data as soon as Gold is refreshed.
```

**Why no redesign is needed:**
Delta Lake's transaction log handles concurrent batch and streaming writes to the same table safely. The downstream Silver, Gold, and Reporting layers are entirely unaware of whether data arrived via batch file or Kafka stream. This is the **Kappa architecture pattern** — one unified pipeline serving both modes.

**Why MSK over Kinesis:**
Full Kafka API compatibility, consumer group semantics, topic compaction, and replay capability. MSK removes cluster management overhead while keeping the Kafka ecosystem intact.

---

## 8. Trade-offs & Alternatives Considered

| Decision | Alternative | Reason for Current Choice |
|---|---|---|
| Delta Lake | Apache Iceberg | Tighter Spark/Databricks integration. Iceberg preferred if Flink or Trino support is required. |
| EMR (Bronze/Silver) | AWS Glue | Glue abstracts too much Spark config. Direct EMR control needed for large shuffle tuning. |
| EMR + Databricks split | Databricks end-to-end | Each platform plays to its strengths — EMR for heavy PySpark I/O, Databricks SQL Warehouse for dbt modelling. |
| dbt (Gold + Reporting) | PySpark for Gold | SQL modelling belongs in a SQL tool. dbt adds testing, lineage docs, and analyst accessibility that PySpark cannot match at the modelling layer. |
| Reporting as views | Reporting as tables | Views are always fresh and cost zero storage. Tables would require an extra incremental build step with no meaningful performance benefit at current query volume. |
| Star schema (Gold) | Data Vault | Star schema optimises for BI query performance. Data Vault preferred if full temporal historisation is a hard requirement. |
| Athena (serving) | Redshift Spectrum | Athena is zero infrastructure and pays per query — right for current volume. Redshift Spectrum is the natural next step when concurrent BI users exceed ~10 or dashboard load times degrade. |
| Airflow MWAA | AWS Step Functions | Richer operator ecosystem, native EMR and dbt integration, existing team familiarity. |
| Kappa — unified pipeline | Lambda — separate batch + speed layers | Lambda creates two code paths to maintain. Delta Lake's unified batch/streaming semantics make a single pipeline viable and simpler. |

---

*End of Architecture Document*
*For implementation detail, ingestion design, transformation logic, and scalability — see `data_engineering/data_engineering.md`*