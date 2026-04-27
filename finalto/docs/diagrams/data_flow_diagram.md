# Data Flow Diagram
## Yelp Data Engineering Platform — End-to-End Data Movement

---

## Main Pipeline Flow

```mermaid
flowchart TD
    A1[business.json] --> B
    A2[review.json]   --> B
    A3[user.json]     --> B
    A4[checkin.json]  --> B
    A5[tip.json]      --> B

    B["Bronze — Raw Data Landing Zone
    ─────────────────────────────
    PySpark on EMR · Delta Lake on S3
    Partitioned by ingestion date · Append only
    Lookback 3 days to catch late-arriving data
    Freshness checks · row count validation
    90 day retention"]

    B -->|"PySpark cleans and types"| C

    C["Silver — Cleaned and Conformed
    ─────────────────────────────
    Cast all types · Remove duplicates
    Explode categories string into rows
    Explode checkin date blob into events
    Derive is_elite boolean flag
    Partitioned by year and month · 1 year retention"]

    C -->|"dbt staging + intermediate"| D

    D["Gold — Star Schema
    ─────────────────────────────
    fact_reviews · fact_checkins
    dim_business · dim_user · dim_date
    dbt tests on every run
    Incremental builds · Z-ordered for fast queries
    Indefinite retention"]

    D -->|"dbt reporting views"| E

    E["Reporting — BI-Ready Views
    ─────────────────────────────
    vw_rising_stars
    vw_top_businesses_by_city
    vw_review_trends
    Always fresh · no extra storage cost"]

    E --> F1[Amazon Athena\nAd-hoc SQL on S3]
    E --> F2[Looker / Tableau\nBI dashboards]
    E --> F3[Data science\nML and features]

    style B fill:#E1F5EE,stroke:#085041,color:#04342C
    style C fill:#E1F5EE,stroke:#085041,color:#04342C
    style D fill:#EEEDFE,stroke:#3C3489,color:#26215C
    style E fill:#EEEDFE,stroke:#3C3489,color:#26215C
    style F1 fill:#F1EFE8,stroke:#5F5E5A,color:#2C2C2A
    style F2 fill:#F1EFE8,stroke:#5F5E5A,color:#2C2C2A
    style F3 fill:#F1EFE8,stroke:#5F5E5A,color:#2C2C2A
```

---

## Airflow Orchestration Chain

```mermaid
flowchart LR
    A["dag_bronze_ingestion
    06:00 UTC daily"] -->|"on SUCCESS"| B["dag_silver_transform
    triggered after Bronze"]
    B -->|"on SUCCESS"| C["dag_gold_dbt
    triggered after Silver"]
    C --> D[dbt run staging]
    D --> E[dbt run intermediate]
    E --> F[dbt run marts]
    F --> G[dbt run reporting]
    G --> H[dbt test]
    H --> I[dbt docs generate]

    style A fill:#EEEDFE,stroke:#3C3489,color:#26215C
    style B fill:#EEEDFE,stroke:#3C3489,color:#26215C
    style C fill:#EEEDFE,stroke:#3C3489,color:#26215C
```

> If any stage fails the chain stops. BI is never served stale or invalid data.

---

## Real-Time Extension — Kafka

```mermaid
flowchart LR
    K["Apache Kafka on MSK
    Topics: yelp.reviews
    yelp.checkins · yelp.tips"] -->|"30 second micro-batch"| S["Spark Structured Streaming
    Exactly-once delivery
    via Delta Lake MERGE"]

    S -->|"same schema · same S3 paths"| B["Bronze Delta Tables
    Batch and streaming coexist
    transparently"]

    B --> SIL["Silver → Gold → Reporting
    Unchanged pipeline
    Source agnostic"]

    style K fill:#FAEEDA,stroke:#854F0B,color:#412402
    style S fill:#FAEEDA,stroke:#854F0B,color:#412402
    style B fill:#E1F5EE,stroke:#085041,color:#04342C
    style SIL fill:#EEEDFE,stroke:#3C3489,color:#26215C
```

---

*End of Data Flow Document*