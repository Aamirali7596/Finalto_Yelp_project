# Data Flow Diagram
## Yelp Data Engineering Platform — End-to-End Data Movement

---

```mermaid
flowchart TD
    A1[business.json] --> B
    A2[review.json]   --> B
    A3[user.json]     --> B
    A4[checkin.json]  --> B
    A5[tip.json]      --> B

    B["🟤 Bronze — Raw Landing Zone
    PySpark on EMR · Delta Lake on S3
    Partition by ingestion date · Append only
    Lookback 3 days to catch late-arriving data"]

    B -->|PySpark cleans & types| C

    C["🟡 Silver — Cleaned & Conformed
    Cast types · Remove duplicates
    Explode categories and checkins
    Derive is_elite flag · Partition by year / month"]

    C -->|dbt models on Databricks| D

    D["🟣 Gold — BI-Ready Star Schema
    fact_reviews · fact_checkins
    dim_business · dim_user · dim_date
    dbt tests on every run · Z-ordered for fast queries"]

    D --> E1[Amazon Athena]
    D --> E2[Looker / Tableau]
    D --> E3[Data science / ML]

    style B fill:#E1F5EE,stroke:#0F6E56,color:#085041
    style C fill:#E1F5EE,stroke:#0F6E56,color:#085041
    style D fill:#EEEDFE,stroke:#534AB7,color:#26215C
    style E1 fill:#F1EFE8,stroke:#5F5E5A,color:#2C2C2A
    style E2 fill:#F1EFE8,stroke:#5F5E5A,color:#2C2C2A
    style E3 fill:#F1EFE8,stroke:#5F5E5A,color:#2C2C2A
```

---

## Airflow Orchestration Chain

```mermaid
flowchart LR
    A[dag_bronze_ingestion\n06:00 UTC daily] -->|on SUCCESS| B[dag_silver_transform]
    B -->|on SUCCESS| C[dag_gold_dbt]
    C --> D[dbt run]
    D --> E[dbt test]
    E --> F[dbt docs generate]

    style A fill:#EEEDFE,stroke:#534AB7,color:#26215C
    style B fill:#EEEDFE,stroke:#534AB7,color:#26215C
    style C fill:#EEEDFE,stroke:#534AB7,color:#26215C
```

**If any stage fails, the chain stops.** Silver never runs if Bronze fails. Gold never runs if Silver fails. BI is never served stale or invalid data.

---

## Future State — Real-Time Extension

```mermaid
flowchart LR
    K[Kafka MSK\nyelp.reviews topic] -->|30s micro-batch| S[Spark Structured Streaming]
    S -->|exactly-once via Delta MERGE| B[Bronze Delta tables\nsame schema · same S3 paths]
    B --> SIL[Silver → Gold\nunchanged pipeline]

    style K fill:#FAEEDA,stroke:#BA7517,color:#412402
    style S fill:#FAEEDA,stroke:#BA7517,color:#412402
    style B fill:#E1F5EE,stroke:#0F6E56,color:#085041
    style SIL fill:#EEEDFE,stroke:#534AB7,color:#26215C
```

Batch and streaming write to the same Bronze Delta tables. Downstream Silver and Gold pipelines are source-agnostic — they don't know or care whether data arrived via file or Kafka.

---

*End of Data Flow Document*