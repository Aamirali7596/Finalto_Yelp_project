# Data Lineage
## Yelp Data Engineering Platform — Source to Gold Traceability

---

## Table-Level Lineage

```mermaid
flowchart LR
    subgraph SRC [Source JSON]
        s1[business.json]
        s2[review.json]
        s3[user.json]
        s4[checkin.json]
        s5[tip.json]
    end

    subgraph BRZ [Bronze]
        b1[bronze.business]
        b2[bronze.reviews]
        b3[bronze.users]
        b4[bronze.checkins]
        b5[bronze.tips]
    end

    subgraph SLV [Silver]
        sv1[silver.business]
        sv2[silver.business_categories]
        sv3[silver.reviews]
        sv4[silver.users]
        sv5[silver.checkin_events]
        sv6[silver.tips]
    end

    subgraph GLD [Gold]
        g1[dim_business]
        g2[fact_reviews]
        g3[dim_user]
        g4[fact_checkins]
        g5[dim_date]
    end

    s1 --> b1 --> sv1 --> g1
    s1 --> b1 --> sv2 --> g1
    s2 --> b2 --> sv3 --> g2
    s3 --> b3 --> sv4 --> g3
    s4 --> b4 --> sv5 --> g4
    s5 --> b5 --> sv6

    style SRC fill:#F1EFE8,stroke:#5F5E5A,color:#2C2C2A
    style BRZ fill:#E1F5EE,stroke:#0F6E56,color:#085041
    style SLV fill:#E1F5EE,stroke:#0F6E56,color:#085041
    style GLD fill:#EEEDFE,stroke:#534AB7,color:#26215C
```

---

## Key Transformation Lineage

```mermaid
flowchart LR
    subgraph RAW [Raw source field]
        r1["business.categories\n'Restaurants, Italian, Pizza'"]
        r2["checkin.date\n'ts1, ts2, ts3, ...'"]
        r3["user.elite\n'2015,2016,2017'"]
        r4["review.stars + review.date"]
    end

    subgraph TRF [Silver transformation]
        t1["split + explode\n→ 3 rows"]
        t2["split + explode\n→ 1 row per event"]
        t3["parse string\n→ is_elite boolean"]
        t4["split by date window\nrecent vs historical"]
    end

    subgraph GLD [Gold output]
        g1["dim_business\nprimary_category"]
        g2["fact_checkins\ncheckin_hour, day_of_week"]
        g3["dim_user\nis_elite, elite_years_count"]
        g4["int_rising_star_candidates\nrating_improvement"]
    end

    r1 --> t1 --> g1
    r2 --> t2 --> g2
    r3 --> t3 --> g3
    r4 --> t4 --> g4

    style RAW fill:#F1EFE8,stroke:#5F5E5A,color:#2C2C2A
    style TRF fill:#E1F5EE,stroke:#0F6E56,color:#085041
    style GLD fill:#EEEDFE,stroke:#534AB7,color:#26215C
```

---

## Rising Star Metric — Full Lineage

```mermaid
flowchart TD
    A["review.stars\nreview.date\nSource: review.json"] -->|cast + validate at Silver| B[silver.reviews]
    B -->|WHERE reviewed_at >= 1 year ago\nAVG stars · COUNT reviews| C[avg_stars_recent\nreview_count_recent]
    B -->|WHERE reviewed_at < 1 year ago\nAVG stars| D[avg_stars_historical]
    C -->|INNER JOIN on business_id| E{Filter}
    D -->|INNER JOIN on business_id| E
    E -->|review_count_recent >= 10\nAND recent avg >= historical avg + 1.0| F[int_rising_star_candidates\nbusiness_id · rating_improvement]
    F -->|JOIN dim_business for name| G[Rising star report\nbusiness_id · name · improvement]

    style A fill:#F1EFE8,stroke:#5F5E5A,color:#2C2C2A
    style B fill:#E1F5EE,stroke:#0F6E56,color:#085041
    style C fill:#E1F5EE,stroke:#0F6E56,color:#085041
    style D fill:#E1F5EE,stroke:#0F6E56,color:#085041
    style E fill:#FAEEDA,stroke:#BA7517,color:#412402
    style F fill:#EEEDFE,stroke:#534AB7,color:#26215C
    style G fill:#EEEDFE,stroke:#534AB7,color:#26215C
```

---

*End of Data Lineage Document*