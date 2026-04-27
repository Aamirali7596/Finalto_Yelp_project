# Data Lineage
## Yelp Data Engineering Platform — Source to Reporting Traceability

---

## Table-Level Lineage

```mermaid
flowchart LR
    subgraph SRC ["Source JSON"]
        s1[business.json]
        s2[review.json]
        s3[user.json]
        s4[checkin.json]
        s5[tip.json]
    end

    subgraph BRZ ["Bronze — PySpark on EMR"]
        b1[bronze.business]
        b2[bronze.reviews]
        b3[bronze.users]
        b4[bronze.checkins]
        b5[bronze.tips]
    end

    subgraph SLV ["Silver — PySpark on EMR"]
        sv1[silver.business]
        sv2[silver.business_categories]
        sv3[silver.reviews]
        sv4[silver.users]
        sv5[silver.checkin_events]
    end

    subgraph GLD ["Gold — dbt on Databricks"]
        g1[dim_business]
        g2[fact_reviews]
        g3[dim_user]
        g4[fact_checkins]
        g5[dim_date]
    end

    subgraph RPT ["Reporting — dbt views"]
        r1[vw_rising_stars]
        r2[vw_top_businesses_by_city]
        r3[vw_review_trends]
    end

    s1 --> b1 --> sv1 --> g1
    s1 --> b1 --> sv2 --> g1
    s2 --> b2 --> sv3 --> g2
    s3 --> b3 --> sv4 --> g3
    s4 --> b4 --> sv5 --> g4
    g2 --> r1
    g1 --> r1
    g2 --> r2
    g1 --> r2
    g2 --> r3

    style SRC fill:#F1EFE8,stroke:#444441,color:#2C2C2A
    style BRZ fill:#E1F5EE,stroke:#085041,color:#04342C
    style SLV fill:#9FE1CB,stroke:#085041,color:#04342C
    style GLD fill:#EEEDFE,stroke:#3C3489,color:#26215C
    style RPT fill:#DDD9FC,stroke:#3C3489,color:#26215C
```

---

## Key Field Transformations

```mermaid
flowchart LR
    subgraph RAW ["Raw source field"]
        r1["business.categories
        'Restaurants, Italian, Pizza'"]
        r2["checkin.date
        'ts1, ts2, ts3 ...'"]
        r3["user.elite
        '2015,2016,2017'"]
        r4["review.stars
        review.date"]
    end

    subgraph TRF ["Silver transformation"]
        t1["split + explode
        3 separate rows"]
        t2["split + explode
        1 row per checkin event"]
        t3["parse string
        is_elite boolean + count"]
        t4["cast + validate
        DoubleType · TimestampType"]
    end

    subgraph GLD ["Gold output"]
        g1["dim_business
        primary_category"]
        g2["fact_checkins
        checkin_hour · day_of_week"]
        g3["dim_user
        is_elite · elite_years_count"]
        g4["fact_reviews
        review_stars · reviewed_at"]
    end

    subgraph RPT ["Reporting views"]
        rp1["vw_rising_stars
        rating_improvement"]
        rp2["vw_top_businesses_by_city
        avg_rating · total_reviews"]
        rp3["vw_review_trends
        monthly counts + avg rating"]
    end

    r1 --> t1 --> g1 --> rp1
    r1 --> t1 --> g1 --> rp2
    r2 --> t2 --> g2
    r3 --> t3 --> g3
    r4 --> t4 --> g4 --> rp1
    r4 --> t4 --> g4 --> rp2
    r4 --> t4 --> g4 --> rp3

    style RAW fill:#F1EFE8,stroke:#444441,color:#2C2C2A
    style TRF fill:#E1F5EE,stroke:#085041,color:#04342C
    style GLD fill:#EEEDFE,stroke:#3C3489,color:#26215C
    style RPT fill:#DDD9FC,stroke:#3C3489,color:#26215C
```

---

## Rising Star Metric — Full Lineage

```mermaid
flowchart TD
    A["review.stars and review.date
    Source: review.json"] -->|"cast and validate at Silver"| B["silver.reviews"]

    B -->|"stg_reviews — rename cols"| C["reviewed_at · review_stars"]

    C -->|"WHERE reviewed_at >= 1 year ago
    AVG stars · COUNT reviews"| D["avg_stars_recent
    review_count_recent"]

    C -->|"WHERE reviewed_at < 1 year ago
    AVG stars"| E["avg_stars_historical"]

    D -->|"INNER JOIN on business_id"| F{"Apply filters"}
    E -->|"INNER JOIN on business_id"| F

    F -->|"review_count >= 10
    AND recent avg >= historical + 1.0"| G["int_rising_star_candidates
    business_id · rating_improvement"]

    G -->|"JOIN dim_business"| H["vw_rising_stars
    business_name · city · improvement_tier"]

    style A fill:#F1EFE8,stroke:#444441,color:#2C2C2A
    style B fill:#9FE1CB,stroke:#085041,color:#04342C
    style C fill:#9FE1CB,stroke:#085041,color:#04342C
    style D fill:#9FE1CB,stroke:#085041,color:#04342C
    style E fill:#9FE1CB,stroke:#085041,color:#04342C
    style F fill:#FAEEDA,stroke:#854F0B,color:#412402
    style G fill:#EEEDFE,stroke:#3C3489,color:#26215C
    style H fill:#DDD9FC,stroke:#3C3489,color:#26215C
```

---

*End of Data Lineage Document*