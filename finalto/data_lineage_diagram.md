# Data Lineage
## Yelp Data Engineering Platform — Field-Level Traceability
**Author:** Aamir | **Version:** 1.0 | **Last Updated:** April 2026

---

> **Related Documents**
> - End-to-end data movement → `docs/data_flow.md`
> - Entity definitions → `docs/data_model.md`
> - Visual ERD → `docs/erd.md`

---

## What is Data Lineage?

Data lineage answers the question: *where did this number come from?*

When a BI analyst sees `avg_stars_recent = 4.6` in a dashboard, they should be able to trace that value back through every transformation to the raw source field that produced it. This document provides that trace for every significant field in the Gold layer.

Lineage operates at three levels:
- **Table lineage** — which table feeds which table
- **Column lineage** — which source column produces which Gold column
- **Transformation lineage** — what logic was applied between source and destination

---

## Table-Level Lineage

```
SOURCE JSON              BRONZE                SILVER                    GOLD
─────────────            ──────────────        ──────────────────────    ───────────────────
business.json       →    bronze.business   →   silver.business        →  dim_business
                                           →   silver.biz_categories  →  (bridge table)

review.json         →    bronze.reviews    →   silver.reviews         →  fact_reviews
                                                                       →  int_rising_star_candidates

user.json           →    bronze.users      →   silver.users           →  dim_user

checkin.json        →    bronze.checkins   →   silver.checkin_events  →  fact_checkins

tip.json            →    bronze.tips       →   silver.tips            →  (available, not in mart)

(generated)         →    —                 →   —                      →  dim_date
```

Note: `dim_date` is a dbt-generated date spine. It is not derived from source data — it is constructed by dbt's `dbt_utils.date_spine` macro covering the full review date range.

---

## Column-Level Lineage

### fact_reviews

| Gold column | Silver source | Bronze source | Transformation applied |
|---|---|---|---|
| `review_id` | `silver.reviews.review_id` | `review.review_id` | None — passed through as PK |
| `business_id` | `silver.reviews.business_id` | `review.business_id` | None — FK |
| `user_id` | `silver.reviews.user_id` | `review.user_id` | None — FK |
| `date_id` | `silver.reviews.review_date` | `review.date` | Cast string → timestamp → joined to `dim_date` on `full_date` |
| `review_stars` | `silver.reviews.stars` | `review.stars` | Cast int → double; validated range 1.0–5.0 |
| `useful_votes` | `silver.reviews.useful` | `review.useful` | None — passed through |
| `funny_votes` | `silver.reviews.funny` | `review.funny` | None — passed through |
| `cool_votes` | `silver.reviews.cool` | `review.cool` | None — passed through |
| `review_length_chars` | Derived | `review.text` | `length(text)` — character count of review body |

---

### fact_checkins

| Gold column | Silver source | Bronze source | Transformation applied |
|---|---|---|---|
| `checkin_id` | Generated | `checkin.business_id` + `checkin.date` | Surrogate key — `md5(business_id \|\| checkin_ts)` |
| `business_id` | `silver.checkin_events.business_id` | `checkin.business_id` | None — FK |
| `date_id` | `silver.checkin_events.checkin_ts` | `checkin.date` (blob) | Blob split → explode → timestamp → joined to `dim_date` |
| `checkin_hour` | `silver.checkin_events.checkin_ts` | `checkin.date` (blob) | `hour(checkin_ts)` |
| `checkin_day_of_week` | `silver.checkin_events.checkin_ts` | `checkin.date` (blob) | `dayofweek(checkin_ts)` → string label |

---

### dim_business

| Gold column | Silver source | Bronze source | Transformation applied |
|---|---|---|---|
| `business_id` | `silver.business.business_id` | `business.business_id` | None — PK |
| `name` | `silver.business.name` | `business.name` | None |
| `city` | `silver.business.city` | `business.city` | Null → `"UNKNOWN"` |
| `state` | `silver.business.state` | `business.state` | Uppercased, null → `"UNKNOWN"` |
| `stars` | `silver.business.stars` | `business.stars` | None — overall avg rating |
| `review_count` | `silver.business.review_count` | `business.review_count` | None |
| `is_open` | `silver.business.is_open` | `business.is_open` | Cast int → boolean |
| `primary_category` | `silver.biz_categories.category` | `business.categories` | Split comma string → explode → take `first()` category per business |
| `latitude` | `silver.business.latitude` | `business.latitude` | None |
| `longitude` | `silver.business.longitude` | `business.longitude` | None |

---

### dim_user

| Gold column | Silver source | Bronze source | Transformation applied |
|---|---|---|---|
| `user_id` | `silver.users.user_id` | `user.user_id` | None — PK |
| `name` | `silver.users.name` | `user.name` | None (PII — flagged in schema metadata) |
| `review_count` | `silver.users.review_count` | `user.review_count` | None |
| `average_stars` | `silver.users.average_stars` | `user.average_stars` | None |
| `is_elite` | `silver.users.is_elite` | `user.elite` | Parse comma-separated year list → `size(split(elite,',')) > 0` → boolean |
| `elite_years_count` | `silver.users.elite_years_count` | `user.elite` | `size(split(elite, ','))` → count of elite years |
| `fan_count` | `silver.users.fans` | `user.fans` | Renamed |
| `yelping_since_year` | `silver.users.yelping_since` | `user.yelping_since` | Cast string → timestamp → `year(yelping_since)` |

---

### dim_date

`dim_date` is fully synthetic — generated by dbt, not derived from source data.

| Gold column | Derivation |
|---|---|
| `date_id` | Surrogate integer key — `row_number()` over date spine |
| `full_date` | Every calendar date from `2004-01-01` to `current_date` |
| `year` | `year(full_date)` |
| `month` | `month(full_date)` |
| `quarter` | `quarter(full_date)` |
| `day_of_week` | `dayofweek(full_date)` (1=Sunday … 7=Saturday) |
| `is_weekend` | `day_of_week in (1, 7)` → boolean |
| `week_of_year` | `weekofyear(full_date)` |

---

## Rising Star Metric — Lineage Detail

The rising star query (Section 3 of the assessment) has a specific lineage path that is worth tracing explicitly because it involves a temporal split of the same source column.

```
review.stars (bronze)
    │
    │  cast int → double
    ▼
silver.reviews.stars
    │
    ├── WHERE reviewed_at >= 1 year ago
    │       │
    │       │  avg(stars) GROUP BY business_id
    │       ▼
    │   int_rising_star_candidates.avg_stars_recent
    │
    └── WHERE reviewed_at < 1 year ago
            │
            │  avg(stars) GROUP BY business_id
            ▼
        int_rising_star_candidates.avg_stars_historical
                │
                │  avg_stars_recent - avg_stars_historical
                ▼
            rating_improvement
                │
                │  FILTER: review_count_recent >= 10
                │          rating_improvement >= 1.0
                ▼
            Rising star businesses → business_id, name
```

**Why this matters for the interview:** The fact that `avg_stars_recent` and `avg_stars_historical` both trace back to `review.stars` — the same Bronze source field — but are split by a temporal filter applied at Silver, is a lineage question that interviewers ask to test whether you understand how aggregations relate to their grain. The answer is: same source column, different WHERE clause, different aggregate.

---

## Lineage Gaps & Known Limitations

| Gap | Description | Mitigation |
|---|---|---|
| `review.text` | Not promoted to Gold — too large, PII risk | Available in Silver for NLP/ML use cases |
| `tip` entity | Not in Gold mart | Silver table available; can be added as `fact_tips` if needed |
| `business.attributes` | Complex nested struct, partially dropped at Silver | Key attributes (WiFi, parking) can be extracted as needed |
| `user.friends` | Comma-separated user_id list, not exploded | Network graph analysis would require a separate bridge table |
| Referential integrity | Source has no FK constraints — orphaned reviews exist | Silver left-joins and flags unmatched IDs in `pipeline_audit` table |

---

## dbt Lineage Graph

dbt auto-generates a full visual lineage graph after every run. This can be viewed by running:

```bash
dbt docs generate
dbt docs serve
```

The graph shows the full DAG from `source('silver', 'reviews')` through `stg_reviews` → `int_rising_star_candidates` → `fact_reviews`, with every column dependency traced automatically.

---

*End of Data Lineage Document*
