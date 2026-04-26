# Data Model
## Yelp Dataset — Entity Overview & Relationships
**Author:** Aamir | **Version:** 1.0 | **Last Updated:** April 2026

---

## Table of Contents

1. [Dataset Overview](#1-dataset-overview)
2. [Entities & Fields](#2-entities--fields)
3. [Relationships Between Entities](#3-relationships-between-entities)
4. [Data Quality Notes](#4-data-quality-notes)
5. [How the Model Evolves Through the Medallion Layers](#5-how-the-model-evolves-through-the-medallion-layers)

> **See also:** `docs/erd.md` for the visual ERD diagram

---

## 1. Dataset Overview

The Yelp dataset contains **5 source entities** delivered as JSON files. Every entity ultimately connects back to `business` — it is the central root entity of the entire dataset.

| Entity | File | Approx. Size | Grain | Role |
|---|---|---|---|---|
| Business | `business.json` | ~120 MB | One row per business | Root entity — all others reference it |
| Review | `review.json` | ~5.5 GB | One row per review | Core transactional fact |
| User | `user.json` | ~3.5 GB | One row per user | Reviewer identity |
| Checkin | `checkin.json` | ~290 MB | One row per business | Aggregated visit timestamps |
| Tip | `tip.json` | ~240 MB | One row per tip | Short user feedback on a business |

---

## 2. Entities & Fields

### Business
The root entity. Every review, checkin, and tip references a business.

| Field | Type | Description |
|---|---|---|
| `business_id` | String | Unique identifier — primary key |
| `name` | String | Business name |
| `address` | String | Street address |
| `city` | String | City |
| `state` | String | State code (e.g. "NV", "PA") |
| `postal_code` | String | Postcode |
| `latitude` | Double | Geographic latitude |
| `longitude` | Double | Geographic longitude |
| `stars` | Double | Overall average star rating (1.0–5.0) |
| `review_count` | Integer | Total number of reviews received |
| `is_open` | Integer | 1 = currently open, 0 = closed |
| `categories` | String | ⚠️ Comma-separated list e.g. `"Restaurants, Italian, Pizza"` |
| `hours` | Struct | Nested object with opening hours per day of week |
| `attributes` | Struct | Nested object with business attributes (WiFi, parking, etc.) |

> ⚠️ **`categories` requires transformation** — it is stored as a flat comma-separated string and must be split and exploded into individual rows for category-level analysis.

---

### Review
The largest and most important entity. Contains the star rating and text for every review ever written.

| Field | Type | Description |
|---|---|---|
| `review_id` | String | Unique identifier — primary key |
| `business_id` | String | Foreign key → Business |
| `user_id` | String | Foreign key → User |
| `stars` | Integer | Star rating given (1–5) |
| `date` | String | ⚠️ Review date as string `"YYYY-MM-DD HH:MM:SS"` |
| `text` | String | Full review text |
| `useful` | Integer | Number of "useful" votes received |
| `funny` | Integer | Number of "funny" votes received |
| `cool` | Integer | Number of "cool" votes received |

> ⚠️ **`date` requires casting** — stored as a string and must be cast to `TimestampType` at Silver.

---

### User
Represents a Yelp user who has written at least one review.

| Field | Type | Description |
|---|---|---|
| `user_id` | String | Unique identifier — primary key |
| `name` | String | User's first name |
| `review_count` | Integer | Total reviews written by this user |
| `yelping_since` | String | Date user joined Yelp `"YYYY-MM-DD HH:MM:SS"` |
| `useful` | Integer | Total "useful" votes received across all reviews |
| `funny` | Integer | Total "funny" votes received |
| `cool` | Integer | Total "cool" votes received |
| `elite` | String | ⚠️ Comma-separated list of years user held Elite status e.g. `"2015,2016,2017"` |
| `friends` | String | Comma-separated list of friend `user_id`s |
| `fans` | Integer | Number of fans |
| `average_stars` | Double | Lifetime average star rating given |
| `compliment_hot` | Integer | Compliments received (various types) |

> ⚠️ **`elite` requires transformation** — parsed into `is_elite` boolean flag (has ever been elite) and `elite_years_count` integer at Silver.

---

### Checkin
One row per business, containing all checkin timestamps as a single denormalised string.

| Field | Type | Description |
|---|---|---|
| `business_id` | String | Foreign key → Business |
| `date` | String | ⚠️ Comma-separated checkin timestamps e.g. `"2016-04-26 19:49:16, 2016-08-30 18:36:57, ..."` |

> ⚠️ **`date` requires heavy transformation** — the entire timestamp list must be split and exploded into individual rows at Silver, one row per checkin event. This is the most denormalised field in the dataset.

---

### Tip
Short text feedback left by a user on a business. Simpler than a review — no star rating.

| Field | Type | Description |
|---|---|---|
| `business_id` | String | Foreign key → Business |
| `user_id` | String | Foreign key → User |
| `text` | String | Tip content |
| `date` | String | Tip date `"YYYY-MM-DD HH:MM:SS"` |
| `compliment_count` | Integer | Number of compliments this tip received |

---

## 3. Relationships Between Entities

```
                        ┌─────────────────┐
                        │    BUSINESS      │
                        │─────────────────│
                        │ business_id (PK) │
                        │ name             │
                        │ city, state      │
                        │ stars            │
                        │ categories       │
                        │ is_open          │
                        └────────┬────────┘
                                 │
              ┌──────────────────┼──────────────────┐
              │                  │                  │
              │ 1:many           │ 1:1              │ 1:many
              ▼                  ▼                  ▼
    ┌──────────────────┐ ┌──────────────┐ ┌──────────────────┐
    │     REVIEW       │ │   CHECKIN    │ │      TIP         │
    │──────────────────│ │──────────────│ │──────────────────│
    │ review_id (PK)   │ │ business_id  │ │ business_id (FK) │
    │ business_id (FK) │ │ date (blob)  │ │ user_id (FK)     │
    │ user_id (FK)     │ │              │ │ text             │
    │ stars            │ └──────────────┘ │ date             │
    │ date             │                  │ compliment_count │
    │ text             │                  └──────────────────┘
    │ useful/funny/cool│
    └────────┬─────────┘
             │
             │ many:1
             ▼
    ┌──────────────────┐
    │      USER        │
    │──────────────────│
    │ user_id (PK)     │
    │ name             │
    │ review_count     │
    │ average_stars    │
    │ elite (list)     │
    │ fans             │
    └──────────────────┘
```

**Relationship summary:**
- One **Business** can have many **Reviews**, many **Tips**, and one **Checkin** record
- One **User** can write many **Reviews** and many **Tips**
- **Review** sits at the intersection of **Business** and **User** — it is the primary fact
- **Checkin** has no user-level granularity in the source data — only business-level aggregated timestamps
- **Tip** is similar to Review but has no star rating and is typically much shorter

---

## 4. Data Quality Notes

These are issues identified in the raw source data that must be handled before the data is usable for BI reporting.

| Entity | Field | Issue | Treatment at Silver |
|---|---|---|---|
| Business | `categories` | Comma-separated string, not an array | `split()` + `explode()` into `bridge_business_categories` |
| Business | `hours`, `attributes` | Deeply nested structs | Flatten relevant subfields, drop unused |
| Business | `stars` | Can be null for new businesses | Keep null — do not impute |
| Review | `date` | String, not timestamp | Cast to `TimestampType` |
| Review | `stars` | Should be 1–5 integer | Validate range, reject out-of-range rows |
| User | `elite` | Comma-separated year string | Parse to `is_elite` boolean + `elite_years_count` |
| User | `yelping_since` | String, not timestamp | Cast to `TimestampType` |
| Checkin | `date` | All timestamps in one string | `split()` + `explode()` → one row per checkin event |
| All | `business_id` / `user_id` | Referential integrity not enforced at source | Left join on Silver — orphaned records flagged |

---

## 5. How the Model Evolves Through the Medallion Layers

The data model is not static — it evolves as data moves through the pipeline. Here is what the model looks like at each layer.

### Bronze — Source Faithful
The model at Bronze mirrors the source exactly. Five tables, one per JSON file. No transformations applied. The schema is as close to the raw JSON as possible.

```
bronze.business   → raw fields including categories string and nested hours/attributes
bronze.reviews    → raw fields including date as string
bronze.users      → raw fields including elite as comma-separated string
bronze.checkins   → business_id + single date blob string
bronze.tips       → raw fields including date as string
```

---

### Silver — Cleaned & Conformed
At Silver, the model expands slightly because denormalised fields are exploded into proper tables. The checkin date blob becomes individual rows. Categories become a bridge table.

```
silver.business          → flat, typed, categories removed (moved to bridge)
silver.business_categories → bridge table: business_id | category (one row per category)
silver.reviews           → typed, date as TimestampType, stars validated
silver.users             → typed, is_elite boolean derived, yelping_since as Timestamp
silver.checkin_events    → one row per checkin event: business_id | checkin_ts
silver.tips              → typed, date as TimestampType
```

---

### Gold — Star Schema (BI-Ready)
At Gold, the model is restructured into a Kimball star schema. Source entity names give way to BI-friendly fact and dimension names. Built entirely by dbt models.

```
gold.fact_reviews        → one row per review, foreign keys to all dimensions
gold.fact_checkins       → one row per checkin event, foreign keys to dimensions
gold.dim_business        → one row per business, primary_category denormalised in
gold.dim_user            → one row per user, is_elite flag, yelping_since_year
gold.dim_date            → date spine: year, month, quarter, day_of_week, is_weekend
```

The star schema is documented in full in `docs/data_engineering.md`.

---

*End of Data Model Document*
*Visual ERD → `docs/erd.md`*