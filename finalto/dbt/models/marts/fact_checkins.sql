-- fact_checkins.sql
-- ─────────────────────────────────────────────────────────────────
-- GRAIN: One row per unique checkin event (checkin_id)
--
-- What one row means:
--   A single visit by any customer to a specific business at a
--   specific timestamp. checkin_id is a surrogate key generated
--   as md5(business_id || checkin_ts) — deterministic and unique.
--
-- Important: the source data had NO per-event grain.
--   checkin.json held one row per business with ALL timestamps
--   packed into a single comma-separated string. The Silver
--   transformation exploded this into individual events, giving
--   each one a surrogate key. That is where the grain is established.
--
-- Grain enforcement:
--   unique_key = 'checkin_id' in config → dbt upserts on this key
--   unique + not_null tests on checkin_id in marts.yml
--   assert_fact_grain_violations.sql singular test (dbt/tests/)
--
-- What would break the grain:
--   - md5 collision (astronomically unlikely for this key combination)
--   - Duplicate checkin events in source (caught by dropDuplicates
--     in silver_transform.py)
--
-- Materialised : incremental — only new events per run
-- Partition    : year / month
-- Z-order      : business_id (applied via post_hook)
-- ─────────────────────────────────────────────────────────────────

{{
    config(
        materialized     = 'incremental',
        unique_key       = 'checkin_id',
        on_schema_change = 'fail',
        post_hook        = [
            "OPTIMIZE {{ this }} ZORDER BY (business_id)"
        ]
    )
}}

with checkins as (
    select * from {{ ref('stg_checkins') }}

    {% if is_incremental() %}
        where checkin_ts > (select max(checkin_ts) from {{ this }})
    {% endif %}
),

dates as (
    select * from {{ ref('dim_date') }}
)

select
    c.checkin_id,
    c.business_id,
    d.date_id,
    c.checkin_hour,
    c.checkin_day_of_week,
    c.year,
    c.month

from checkins c
left join dates d
    on cast(c.checkin_ts as date) = d.full_date

where c.checkin_id is not null