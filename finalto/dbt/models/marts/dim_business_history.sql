-- dim_business_history.sql
-- ─────────────────────────────────────────────────────────────────
-- GRAIN: One row per version of a business record.
--        A business can have multiple rows — one per time its
--        tracked attributes changed.
--
-- SCD Type 2 — keeps full history of business attribute changes.
-- Use this table when you need point-in-time accuracy:
--   "What category was this business in when this review was written?"
--   "Was this business open at the time of the checkin?"
--
-- Use dim_business (Type 1) when you need current state:
--   "What category is this business in today?"
--
-- Key columns:
--   valid_from  — when this version became active
--   valid_to    — when this version was superseded (NULL = still active)
--   is_current  — TRUE for the current version of each business
--   record_hash — md5 of tracked attributes, used to detect changes
--
-- Tracked attributes (changes to these create a new version row):
--   name, city, state, stars, review_count, is_open, primary_category
--
-- Materialised: incremental — append new versions when changes detected
-- ─────────────────────────────────────────────────────────────────

{{
    config(
        materialized     = 'incremental',
        unique_key       = ['business_id', 'valid_from'],
        on_schema_change = 'fail'
    )
}}

with current_snapshot as (
    -- Pull current business attributes from the Type 1 dimension
    select
        business_id,
        name,
        city,
        state,
        stars,
        review_count,
        is_open,
        primary_category,
        latitude,
        longitude,
        -- Hash all tracked attributes to detect any change
        md5(concat_ws('|',
            coalesce(name,             ''),
            coalesce(city,             ''),
            coalesce(state,            ''),
            coalesce(cast(stars as string),        ''),
            coalesce(cast(review_count as string), ''),
            coalesce(cast(is_open as string),      ''),
            coalesce(primary_category, '')
        ))                              as record_hash

    from {{ ref('dim_business') }}
),

{% if is_incremental() %}

-- On incremental runs: find businesses whose hash has changed
-- since the last time we saw them. These need a new version row.
previous_snapshot as (
    select
        business_id,
        record_hash                     as previous_hash
    from {{ this }}
    where is_current = true
),

changed as (
    select c.*
    from current_snapshot c
    left join previous_snapshot p
        on c.business_id = p.business_id
    where p.business_id is null              -- new business never seen before
       or c.record_hash != p.previous_hash   -- existing business that changed
)

select
    changed.business_id,
    changed.name,
    changed.city,
    changed.state,
    changed.stars,
    changed.review_count,
    changed.is_open,
    changed.primary_category,
    changed.latitude,
    changed.longitude,
    changed.record_hash,
    {{ scd_type2_cols() }}

from changed

{% else %}

-- On first run: load all current businesses as version 1
select
    business_id,
    name,
    city,
    state,
    stars,
    review_count,
    is_open,
    primary_category,
    latitude,
    longitude,
    record_hash,
    {{ scd_type2_cols() }}

from current_snapshot

{% endif %}-- dim_business_history.sql
-- ─────────────────────────────────────────────────────────────────
-- GRAIN: One row per version of a business record.
--        A business can have multiple rows — one per time its
--        tracked attributes changed.
--
-- SCD Type 2 — keeps full history of business attribute changes.
-- Use this table when you need point-in-time accuracy:
--   "What category was this business in when this review was written?"
--   "Was this business open at the time of the checkin?"
--
-- Use dim_business (Type 1) when you need current state:
--   "What category is this business in today?"
--
-- Key columns:
--   valid_from  — when this version became active
--   valid_to    — when this version was superseded (NULL = still active)
--   is_current  — TRUE for the current version of each business
--   record_hash — md5 of tracked attributes, used to detect changes
--
-- Tracked attributes (changes to these create a new version row):
--   name, city, state, stars, review_count, is_open, primary_category
--
-- Materialised: incremental — append new versions when changes detected
-- ─────────────────────────────────────────────────────────────────

{{
    config(
        materialized     = 'incremental',
        unique_key       = ['business_id', 'valid_from'],
        on_schema_change = 'fail'
    )
}}

with current_snapshot as (
    -- Pull current business attributes from the Type 1 dimension
    select
        business_id,
        name,
        city,
        state,
        stars,
        review_count,
        is_open,
        primary_category,
        latitude,
        longitude,
        -- Hash all tracked attributes to detect any change
        md5(concat_ws('|',
            coalesce(name,             ''),
            coalesce(city,             ''),
            coalesce(state,            ''),
            coalesce(cast(stars as string),        ''),
            coalesce(cast(review_count as string), ''),
            coalesce(cast(is_open as string),      ''),
            coalesce(primary_category, '')
        ))                              as record_hash

    from {{ ref('dim_business') }}
),

{% if is_incremental() %}

-- On incremental runs: find businesses whose hash has changed
-- since the last time we saw them. These need a new version row.
previous_snapshot as (
    select
        business_id,
        record_hash                     as previous_hash
    from {{ this }}
    where is_current = true
),

changed as (
    select c.*
    from current_snapshot c
    left join previous_snapshot p
        on c.business_id = p.business_id
    where p.business_id is null              -- new business never seen before
       or c.record_hash != p.previous_hash   -- existing business that changed
)

select
    changed.business_id,
    changed.name,
    changed.city,
    changed.state,
    changed.stars,
    changed.review_count,
    changed.is_open,
    changed.primary_category,
    changed.latitude,
    changed.longitude,
    changed.record_hash,
    {{ scd_type2_cols() }}

from changed

{% else %}

-- On first run: load all current businesses as version 1
select
    business_id,
    name,
    city,
    state,
    stars,
    review_count,
    is_open,
    primary_category,
    latitude,
    longitude,
    record_hash,
    {{ scd_type2_cols() }}

from current_snapshot

{% endif %}