-- dim_user_history.sql
-- ─────────────────────────────────────────────────────────────────
-- GRAIN: One row per version of a user record.
--        A user can have multiple rows — one per time their
--        tracked attributes changed.
--
-- SCD Type 2 — keeps full history of user attribute changes.
-- Use this table when you need point-in-time accuracy:
--   "Was this user Elite when they wrote this review?"
--   "What was this user's average stars at the time of the review?"
--
-- Use dim_user (Type 1) when you need current state:
--   "Is this user currently Elite?"
--
-- Key columns:
--   valid_from  — when this version became active
--   valid_to    — when this version was superseded (NULL = still active)
--   is_current  — TRUE for the current version of each user
--   record_hash — md5 of tracked attributes, used to detect changes
--
-- Tracked attributes (changes create a new version row):
--   is_elite, elite_years_count, review_count, average_stars, fan_count
--
-- Note: user_name is NOT tracked as a change trigger — name changes
-- are cosmetic and should not create analytical version splits.
-- PII note: name is still included in the output but not hashed.
--
-- Materialised: incremental — append new versions when changes detected
-- ─────────────────────────────────────────────────────────────────

{{
    config(
        materialized     = 'incremental',
        unique_key       = ['user_id', 'valid_from'],
        on_schema_change = 'fail'
    )
}}

with current_snapshot as (
    select
        user_id,
        name,
        review_count,
        average_stars,
        is_elite,
        elite_years_count,
        fan_count,
        yelping_since_year,
        total_useful_votes,
        total_funny_votes,
        total_cool_votes,
        -- Hash only analytically meaningful attributes
        -- Name excluded — cosmetic change, not an analytical event
        md5(concat_ws('|',
            coalesce(cast(is_elite as string),          ''),
            coalesce(cast(elite_years_count as string), ''),
            coalesce(cast(review_count as string),      ''),
            coalesce(cast(average_stars as string),     ''),
            coalesce(cast(fan_count as string),         '')
        ))                              as record_hash

    from {{ ref('dim_user') }}
),

{% if is_incremental() %}

previous_snapshot as (
    select
        user_id,
        record_hash                     as previous_hash
    from {{ this }}
    where is_current = true
),

changed as (
    select c.*
    from current_snapshot c
    left join previous_snapshot p
        on c.user_id = p.user_id
    where p.user_id is null
       or c.record_hash != p.previous_hash
)

select
    changed.user_id,
    changed.name,
    changed.review_count,
    changed.average_stars,
    changed.is_elite,
    changed.elite_years_count,
    changed.fan_count,
    changed.yelping_since_year,
    changed.total_useful_votes,
    changed.total_funny_votes,
    changed.total_cool_votes,
    changed.record_hash,
    {{ scd_type2_cols() }}

from changed

{% else %}

select
    user_id,
    name,
    review_count,
    average_stars,
    is_elite,
    elite_years_count,
    fan_count,
    yelping_since_year,
    total_useful_votes,
    total_funny_votes,
    total_cool_votes,
    record_hash,
    {{ scd_type2_cols() }}

from current_snapshot

{% endif %}