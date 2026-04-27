-- fact_reviews.sql
-- ─────────────────────────────────────────────────────────────────
-- GRAIN: One row per unique review (review_id)
--
-- What one row means:
--   A single review written by one user about one business on one date.
--   review_id is the natural key — it uniquely identifies a review
--   in the Yelp source system.
--
-- Grain enforcement:
--   unique_key = 'review_id' in config → dbt upserts on this key
--   unique + not_null tests on review_id in marts.yml
--   assert_fact_grain_violations.sql singular test (dbt/tests/)
--
-- What would break the grain:
--   - A JOIN that fans out rows (e.g. joining to a multi-row dim)
--   - A source dedup failure producing duplicate review_ids
--   Both are caught by the unique test on review_id.
--
-- Materialised : incremental — only new rows per run
-- Partition    : year / month
-- Z-order      : business_id, user_id (applied via post_hook)
-- ─────────────────────────────────────────────────────────────────

{{
    config(
        materialized     = 'incremental',
        unique_key       = 'review_id',
        on_schema_change = 'fail',
        post_hook        = [
            "OPTIMIZE {{ this }} ZORDER BY (business_id, user_id)"
        ]
    )
}}

with reviews as (
    select * from {{ ref('int_reviews_with_dates') }}

    -- Incremental filter: on subsequent runs, only process reviews
    -- that arrived after the latest reviewed_at already in the table.
    -- On the first run, is_incremental() is False so all rows are loaded.
    {% if is_incremental() %}
        where reviewed_at > (select max(reviewed_at) from {{ this }})
    {% endif %}
)

select
    review_id,
    business_id,
    user_id,
    date_id,
    review_stars,
    useful_votes,
    funny_votes,
    cool_votes,
    review_length_chars,
    year,
    month

from reviews
where review_id is not null