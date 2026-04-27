-- fact_reviews.sql
-- Gold mart: primary analytical fact table.
-- Grain       : one row per review
-- Materialised: incremental (only new rows processed per run)
-- Partition   : year
-- Z-order     : applied via post_hook on business_id, user_id

{{
    config(
        materialized  = 'incremental',
        unique_key    = 'review_id',
        on_schema_change = 'fail',
        post_hook     = [
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