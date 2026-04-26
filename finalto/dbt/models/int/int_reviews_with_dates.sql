-- int_reviews_with_dates.sql
-- Intermediate model: join reviews to the date dimension
-- to resolve date_id foreign key for the fact table.
-- This is ephemeral — it exists only as a CTE during the fact build,
-- never written to disk as its own table.
-- Grain: one row per review

with reviews as (
    select * from {{ ref('stg_reviews') }}
),

dates as (
    select * from {{ ref('dim_date') }}
),

joined as (
    select
        r.review_id,
        r.business_id,
        r.user_id,
        d.date_id,
        r.review_stars,
        r.useful_votes,
        r.funny_votes,
        r.cool_votes,
        r.review_length_chars,
        r.reviewed_at,
        r.year,
        r.month

    from reviews r
    -- Join on the calendar date portion of the timestamp
    left join dates d
        on cast(r.reviewed_at as date) = d.full_date
)

select * from joined