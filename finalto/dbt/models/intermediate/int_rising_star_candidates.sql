-- int_rising_star_candidates.sql
-- Intermediate model: identify rising star business candidates.
-- A rising star is a business that:
--   1. Has at least 10 reviews in the past 12 months
--   2. Has an average rating in the past 12 months that is
--      at least 1.0 star higher than its historical average
--
-- This intermediate model powers:
--   - The SQL Section 3 answer
--   - Any BI dashboard showing business improvement trends
--
-- Grain: one row per qualifying business

with reviews as (
    select * from {{ ref('stg_reviews') }}
),

-- All reviews from the past 12 months
recent as (
    select
        business_id,
        avg(review_stars)   as avg_stars_recent,
        count(review_id)    as review_count_recent
    from reviews
    where reviewed_at >= dateadd(year, -1, current_date)
    group by business_id
),

-- All reviews older than 12 months (the baseline)
historical as (
    select
        business_id,
        avg(review_stars)   as avg_stars_historical
    from reviews
    where reviewed_at < dateadd(year, -1, current_date)
    group by business_id
),

-- Join the two windows and apply the rising star criteria
candidates as (
    select
        r.business_id,
        round(r.avg_stars_recent,      2) as avg_stars_recent,
        round(h.avg_stars_historical,  2) as avg_stars_historical,
        round(
            r.avg_stars_recent - h.avg_stars_historical,
            2
        )                                  as rating_improvement,
        r.review_count_recent

    from recent r
    -- INNER JOIN: businesses with no historical reviews are excluded.
    -- They have no baseline to compare against so cannot qualify.
    inner join historical h
        on r.business_id = h.business_id

    where r.review_count_recent      >= 10
      and r.avg_stars_recent         >= h.avg_stars_historical + 1.0
)

select * from candidates
order by rating_improvement desc