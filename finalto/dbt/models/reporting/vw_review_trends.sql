-- vw_review_trends.sql
-- Reporting view: monthly review volume and average rating over time.
-- Materialised as: view (always reflects latest Gold data)
-- Schema: gold_reporting
--
-- Use case: trend dashboards showing Yelp's growth over time
-- and whether platform-wide average rating has shifted.
-- Also useful for spotting seasonal patterns in review activity.

with reviews as (
    select * from {{ ref('fact_reviews') }}
),

dates as (
    select * from {{ ref('dim_date') }}
)

select
    d.year,
    d.month,
    -- Full date label for charting (first day of each month)
    date_format(
        make_date(d.year, d.month, 1),
        'yyyy-MM'
    )                                       as year_month,
    count(f.review_id)                      as total_reviews,
    round(avg(f.review_stars), 2)           as avg_rating,
    round(avg(f.useful_votes), 2)           as avg_useful_votes,
    round(avg(f.review_length_chars), 0)    as avg_review_length

from reviews f
join dates d on f.date_id = d.date_id

group by d.year, d.month

order by d.year, d.month