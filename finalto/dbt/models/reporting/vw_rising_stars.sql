-- vw_rising_stars.sql
-- Reporting view: businesses that have significantly improved their ratings.
-- Materialised as: view (rebuilt on every query, always fresh)
-- Schema: gold_reporting
--
-- Definition of a rising star:
--   - At least 10 reviews in the past 12 months
--   - Average rating in the past 12 months is >= 1.0 star higher
--     than their average before the past 12 months
--
-- This is the direct answer to SQL Section 3 of the assessment.
-- BI teams query this view — they don't need to know about the
-- underlying CTEs or the two-window aggregation logic.

with rising_stars as (
    select * from {{ ref('int_rising_star_candidates') }}
),

businesses as (
    select * from {{ ref('dim_business') }}
)

select
    b.business_id,
    b.name                                      as business_name,
    b.city,
    b.state,
    b.primary_category,
    r.avg_stars_recent,
    r.avg_stars_historical,
    r.rating_improvement,
    r.review_count_recent,
    -- Classify improvement magnitude for easy BI filtering
    case
        when r.rating_improvement >= 2.0 then 'Major improvement'
        when r.rating_improvement >= 1.5 then 'Strong improvement'
        else 'Improvement'
    end                                         as improvement_tier

from rising_stars r
inner join businesses b
    on r.business_id = b.business_id

order by r.rating_improvement desc