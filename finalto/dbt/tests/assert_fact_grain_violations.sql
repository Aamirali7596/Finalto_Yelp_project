-- assert_fact_grain_violations.sql
-- ─────────────────────────────────────────────────────────────────
-- Singular test: assert that both fact tables have no grain violations.
--
-- A grain violation means the primary key column has duplicates —
-- i.e. the same review_id or checkin_id appears more than once.
-- This would mean our grain definition is broken and every aggregate
-- built on these tables is potentially wrong.
--
-- dbt singular tests return rows on FAILURE, nothing on SUCCESS.
-- If this query returns any rows → test fails → DAG halts.
--
-- Checks:
--   fact_reviews  → review_id must be unique
--   fact_checkins → checkin_id must be unique
-- ─────────────────────────────────────────────────────────────────

-- Check fact_reviews grain
select
    'fact_reviews'          as table_name,
    'review_id'             as grain_key,
    review_id               as key_value,
    count(*)                as duplicate_count
from {{ ref('fact_reviews') }}
group by review_id
having count(*) > 1

union all

-- Check fact_checkins grain
select
    'fact_checkins'         as table_name,
    'checkin_id'            as grain_key,
    checkin_id              as key_value,
    count(*)                as duplicate_count
from {{ ref('fact_checkins') }}
group by checkin_id
having count(*) > 1