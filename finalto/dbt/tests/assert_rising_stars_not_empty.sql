-- tests/assert_rising_stars_not_empty.sql
-- Singular test: assert that the rising star intermediate model
-- returns at least one qualifying business after every Gold run.
--
-- dbt singular tests work by returning rows that represent FAILURES.
-- If this query returns zero rows → test passes (no failures found).
-- If this query returns one or more rows → test fails.
--
-- Logic: count the rows in int_rising_star_candidates.
--        If the count is zero, return one failure row.
--        If the count is greater than zero, return nothing (pass).
--
-- Why this test matters:
--   A zero-row result could mean:
--   1. The date filter is wrong (e.g. dateadd syntax error)
--   2. Silver reviews data is missing or empty
--   3. No business genuinely qualifies (unlikely given dataset size)
--   Any of these scenarios warrants investigation before Gold serves BI.

select
    'int_rising_star_candidates returned zero rows' as failure_reason,
    count(*)                                         as rising_star_count
from {{ ref('int_rising_star_candidates') }}
having count(*) = 0