-- dim_user.sql
-- Gold mart: user dimension table.
-- Grain       : one row per user (user_id is unique)
-- Materialised: table (full refresh — SCD Type 1)
-- Note        : user_name is PII — flagged in schema.yml

{{
    config(
        materialized     = 'table',
        on_schema_change = 'fail'
    )
}}

with users as (
    select * from {{ ref('stg_users') }}
)

select
    user_id,
    user_name                   as name,
    lifetime_review_count       as review_count,
    lifetime_avg_stars          as average_stars,
    is_elite,
    elite_years_count,
    fan_count,
    yelping_since_year,
    -- Social engagement totals
    useful                      as total_useful_votes,
    funny                       as total_funny_votes,
    cool                        as total_cool_votes

from users