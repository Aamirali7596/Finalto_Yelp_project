-- stg_users.sql
-- Staging model for the Silver users table.
-- Purpose: rename columns, expose is_elite and elite_years_count
--          (already derived at Silver), derive yelping_since_year.
-- Grain  : one row per user (user_id is unique)

with source as (
    select * from {{ source('silver', 'users') }}
),

staged as (
    select
        user_id,

        -- PII: name is flagged in schema.yml
        name                                    as user_name,

        review_count                            as lifetime_review_count,
        average_stars                           as lifetime_avg_stars,

        -- Derived at Silver from the raw comma-separated elite year string
        is_elite,
        elite_years_count,

        fans                                    as fan_count,

        -- Extract just the year from the parsed timestamp
        year(yelping_since)                     as yelping_since_year,

        -- Social engagement metrics
        useful,
        funny,
        cool

    from source
    where user_id is not null
)

select * from staged