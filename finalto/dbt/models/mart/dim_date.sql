-- dim_date.sql
-- Gold mart: date dimension (date spine).
-- Grain       : one row per calendar date
-- Materialised: table (static — rebuilt once, rarely changes)
-- Source      : dbt_utils.date_spine macro — NOT derived from source data.
--               Covers 2004-01-01 (earliest Yelp review) to current_date.

{{
    config(
        materialized     = 'table',
        on_schema_change = 'fail'
    )
}}

with date_spine as (
    {{
        dbt_utils.date_spine(
            datepart = "day",
            start_date = "cast('2004-01-01' as date)",
            end_date   = "cast(current_date as date)"
        )
    }}
),

dates as (
    select
        cast(date_day as date)          as full_date
    from date_spine
)

select
    -- Surrogate integer key — used as FK in fact tables
    {{ dbt_utils.generate_surrogate_key(['full_date']) }}
                                        as date_id,
    full_date,
    year(full_date)                     as year,
    month(full_date)                    as month,
    quarter(full_date)                  as quarter,
    dayofweek(full_date)                as day_of_week,
    date_format(full_date, 'EEEE')      as day_name,
    weekofyear(full_date)               as week_of_year,
    -- Weekend flag: Saturday (7) or Sunday (1) in Spark's dayofweek convention
    case
        when dayofweek(full_date) in (1, 7) then true
        else false
    end                                 as is_weekend

from dates
order by full_date