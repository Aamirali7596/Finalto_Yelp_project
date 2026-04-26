-- dim_business.sql
-- Gold mart: business dimension table.
-- Grain       : one row per business (business_id is unique)
-- Materialised: table (full refresh — SCD Type 1, latest state wins)
-- Includes primary_category denormalised in from the bridge table.

{{
    config(
        materialized     = 'table',
        on_schema_change = 'fail',
        post_hook        = [
            "OPTIMIZE {{ this }} ZORDER BY (city, state)"
        ]
    )
}}

with business as (
    select * from {{ ref('stg_business') }}
),

categories as (
    select * from {{ ref('int_business_categories') }}
)

select
    b.business_id,
    b.business_name                     as name,
    b.city,
    b.state,
    b.postal_code,
    b.latitude,
    b.longitude,
    b.overall_stars                     as stars,
    b.total_review_count                as review_count,
    b.is_currently_open                 as is_open,
    -- Denormalise primary category for simpler BI queries
    coalesce(c.primary_category, 'Unknown') as primary_category

from business b
left join categories c
    on b.business_id = c.business_id