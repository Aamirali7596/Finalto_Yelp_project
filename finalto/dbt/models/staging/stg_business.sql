-- stg_business.sql
-- Staging model for the Silver business table.
-- Purpose: rename columns, select relevant fields,
--          cast is_open to boolean, pass through for Gold models.
-- Grain  : one row per business (business_id is unique)

with source as (
    select * from {{ source('silver', 'business') }}
),

staged as (
    select
        business_id,
        name                        as business_name,
        city,
        state,
        postal_code,
        latitude,
        longitude,

        -- Overall Yelp rating (aggregate across all reviews ever)
        stars                       as overall_stars,
        review_count                as total_review_count,
        is_open                     as is_currently_open

    from source
    where business_id is not null
)

select * from staged