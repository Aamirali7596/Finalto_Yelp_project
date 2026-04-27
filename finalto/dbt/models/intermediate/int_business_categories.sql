-- int_business_categories.sql
-- Intermediate model: derive primary_category per business
-- from the Silver business_categories bridge table.
-- primary_category = the first category in the original comma-separated string,
-- used as a single denormalised attribute in dim_business.
-- Grain: one row per business

with categories as (
    select * from {{ source('silver', 'business_categories') }}
),

-- Rank categories per business — rank 1 = primary category
ranked as (
    select
        business_id,
        category,
        row_number() over (
            partition by business_id
            order by category asc   -- deterministic ordering for reproducibility
        ) as category_rank

    from categories
),

primary_only as (
    select
        business_id,
        category as primary_category
    from ranked
    where category_rank = 1
)

select * from primary_only