-- stg_reviews.sql
-- Staging model for the Silver reviews table.
-- Purpose: rename columns to business-friendly names, light casting,
--          derive review_length_chars, apply source freshness filter.
-- Grain  : one row per review (review_id is unique)

with source as (
    select * from {{ source('silver', 'reviews') }}
),

staged as (
    select
        review_id,
        business_id,
        user_id,

        -- Rename to be explicit about what this column means
        cast(stars as double)       as review_stars,

        -- Rename date column to be more descriptive
        review_date                 as reviewed_at,

        -- Rename vote columns
        useful                      as useful_votes,
        funny                       as funny_votes,
        cool                        as cool_votes,

        -- Derive review length — useful for Elite vs non-Elite analysis
        -- Computed here so Gold models don't need to carry the raw text field
        length(text)                as review_length_chars,

        -- Partition columns passed through for incremental materialisation
        year,
        month

    from source
    -- Silver guarantees stars are 1.0–5.0 — no re-validation needed here
    where review_id   is not null
    and reviewed_at is not null
)

select * from staged