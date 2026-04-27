-- stg_checkins.sql
-- Staging model for the Silver checkin_events table.
-- Note: by the time data reaches Silver, the raw date blob has already
--       been exploded into individual rows (one per checkin event).
--       This model simply renames and selects the relevant fields.
-- Grain : one row per checkin event (checkin_id is unique)

with source as (
    select * from {{ source('silver', 'checkin_events') }}
),

staged as (
    select
        checkin_id,
        business_id,
        checkin_ts,

        -- Pre-computed at Silver for BI query performance
        checkin_hour,
        checkin_day_of_week,

        year,
        month

    from source
    where checkin_id    is not null
      and business_id   is not null
      and checkin_ts    is not null
)

select * from staged