-- fact_checkins.sql
-- Gold mart: checkin events fact table.
-- Grain       : one row per checkin event
-- Materialised: incremental (append new events per run)
-- Partition   : year
-- Z-order     : applied via post_hook on business_id

{{
    config(
        materialized  = 'incremental',
        unique_key    = 'checkin_id',
        on_schema_change = 'fail',
        post_hook     = [
            "OPTIMIZE {{ this }} ZORDER BY (business_id)"
        ]
    )
}}

with checkins as (
    select * from {{ ref('stg_checkins') }}

    {% if is_incremental() %}
        where checkin_ts > (select max(checkin_ts) from {{ this }})
    {% endif %}
),

dates as (
    select * from {{ ref('dim_date') }}
)

select
    c.checkin_id,
    c.business_id,
    d.date_id,
    c.checkin_hour,
    c.checkin_day_of_week,
    c.year,
    c.month

from checkins c
left join dates d
    on cast(c.checkin_ts as date) = d.full_date

where c.checkin_id is not null