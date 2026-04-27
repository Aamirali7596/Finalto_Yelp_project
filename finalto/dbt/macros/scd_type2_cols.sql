-- macros/scd_type2_cols.sql
-- ─────────────────────────────────────────────────────────────────
-- Macro: scd_type2_cols
--
-- Generates the standard SCD Type 2 metadata columns used in
-- dim_business_history and dim_user_history.
--
-- Columns added:
--   valid_from  — timestamp when this version of the record became active
--   valid_to    — timestamp when this version was superseded (null = current)
--   is_current  — boolean flag for easy filtering to current state
--   record_hash — md5 of all tracked attribute columns, used to detect changes
--
-- Usage in a model:
--   {{ scd_type2_cols() }}
--
-- This macro exists so the SCD pattern is defined once.
-- If the pattern changes (e.g. adding a dbt_updated_at column),
-- change it here and all SCD models pick it up automatically.
-- ─────────────────────────────────────────────────────────────────

{% macro scd_type2_cols() %}
    cast(current_timestamp as timestamp)    as valid_from,
    cast(null as timestamp)                 as valid_to,
    true                                    as is_current
{% endmacro %}