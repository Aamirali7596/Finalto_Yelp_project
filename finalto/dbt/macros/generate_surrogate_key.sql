-- macros/generate_surrogate_key.sql
-- Wraps dbt_utils.generate_surrogate_key for consistent surrogate key
-- generation across all Gold dimension and fact models.
--
-- Usage:
--   {{ generate_surrogate_key(['business_id', 'category']) }}
--
-- Returns: md5 hash of the concatenated column values as a string.
-- Nulls are coalesced to empty string before hashing so the key
-- is always non-null even if individual columns are null.
--
-- This macro delegates to dbt_utils — it exists here so that if we
-- ever need to swap the hashing strategy (e.g. to sha256), we change
-- it in one place rather than across every model.

{% macro generate_surrogate_key(column_names) %}
    {{ dbt_utils.generate_surrogate_key(column_names) }}
{% endmacro %}