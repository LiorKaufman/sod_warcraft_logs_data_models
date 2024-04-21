{% macro convert_millis_to_timestamp(column_name, start_time=None) %}

{% if start_time is none %}
    -- Default to Unix epoch start time if no start time is provided
    TIMESTAMP '1970-01-01 00:00:00' + CAST({{ column_name }} / 1000 AS BIGINT) * INTERVAL '1 second'
{% else %}
    -- Use provided start time, assuming it is a valid TIMESTAMP
    {{ start_time }} + CAST({{ column_name }} / 1000 AS BIGINT) * INTERVAL '1 second'
{% endif %}

{% endmacro %}
