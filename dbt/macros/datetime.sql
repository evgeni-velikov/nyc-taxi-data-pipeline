{% macro dwh_updated_at() %}

    {% if var('dwh_updated_at', none) is not none %}
        {{ return("cast('" ~ var('dwh_updated_at') ~ "' as timestamp)") }}
    {% endif %}

    {{ return("current_timestamp") }}

{% endmacro %}
