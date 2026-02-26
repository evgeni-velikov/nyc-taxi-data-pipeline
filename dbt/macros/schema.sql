{% macro generate_schema_name(custom_schema_name, node) %}

    {% set user = env_var('DBT_USER', 'local') %}

    {% if target.name == 'dev' %}
        {{ target.schema }}_{{ user }}_{{ custom_schema_name }}
    {% else %}
        {{ custom_schema_name }}
    {% endif %}

{% endmacro %}
