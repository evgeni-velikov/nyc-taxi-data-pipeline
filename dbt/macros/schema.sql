{% macro generate_schema_name(custom_schema_name, node) %}

    {% set user = env_var('DBT_USER', 'local') %}
    {% set is_seed = node.resource_type == 'seed' %}

    {% if target.name == 'dev' and not is_seed %}
        {{ target.schema }}_{{ user }}_{{ custom_schema_name }}
    {% else %}
        {{ custom_schema_name }}
    {% endif %}

{% endmacro %}
