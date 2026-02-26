{% macro safe_ref(model_name) %}
    {%- set is_unit = var('unit_test', false) -%}

    {% if is_unit %}
        {%- set mock_schema = var('mock_schema', target.schema) -%}
        {{ return(adapter.get_relation(
            database=target.database,
            schema=mock_schema,
            identifier=model_name
        )) }}
    {% else %}
        {{ return(ref(model_name)) }}
    {% endif %}

{% endmacro %}

{% macro safe_source(source_name, table_name) %}
    {%- set is_unit = var('unit_test', false) -%}

    {% if is_unit %}
        {%- set mock_schema = var('mock_schema', target.schema) -%}
        {{ return(adapter.get_relation(
            database=target.database,
            schema=mock_schema,
            identifier=table_name
        )) }}
    {% else %}
        {{ return(source(source_name, table_name)) }}
    {% endif %}

{% endmacro %}
