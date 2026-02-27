{% macro is_incremental() %}

    {% if var('unit_test', false) %}
        {{ return(false) }}
    {% endif %}

    {{ return(this is not none and model.config.materialized == 'incremental' and not flags.FULL_REFRESH) }}

{% endmacro %}
