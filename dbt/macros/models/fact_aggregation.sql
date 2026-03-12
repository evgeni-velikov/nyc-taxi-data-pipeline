{% macro fact_aggregation_model(source_model, grain_columns, metric_columns, watermark_condition=none) %}

    {% if is_incremental() %}
    get_max_dwh_updated_at AS (
        SELECT MAX(max_dwh_updated_at) AS max_dwh_updated_at
        FROM {{ this }}
        {% if watermark_condition is not none %}
        WHERE {{ watermark_condition }}
        {% endif %}
    ),
    {% endif %}

    import_source AS (
        SELECT *
        FROM {{ ref(source_model) }}
        {% if is_incremental() %}
        WHERE dwh_updated_at > (SELECT max_dwh_updated_at FROM get_max_dwh_updated_at)
        {% endif %}
    ),

    final AS (
        SELECT
            {% for col in grain_columns %}
            {{ col }},
            {% endfor %}
            {% for col in metric_columns %}
            {{ col }},
            {% endfor %}
            dwh_updated_at AS max_dwh_updated_at,
            {{ timestamp_mock() }} AS dwh_updated_at
        FROM import_source
    )

{% endmacro %}
