{% macro fact_aggregation_model(metrics, grain_columns, max_dwh_condition=none) %}

    {% if is_incremental() %}
    get_max_dwh_updated_at AS (
        SELECT MAX(max_dwh_updated_at) AS max_dwh_updated_at
        FROM {{ this }}
        {% if max_dwh_condition is not none %}
            WHERE {{ max_dwh_condition }}
        {% endif %}
    ),
    {% endif %}

    import_source AS (
        SELECT *
        FROM {{ ref('taxi_trips_unpivot') }}
        WHERE metric_name IN ({{ "'" + metrics | join("', '") + "'" }})
        {% if is_incremental() %}
        AND dwh_updated_at > (SELECT max_dwh_updated_at FROM get_max_dwh_updated_at)
        {% endif %}
    ),

    final_agg AS (
        SELECT
            {% for col in grain_columns %}
                {{ col }},
            {% endfor %}

            {% for metric in metrics %}
                SUM(CASE WHEN metric_name = '{{ metric }}'
                         THEN metric_value ELSE 0 END) AS {{ metric }},
            {% endfor %}

            MAX(dwh_updated_at) AS max_dwh_updated_at,
            CURRENT_TIMESTAMP AS dwh_updated_at
        FROM import_source
        GROUP BY
            {% for col in grain_columns %}
                {{ loop.index }}{% if not loop.last %}, {% endif %}
            {% endfor %}
    )

{% endmacro %}
