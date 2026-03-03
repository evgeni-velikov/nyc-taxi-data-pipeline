{% macro fact_aggregation_model(
    metrics,
    grain_columns
) %}

{% set is_unit = var('unit_test', false) %}
{% set run_incremental = not is_unit and is_incremental() %}

{{
    config(
        materialized = 'view' if is_unit else 'incremental',
        unique_key = grain_columns,
        incremental_strategy='merge',
        cluster_by=['vendor_id', 'pickup_location_id', 'dropoff_location_id', 'date_hour_pickup_datetime']
    )
}}

WITH

{% if run_incremental %}
get_max_dwh_updated_at AS (
    SELECT MAX(max_dwh_updated_at) AS max_dwh_updated_at
    FROM {{ this }}
),
{% endif %}

import_source AS (
    SELECT *
    FROM {{ ref('taxi_trips_unpivot') }}
    WHERE 1=1
    {% if run_incremental %}
        AND dwh_updated_at > (SELECT max_dwh_updated_at FROM get_max_dwh_updated_at)
    {% endif %}
        AND metric_name {{ metrics }}
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

SELECT * FROM final_agg

{% endmacro %}
