{% macro int_taxi_trips_aggregation(grain_columns, metrics) %}

    {% if is_incremental() %}
    get_max_partition_date AS (
        SELECT COALESCE(MAX(partition_date), DATE '1900-01-01') AS max_partition_date
        FROM {{ this }}
    ),
    {% endif %}

    import_stg_taxi_trips AS (
        SELECT *,
            DATE_TRUNC('HOUR', pickup_datetime) AS date_hour_pickup_datetime
        FROM {{ ref('stg_taxi_trips') }}
        {% if is_incremental() %}
        WHERE partition_date >= (SELECT max_partition_date FROM get_max_partition_date)
        {% endif %}
    ),

    final AS (
        SELECT
            {% for col in grain_columns %}
            {{ col }},
            {% endfor %}
            MAX(partition_date) AS partition_date,
            {% for metric in metrics %}
            {{ metric.expr }} AS {{ metric.name }},
            {% endfor %}
            {{ timestamp_mock() }} AS dwh_updated_at
        FROM import_stg_taxi_trips
        GROUP BY
            {% for i in range(grain_columns | length) %}
            {{ i + 1 }}{% if not loop.last %}, {% endif %}
            {% endfor %}
    )

{% endmacro %}
