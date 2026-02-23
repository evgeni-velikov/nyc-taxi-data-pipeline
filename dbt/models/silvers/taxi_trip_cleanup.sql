{{
    config(
        materialized='incremental',
        partition_by="partition_date",
        cluster_by=["type"],
        tags=['silver', 'staging']
    )
}}

-- TODO finish the common columns and taxi_streams columns
{% set common_columns = [
    {"column": "VendorID", "alias": "vendor_id"},
] %}
{% set taxi_streams = [
    {
        "model": "yellow_trip_data",
        "type": "yellow",
        "columns": [
            {"column": "airport_fee", "alias": "total_fee"},
        ],
    },
    {
        "model": "green_trip_data",
        "type": "green",
        "columns": [
            {"column": "ehail_fee", "alias": "total_fee"},
        ],
    }
] %}

WITH

{% for s in taxi_streams %}
import_{{ s.type }}_trip_data AS (
    SELECT *
    FROM {{ ref(s.model) }}
    {% if is_incremental() %}
    WHERE partition_date > (
        SELECT COALESCE(MAX(partition_date), DATE '2019-01-01')
        FROM {{ this }}
        WHERE type = '{{ s.type }}'
    )
    {% endif %}
),
{% endfor %}

taxi_trip_res AS (

{% for s in taxi_streams %}
    SELECT
        {% for c in common_columns %}
            {{ c.column }} AS {{ c.alis }},
        {% endfor %}
        {% for c in s.columns %}
            {{ c.column }} AS {{ c.alis }},
        {% endfor %}
        '{{ s.type }}' AS type,
        CURRENT_TIMESTAMP() AS dwh_updated_at
    FROM import_{{ s.type }}_trip_data
    {% if not loop.last %}UNION ALL{% endif %}
{% endfor %}

)

SELECT * FROM taxi_trip_res
