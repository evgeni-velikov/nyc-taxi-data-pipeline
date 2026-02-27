{% set is_unit = var('unit_test', false) %}
{% set run_incremental = not is_unit and is_incremental() %}

{{
    config(
        materialized = 'view' if is_unit else 'incremental',
        partition_by="partition_date",
        cluster_by=["type"],
        tags=['silver', 'staging']
    )
}}

{% set common_columns = [
    {"column": "VendorID", "alias": "vendor_id", "cast": "BIGINT"},
    {"column": "partition_date", "alias": "partition_date", "cast": None},
    {"column": "PULocationID", "alias": "pickup_location_id", "cast": "BIGINT"},
    {"column": "DOLocationID", "alias": "dropoff_location_id", "cast": "BIGINT"},
    {"column": "RatecodeID", "alias": "rate_code_id", "cast": None},
    {"column": "store_and_fwd_flag", "alias": "store_and_fwd_flag", "cast": None},
    {"column": "payment_type", "alias": "payment_type", "cast": "INT"},
    {"column": "improvement_surcharge", "alias": "improvement_surcharge", "cast": "DOUBLE"},
    {"column": "congestion_surcharge", "alias": "congestion_surcharge", "cast": "DOUBLE"},
    {"column": "passenger_count", "alias": "passenger_count", "cast": "INT"},
    {"column": "trip_distance", "alias": "trip_distance", "cast": "DOUBLE"},
    {"column": "mta_tax", "alias": "mta_tax", "cast": "DOUBLE"},
    {"column": "extra", "alias": "extra", "cast": "DOUBLE"},
    {"column": "fare_amount", "alias": "fare_amount", "cast": "DOUBLE"},
    {"column": "tip_amount", "alias": "tip_amount", "cast": "DOUBLE"},
    {"column": "tolls_amount", "alias": "tolls_amount", "cast": "DOUBLE"},
    {"column": "total_amount", "alias": "total_amount", "cast": "DOUBLE"},
] %}
{% set taxi_streams = [
    {
        "model": "yellow_trip_data",
        "type": "yellow",
        "unique_columns": [
            {"column": "tpep_pickup_datetime", "alias": "pickup_datetime", "cast": None},
            {"column": "tpep_dropoff_datetime", "alias": "dropoff_datetime", "cast": None},
            {"column": "airport_fee", "alias": "total_fee", "cast": "DOUBLE"},
            {"column": "'airport'", "alias": "fee_type", "cast": None},
            {"column": "STRING(NULL)", "alias": "trip_type", "cast": None},
        ],
    },
    {
        "model": "green_trip_data",
        "type": "green",
        "unique_columns": [
            {"column": "lpep_pickup_datetime", "alias": "pickup_datetime", "cast": None},
            {"column": "lpep_dropoff_datetime", "alias": "dropoff_datetime", "cast": None},
            {"column": "ehail_fee", "alias": "total_fee", "cast": "DOUBLE"},
            {"column": "'ehail'", "alias": "fee_type", "cast": None},
            {"column": "trip_type", "alias": "trip_type", "cast": "STRING"}
        ],
    },
] %}


WITH

{% for s in taxi_streams %}
import_{{ s.type }}_trip_data AS (
    SELECT *
    FROM {{ ref(s.model) }}
    {% if run_incremental %}
    WHERE partition_date > (
        SELECT COALESCE(MAX(partition_date), DATE '2019-01-01')
        FROM {{ this }}
        WHERE type = '{{ s.type }}'
    )
    {% endif %}
),
{% endfor %}

stg_taxi_trip_res AS (

{% for s in taxi_streams %}
    SELECT
        {% set cols = common_columns + s.unique_columns %}
        {% for c in cols %}
            {% if c.cast %}
                CAST({{ c.column }} AS {{ c.cast }}) AS {{ c.alias }},
            {% else %}
                {{ c.column }} AS {{ c.alias }},
            {% endif %}
        {% endfor %}
        '{{ s.type }}' AS type,
        CURRENT_TIMESTAMP as dwh_updated_at
    FROM import_{{ s.type }}_trip_data
    {% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
)

SELECT * FROM stg_taxi_trip_res
