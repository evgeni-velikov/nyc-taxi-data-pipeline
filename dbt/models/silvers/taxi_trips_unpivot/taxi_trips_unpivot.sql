{% set is_unit = var('unit_test', false) %}
{% set run_incremental = not is_unit and is_incremental() %}

{{
    config(
        materialized = 'view' if is_unit else 'incremental',
        unique_key = [
            'vendor_id', 'pickup_location_id', 'dropoff_location_id',
            'date_hour_pickup_datetime', 'date_hour_dropoff_datetime',
            'metric_name', 'taxi_type', 'payment_type', 'rate_code_id', 'trip_type'
        ],
        incremental_strategy='merge',
        partition_by=['partition_date', 'taxi_type', 'metric_name']
    )
}}

WITH

-- Import

{% if run_incremental %}
get_max_partition_date AS (
    SELECT COALESCE(MAX(partition_date), DATE '1900-01-01') AS max_partition_date
    FROM {{ this }}
),
{% endif %}
import_stg_taxi_trips AS (
    SELECT *,
        DATE_TRUNC('HOUR', pickup_datetime) AS date_hour_pickup_datetime,
        DATE_TRUNC('HOUR', dropoff_datetime) AS date_hour_dropoff_datetime
    FROM {{ ref('stg_taxi_trips') }}
    WHERE 1=1
    {% if run_incremental %}
    AND partition_date >= (SELECT max_partition_date FROM get_max_partition_date)
    {% endif %}
    AND dropoff_location_id IS NOT NULL AND dropoff_datetime IS NOT NULL
),


-- Logic

{% if run_incremental %}
keys_to_update AS (
    SELECT
        source.vendor_id,
        source.pickup_location_id,
        source.dropoff_location_id,
        source.date_hour_pickup_datetime,
        source.date_hour_dropoff_datetime,
        source.taxi_type,
        source.payment_type,
        source.rate_code_id,
        source.trip_type
    FROM {{ this }} AS source
    INNER JOIN import_stg_taxi_trips AS target
        ON source.vendor_id = target.vendor_id
        AND source.pickup_location_id = target.pickup_location_id
        AND source.dropoff_location_id = target.dropoff_location_id
        AND source.date_hour_pickup_datetime = target.date_hour_pickup_datetime
        AND source.date_hour_dropoff_datetime = target.date_hour_dropoff_datetime
        AND source.taxi_type = target.type
        AND source.payment_type = target.payment_type
        AND source.rate_code_id = target.rate_code_id
        AND source.trip_type = target.trip_type
    GROUP BY 1,2,3,4,5,6,7,8,9
),
{% endif %}


-- Results

taxi_trips_unpivot_res AS (
    SELECT
        source.vendor_id,
        source.pickup_location_id,
        source.dropoff_location_id,
        source.date_hour_pickup_datetime,
        source.date_hour_dropoff_datetime,
        source.type AS taxi_type,
        source.payment_type,
        source.trip_type,
        source.rate_code_id,
        MAX(source.partition_date) AS partition_date,
        STACK(12,
            'total_improvement_surcharge', SUM(source.improvement_surcharge),
            'total_congestion_surcharge', SUM(source.congestion_surcharge),
            'total_passenger_count', SUM(source.passenger_count),
            'avg_trip_distance', AVG(source.trip_distance),
            'mta_tax', SUM(source.mta_tax),
            'extra', SUM(source.extra),
            'fare_amount', SUM(source.fare_amount),
            'tip_amount', SUM(source.tip_amount),
            'tolls_amount', SUM(source.tolls_amount),
            'total_amount', SUM(source.total_amount),
            'total_airport_fee', SUM(IF(source.fee_type = 'airport', source.total_fee, 0)),
            'total_ehail_fee', SUM(IF(source.fee_type = 'ehail', source.total_fee, 0))
        ) AS (metric_name, metric_value),
        CURRENT_TIMESTAMP() AS dwh_updated_at
    FROM import_stg_taxi_trips AS source
    {% if run_incremental %}
    LEFT JOIN keys_to_update AS target
        ON source.vendor_id = target.vendor_id
        AND source.pickup_location_id = target.pickup_location_id
        AND source.dropoff_location_id = target.dropoff_location_id
        AND source.date_hour_pickup_datetime = target.date_hour_pickup_datetime
        AND source.date_hour_dropoff_datetime = target.date_hour_dropoff_datetime
        AND source.rate_code_id = target.rate_code_id
        AND source.payment_type = target.payment_type
        AND source.trip_type = target.trip_type
    {% endif %}
    GROUP BY 1,2,3,4,5,6,7,8,9
)

SELECT * FROM taxi_trips_unpivot_res
