{{
    config(
        materialized='incremental',
        unique_key = [
            'vendor_id', 'pickup_location_id', 'dropoff_location_id',
            'date_hour_pickup_datetime', 'metric_name', 'taxi_type',
            'payment_type', 'rate_code_id', 'trip_type'
        ],
        incremental_strategy='merge',
        partition_by=['metric_name', 'partition_date'],
        cluster_by=['vendor_id', 'taxi_type', 'dwh_updated_at']
    )
}}

WITH

-- Import

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
    WHERE partition_date >= max_partition_date
    {% endif %}
),


-- Logic

taxi_trips_aggregated AS (
    SELECT
        vendor_id,
        pickup_location_id,
        dropoff_location_id,
        date_hour_pickup_datetime,
        type AS taxi_type,
        payment_type,
        trip_type,
        rate_code_id,
        MAX(partition_date) AS partition_date,
        COUNT(*) AS total_trips,
        SUM(improvement_surcharge) AS total_improvement_surcharge,
        SUM(congestion_surcharge) AS total_congestion_surcharge,
        SUM(passenger_count) AS total_passenger_count,
        SUM(trip_distance) AS total_trip_distance,
        SUM(mta_tax) AS total_mta_tax,
        SUM(extra) AS total_extra,
        SUM(fare_amount) AS total_fare_amount,
        SUM(tip_amount) AS total_tip_amount,
        SUM(tolls_amount) AS total_tolls_amount,
        SUM(total_amount) AS total_amount,
        SUM(IF(fee_type = 'airport', total_fee, 0)) AS total_airport_fee,
        SUM(IF(fee_type = 'ehail', total_fee, 0)) AS total_ehail_fee
    FROM import_stg_taxi_trips
    GROUP BY 1,2,3,4,5,6,7,8
),

taxi_trips_unpivot_res AS (
    SELECT
        vendor_id,
        pickup_location_id,
        dropoff_location_id,
        date_hour_pickup_datetime,
        taxi_type,
        payment_type,
        rate_code_id,
        trip_type,
        partition_date,
        STACK(13,
            'total_trips', CAST(total_trips AS DOUBLE),
            'total_improvement_surcharge', CAST(total_improvement_surcharge AS DOUBLE),
            'total_congestion_surcharge', CAST(total_congestion_surcharge AS DOUBLE),
            'total_passenger_count', CAST(total_passenger_count AS DOUBLE),
            'total_trip_distance', CAST(total_trip_distance AS DOUBLE),
            'total_mta_tax', CAST(total_mta_tax AS DOUBLE),
            'total_extra', CAST(total_extra AS DOUBLE),
            'total_fare_amount', CAST(total_fare_amount AS DOUBLE),
            'total_tip_amount', CAST(total_tip_amount AS DOUBLE),
            'total_tolls_amount', CAST(total_tolls_amount AS DOUBLE),
            'total_amount', CAST(total_amount AS DOUBLE),
            'total_airport_fee', CAST(total_airport_fee AS DOUBLE),
            'total_ehail_fee', CAST(total_ehail_fee AS DOUBLE)
        ) AS (metric_name, metric_value),
        {{ timestamp_mock() }} as dwh_updated_at
    FROM taxi_trips_aggregated
)


-- Results

SELECT * FROM taxi_trips_unpivot_res
WHERE metric_value IS NOT NULL
