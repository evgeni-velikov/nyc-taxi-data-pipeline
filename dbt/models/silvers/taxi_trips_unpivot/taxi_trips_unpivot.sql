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
        partition_by="partition_date",
        cluster_by=['metric_name', 'dwh_updated_at']
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
    -- Reprocessing buffer (1 day)
    AND partition_date >= date_sub(
        (SELECT max_partition_date FROM max_processed_partition),
        1
    )
    {% endif %}
    -- Select only if the trip is finished
    AND pickup_location_id IS NOT NULL
    AND pickup_datetime IS NOT NULL
    AND dropoff_location_id IS NOT NULL
    AND dropoff_datetime IS NOT NULL
),


-- Logic

taxi_trips_unpivot_res AS (
    SELECT
        source.vendor_id,
        source.pickup_location_id,
        source.dropoff_location_id,
        source.date_hour_pickup_datetime,
        source.date_hour_dropoff_datetime,
        source.type AS taxi_type,
        source.payment_type, -- TODO check if is numbers convert it to strings with case when
        source.trip_type, -- TODO check if is numbers convert it to strings with case when
        source.rate_code_id, -- TODO check if is numbers convert it to strings with case when
        MAX(source.partition_date) AS partition_date,
        STACK(13,
            'total_trips', COUNT(*),
            'total_improvement_surcharge', SUM(source.improvement_surcharge),
            'total_congestion_surcharge', SUM(source.congestion_surcharge),
            'total_passenger_count', SUM(source.passenger_count),
            'total_trip_distance', SUM(source.trip_distance),
            'total_mta_tax', SUM(source.mta_tax),
            'total_extra', SUM(source.extra),
            'total_fare_amount', SUM(source.fare_amount),
            'total_tip_amount', SUM(source.tip_amount),
            'total_tolls_amount', SUM(source.tolls_amount),
            'total_amount', SUM(source.total_amount),
            'total_airport_fee', SUM(IF(source.fee_type = 'airport', source.total_fee, 0)),
            'total_ehail_fee', SUM(IF(source.fee_type = 'ehail', source.total_fee, 0))
        ) AS (metric_name, metric_value),
        CURRENT_TIMESTAMP() AS dwh_updated_at
    FROM import_stg_taxi_trips AS source
    GROUP BY 1,2,3,4,5,6,7,8,9
)


-- Results

SELECT * FROM taxi_trips_unpivot_res
