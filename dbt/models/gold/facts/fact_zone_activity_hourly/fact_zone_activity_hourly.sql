{% set metrics = [
    'total_trips', 'total_passenger_count', 'total_trip_distance',
] %}
{% set unique_keys = [
    'vendor_id', 'pickup_location_id', 'dropoff_location_id',
    'date_hour_pickup_datetime', 'taxi_type',
] %}

{{
    config(
        unique_key = unique_keys,
        incremental_strategy='merge',
        materialized='incremental',
        cluster_by=['vendor_id', 'pickup_location_id', 'dropoff_location_id', 'date_hour_pickup_datetime']
    )
}}


WITH

import_stg_fhv_trips AS (
    SELECT * FROM {{ ref('stg_fhv_trips') }}
    {% if is_incremental() %}
    WHERE
        dwh_updated_at >= (
            SELECT COALESCE(MAX(max_dwh_updated_at), TIMESTAMP '1900-01-01')
            FROM {{ this }}
            WHERE operator_type = 'dispatching_base'
        )
   {% endif %}
),

{{
    fact_aggregation_model(
        metrics=metrics,
        grain_columns=unique_keys,
        max_dwh_condition="operator_type = 'vendor'"
    )
}},


-- Logic

fact_zone_activity_hourly_res AS (
    SELECT
        dispatching_base_id AS operator_id,
        'dispatching_base' AS operator_type,
        pickup_location_id,
        dropoff_location_id,
        DATE_TRUNC('HOUR', pickup_datetime) AS date_hour_pickup_datetime,
        'fhv' AS taxi_type,
        COUNT(*) AS total_trips,
        BIGINT(NULL) AS total_passenger_count,
        DOUBLE(NULL) AS total_trip_distance,
        MAX(dwh_updated_at) AS max_dwh_updated_at,
        {{ timestamp_mock() }} AS dwh_updated_at
    FROM import_stg_fhv_trips
    GROUP BY 1,3,4,5,6
    UNION ALL
    SELECT
        vendor_id AS operator_id,
        'vendor' AS operator_type,
        pickup_location_id,
        dropoff_location_id,
        date_hour_pickup_datetime,
        taxi_type,
        total_trips,
        total_passenger_count,
        total_trip_distance,
        max_dwh_updated_at,
        dwh_updated_at
    FROM final_agg
)


-- Result

SELECT * FROM fact_zone_activity_hourly_res
