{% set metrics = [
    'total_trips', 'total_passenger_count', 'total_trip_distance',
] %}
{% set unique_keys = [
    'vendor_id', 'pickup_location_id', 'dropoff_location_id',
    'date_hour_pickup_datetime', 'date_hour_dropoff_datetime', 'taxi_type',
] %}

{{
    config(
        unique_key = unique_keys,
        incremental_strategy='merge',
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

union_models AS (
    SELECT
        dispatching_base_id AS operator_id,
        'dispatching_base' AS operator_type,
        pickup_location_id,
        dropoff_location_id,
        date_hour_pickup_datetime,
        date_hour_dropoff_datetime,
        'fhv' AS taxi_type,
        total_trips,
        BIGINT(NULL) AS total_passenger_count,
        DOUBLE(NULL) AS total_trip_distance,
        MAX(dwh_updated_at) AS max_dwh_updated_at,
        CURRENT_TIMESTAMP() AS dwh_updated_at
    FROM import_stg_fhv_trips
    UNION ALL
    SELECT
        vendor_id AS operator_id,
        'vendor' AS operator_type,
        pickup_location_id,
        dropoff_location_id,
        date_hour_pickup_datetime,
        date_hour_dropoff_datetime,
        taxi_type,
        total_trips,
        total_passenger_count,
        total_trip_distance,
        MAX(dwh_updated_at) AS max_dwh_updated_at,
        CURRENT_TIMESTAMP() AS dwh_updated_at
    FROM fact_zone_activity_hourly
)

SELECT * FROM union_models
