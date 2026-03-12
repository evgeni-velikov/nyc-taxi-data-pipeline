{% set grain_columns = [
    'vendor_id', 'pickup_location_id', 'dropoff_location_id',
    'date_hour_pickup_datetime', 'taxi_type',
] %}
{% set metric_columns = [
    'total_trips', 'total_passenger_count', 'total_trip_distance',
] %}

{{
    config(
        unique_key=[
            'operator_id', 'operator_type', 'pickup_location_id', 'dropoff_location_id',
            'date_hour_pickup_datetime', 'taxi_type',
        ],
        incremental_strategy='merge',
        materialized='incremental',
        partition_by=['pickup_date'],
        cluster_by=['pickup_location_id', 'dropoff_location_id', 'operator_id']
    )
}}

WITH

{{
    fact_aggregation_model(
        source_model='int_taxi_trips_zone_activity',
        grain_columns=grain_columns,
        metric_columns=metric_columns,
        watermark_condition="operator_type = 'vendor'"
    )
}},

-- FHV dispatching base import

import_stg_fhv_trips AS (
    SELECT
        *,
        DATE(pickup_datetime) AS pickup_date
    FROM {{ ref('stg_fhv_trips') }}
    {% if is_incremental() %}
    WHERE dwh_updated_at >= (
        SELECT COALESCE(MAX(max_dwh_updated_at), TIMESTAMP '1900-01-01')
        FROM {{ this }}
        WHERE operator_type = 'dispatching_base'
    )
    {% endif %}
),


-- Logic

dispatching_base_agg AS (
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
        MAX(pickup_date) AS pickup_date,
        MAX(dwh_updated_at) AS max_dwh_updated_at,
        {{ timestamp_mock() }} AS dwh_updated_at
    FROM import_stg_fhv_trips
    GROUP BY 1, 3, 4, 5, 6
),

vendor_agg AS (
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
        pickup_date,
        max_dwh_updated_at,
        dwh_updated_at
    FROM final
)


-- Result

SELECT * FROM dispatching_base_agg
UNION ALL
SELECT * FROM vendor_agg
