{{
    config(
        materialized='incremental',
        unique_key=[
            'vendor_id', 'pickup_location_id', 'dropoff_location_id',
            'date_hour_pickup_datetime', 'taxi_type'
        ],
        incremental_strategy='merge',
        partition_by=['partition_date'],
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
    WHERE partition_date >= (SELECT max_partition_date FROM get_max_partition_date)
    {% endif %}
),


-- Logic

final AS (
    SELECT
        vendor_id,
        pickup_location_id,
        dropoff_location_id,
        date_hour_pickup_datetime,
        type AS taxi_type,
        MAX(partition_date) AS partition_date,
        COUNT(*) AS total_trips,
        SUM(passenger_count) AS total_passenger_count,
        SUM(trip_distance) AS total_trip_distance,
        {{ timestamp_mock() }} AS dwh_updated_at
    FROM import_stg_taxi_trips
    GROUP BY 1, 2, 3, 4, 5
)


-- Result

SELECT * FROM final
