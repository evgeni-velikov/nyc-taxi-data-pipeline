{{ config(materialized='view') }}


SELECT
    LocationID,
    Borough,
    Zone,
    service_zone,
    processing_time
FROM {{ source('bronze', 'taxi_trip_zones') }}
