{{ config(materialized='view') }}


SELECT
    LocationID,
    Borough,
    Zone,
    service_zone
FROM {{ source('bronze', 'taxi_trip_zones') }}
