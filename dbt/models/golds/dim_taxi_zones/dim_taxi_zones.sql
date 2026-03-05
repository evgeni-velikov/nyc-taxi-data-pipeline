{{ config(materialized='table') }}

WITH

import_taxi_trip_zones AS (
    SELECT *
    FROM {{ ref('vw_taxi_trip_zones') }}
),

dim_taxi_zones_res AS (
    SELECT
        LocationID AS location_id,
        Borough AS borough,
        Zone AS zone,
        service_zone
    FROM import_taxi_trip_zones
)

SELECT * FROM dim_taxi_zones_res
