{{ config(materialized='view', tags=['bronze']) }}


SELECT
    dispatching_base_num,
    pickup_datetime,
    dropOff_datetime,
    PULocationID,
    DOLocationID,
    SR_Flag,
    Affiliated_base_number,
    processing_time,
    partition_date
FROM {{ safe_source('bronze', 'fhv_trip_data') }}
