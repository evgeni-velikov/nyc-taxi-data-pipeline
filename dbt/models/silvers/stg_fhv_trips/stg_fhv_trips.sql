{% set is_unit = var('unit_test', false) %}
{% set run_incremental = not is_unit and is_incremental() %}

{{
    config(
        materialized = 'view' if is_unit else 'incremental',
        incremental_strategy='append',
        partition_by=['partition_date']
    )
}}

WITH

{% if run_incremental %}
get_max_partition_date AS (
    SELECT COALESCE(MAX(partition_date), DATE '1900-01-01') AS max_date
    FROM {{ this }}
),
{% endif %}

import_fhv_trip_data AS (
    SELECT *
    FROM {{ ref('fhv_trip_data') }}
    {% if run_incremental %}
    WHERE partition_date > (SELECT max_date FROM get_max_partition_date)
    {% endif %}
),

stg_fhv_trip_res AS (
    SELECT
        pickup_datetime,
        dropOff_datetime AS dropoff_datetime,
        CAST(PULocationID AS BIGINT) AS pickup_location_id,
        CAST(DOLocationID AS BIGINT) AS dropoff_location_id,
        SR_Flag AS surcharge_flag,
        Affiliated_base_number AS affiliated_base_number,
        dispatching_base_num AS dispatching_base_id,
        processing_time,
        partition_date,
        CURRENT_TIMESTAMP as dwh_updated_at
    FROM import_fhv_trip_data
)

SELECT * FROM stg_fhv_trip_res
