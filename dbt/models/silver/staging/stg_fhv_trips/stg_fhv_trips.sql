{{
    config(
        incremental_strategy='append',
        partition_by=['partition_date']
    )
}}

WITH

{% if is_incremental() %}
get_max_partition_date AS (
    SELECT COALESCE(MAX(partition_date), DATE '1900-01-01') AS max_date
    FROM {{ this }}
),
{% endif %}

{% set required_columns = [
    'dispatching_base_num', 'PULocationID', 'pickup_datetime',
    'DOLocationID', 'dropOff_datetime'
] %}

import_fhv_trip_data AS (
    SELECT *
    FROM {{ ref('vw_fhv_trip_data') }}
    WHERE 1=1
    {% if is_incremental() %}
    AND partition_date > (SELECT max_date FROM get_max_partition_date)
    {% endif %}
    {% for column in required_columns %}
    AND {{ column }} IS NOT NULL
    {% endfor %}
    AND pickup_datetime < dropoff_datetime
    AND DATE_TRUNC('month', pickup_datetime) BETWEEN ADD_MONTHS(partition_date, -1) AND partition_date
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
        {{ timestamp_mock() }} as dwh_updated_at
    FROM import_fhv_trip_data
)


-- Result

SELECT * FROM stg_fhv_trip_res
