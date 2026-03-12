{{
    config(
        unique_key=[
            'vendor_id', 'pickup_location_id', 'dropoff_location_id',
            'date_hour_pickup_datetime', 'taxi_type',
        ],
        incremental_strategy='merge',
        materialized='incremental',
        cluster_by=['vendor_id', 'pickup_location_id', 'dropoff_location_id', 'date_hour_pickup_datetime']
    )
}}

WITH

-- Import

{% if is_incremental() %}
get_max_dwh_updated_at AS (
    SELECT MAX(max_dwh_updated_at) AS max_dwh_updated_at
    FROM {{ this }}
),
{% endif %}

import_source AS (
    SELECT *
    FROM {{ ref('int_taxi_trips_charges') }}
    {% if is_incremental() %}
    WHERE dwh_updated_at > (SELECT max_dwh_updated_at FROM get_max_dwh_updated_at)
    {% endif %}
)


-- Result

SELECT
    vendor_id,
    pickup_location_id,
    dropoff_location_id,
    date_hour_pickup_datetime,
    taxi_type,
    total_trips,
    total_improvement_surcharge,
    total_congestion_surcharge,
    total_mta_tax,
    total_extra,
    total_airport_fee,
    total_ehail_fee,
    dwh_updated_at AS max_dwh_updated_at,
    {{ timestamp_mock() }} AS dwh_updated_at
FROM import_source
