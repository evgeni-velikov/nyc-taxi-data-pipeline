{{
    config(
        unique_key=[
            'vendor_id', 'pickup_location_id', 'dropoff_location_id',
            'date_hour_pickup_datetime', 'taxi_type', 'payment_type',
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
    FROM {{ ref('int_taxi_trips_revenue') }}
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
    payment_type,
    total_trips,
    total_fare_amount,
    total_tip_amount,
    total_tolls_amount,
    total_amount,
    dwh_updated_at AS max_dwh_updated_at,
    {{ timestamp_mock() }} AS dwh_updated_at
FROM import_source
