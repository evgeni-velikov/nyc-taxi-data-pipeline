{% set grain_columns = [
    'vendor_id',
    'pickup_location_id',
    'dropoff_location_id',
    'date_hour_pickup_datetime',
    "DATE_TRUNC('MONTH', date_hour_pickup_datetime) AS pickup_month",
    'type AS taxi_type',
] %}
{% set metrics = [
    {'name': 'total_trips',                   'expr': 'COUNT(*)'},
    {'name': 'total_improvement_surcharge',   'expr': 'SUM(improvement_surcharge)'},
    {'name': 'total_congestion_surcharge',    'expr': 'SUM(congestion_surcharge)'},
    {'name': 'total_mta_tax',                 'expr': 'SUM(mta_tax)'},
    {'name': 'total_extra',                   'expr': 'SUM(extra)'},
    {'name': 'total_airport_fee',             'expr': "SUM(COALESCE(IF(fee_type = 'airport', total_fee, 0), 0))"},
    {'name': 'total_ehail_fee',               'expr': "SUM(COALESCE(IF(fee_type = 'ehail', total_fee, 0), 0))"},
] %}

{{
    config(
        materialized='incremental',
        unique_key=[
            'vendor_id', 'pickup_location_id', 'dropoff_location_id',
            'date_hour_pickup_datetime', 'taxi_type'
        ],
        incremental_strategy='merge',
        partition_by=['pickup_month'],
        cluster_by=['pickup_location_id', 'dropoff_location_id', 'vendor_id']
    )
}}

WITH

{{ int_taxi_trips_aggregation(grain_columns, metrics) }}

SELECT * FROM final
