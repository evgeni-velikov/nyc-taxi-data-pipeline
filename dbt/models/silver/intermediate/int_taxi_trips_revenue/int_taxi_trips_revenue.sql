{% set grain_columns = [
    'vendor_id',
    'pickup_location_id',
    'dropoff_location_id',
    'date_hour_pickup_datetime',
    "DATE_TRUNC('MONTH', date_hour_pickup_datetime) AS pickup_month",
    'type AS taxi_type',
    'payment_type',
] %}
{% set metrics = [
    {'name': 'total_trips',        'expr': 'COUNT(*)'},
    {'name': 'total_fare_amount',  'expr': 'SUM(fare_amount)'},
    {'name': 'total_tip_amount',   'expr': 'SUM(tip_amount)'},
    {'name': 'total_tolls_amount', 'expr': 'SUM(tolls_amount)'},
    {'name': 'total_amount',       'expr': 'SUM(total_amount)'},
] %}

{{
    config(
        unique_key=[
            'vendor_id', 'pickup_location_id', 'dropoff_location_id',
            'date_hour_pickup_datetime', 'taxi_type', 'payment_type'
        ],
        incremental_strategy='merge',
        partition_by=['pickup_month'],
        cluster_by=['pickup_location_id', 'dropoff_location_id', 'vendor_id']
    )
}}

WITH

{{ int_taxi_trips_aggregation(grain_columns, metrics) }}

SELECT * FROM final
