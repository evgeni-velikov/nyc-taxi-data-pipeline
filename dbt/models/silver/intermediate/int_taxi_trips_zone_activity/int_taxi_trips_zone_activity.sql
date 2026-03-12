{% set grain_columns = [
    'vendor_id',
    'pickup_location_id',
    'dropoff_location_id',
    'date_hour_pickup_datetime',
    'DATE(date_hour_pickup_datetime) AS pickup_date',
    'type AS taxi_type',
] %}
{% set metrics = [
    {'name': 'total_trips',            'expr': 'COUNT(*)'},
    {'name': 'total_passenger_count',  'expr': 'SUM(passenger_count)'},
    {'name': 'total_trip_distance',    'expr': 'SUM(trip_distance)'},
] %}

{{
    config(
        materialized='incremental',
        unique_key=[
            'vendor_id', 'pickup_location_id', 'dropoff_location_id',
            'date_hour_pickup_datetime', 'taxi_type'
        ],
        incremental_strategy='merge',
        partition_by=['pickup_date'],
        cluster_by=['pickup_location_id', 'dropoff_location_id', 'vendor_id']
    )
}}

WITH

{{ int_taxi_trips_aggregation(grain_columns, metrics) }}

SELECT * FROM final
