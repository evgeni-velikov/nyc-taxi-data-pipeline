{% set grain_columns = [
    'vendor_id', 'pickup_location_id', 'dropoff_location_id',
    'date_hour_pickup_datetime', 'taxi_type', 'payment_type',
] %}
{% set metric_columns = [
    'total_trips',
    'total_fare_amount', 'total_tip_amount',
    'total_tolls_amount', 'total_amount',
] %}

{{
    config(
        unique_key=grain_columns,
        incremental_strategy='merge',
        materialized='incremental',
        cluster_by=['vendor_id', 'pickup_location_id', 'dropoff_location_id', 'date_hour_pickup_datetime']
    )
}}

WITH

{{
    fact_aggregation_model(
        source_model='int_taxi_trips_revenue',
        grain_columns=grain_columns,
        metric_columns=metric_columns
    )
}}

SELECT * FROM final
