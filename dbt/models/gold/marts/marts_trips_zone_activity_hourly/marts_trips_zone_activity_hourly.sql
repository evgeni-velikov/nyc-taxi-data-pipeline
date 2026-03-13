{{ config(materialized='view') }}

{% set fact_table = 'fact_zone_activity_hourly' %}
{% set avg_divide_col = 'total_trips' %}
{% set grain_columns = [
    'operator_id',
    'operator_type',
    'pickup_location_id',
    'dropoff_location_id',
    'date_hour_pickup_datetime',
    'taxi_type',
] %}
{% set fact_columns = [
    'passenger_count',
    'trip_distance',
] %}

{{
    marts_analytics_model(
        fact_table,
        grain_columns,
        fact_columns,
        avg_divide_col
    )
}}
