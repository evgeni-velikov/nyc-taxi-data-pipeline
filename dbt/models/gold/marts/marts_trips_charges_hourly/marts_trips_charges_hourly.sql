{{ config(materialized='view') }}

{% set fact_table = 'fact_charges_hourly' %}
{% set avg_divide_col = 'total_trips' %}
{% set grain_columns = [
    'vendor_id',
    'pickup_location_id',
    'dropoff_location_id',
    'taxi_type',
] %}
{% set fact_columns = [
    'airport_fee',
    'ehail_fee',
    'improvement_surcharge',
    'congestion_surcharge',
    'mta_tax',
    'extra',
] %}

{{
    marts_analytics_model(
        fact_table,
        grain_columns,
        fact_columns,
        avg_divide_col
    )
}}
