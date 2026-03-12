{{ config(materialized='view') }}

{#        cluster_by=["date", "pickup_location_id", "dropoff_location_id", "vendor_id"]#}

{% set fact_table = 'fact_revenue_hourly' %}
{% set avg_divide_col = 'total_trips' %}
{% set grain_columns = [
    'vendor_id',
    'pickup_location_id',
    'dropoff_location_id',
    'taxi_type',
    'payment_type',
] %}
{% set fact_columns = [
    'fare_amount',
    'tip_amount',
    'tolls_amount',
    'amount',
] %}

{{
    marts_analytics_model(
        fact_table,
        grain_columns,
        fact_columns,
        avg_divide_col
    )
}}
