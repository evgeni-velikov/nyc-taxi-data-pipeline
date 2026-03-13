{% set grain_columns = [
    'vendor_id', 'pickup_location_id', 'dropoff_location_id',
    'date_hour_pickup_datetime', 'taxi_type',
] %}
{% set metric_columns = [
    'total_trips',
    'total_improvement_surcharge', 'total_congestion_surcharge',
    'total_mta_tax', 'total_extra',
    'total_airport_fee', 'total_ehail_fee',
] %}

{{
    config(
        unique_key=grain_columns,
        incremental_strategy='merge',
        materialized='incremental',
        partition_by=['pickup_month'],
        cluster_by=['pickup_location_id', 'dropoff_location_id', 'vendor_id']
    )
}}

WITH

{{
    fact_aggregation_model(
        source_model='int_taxi_trips_charges',
        grain_columns=grain_columns,
        metric_columns=metric_columns
    )
}}

SELECT * FROM final
