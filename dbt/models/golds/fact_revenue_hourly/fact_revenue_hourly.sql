{% set metrics = [
    'total_trips', 'total_fare_amount', 'total_tip_amount',
    'total_tolls_amount', 'total_amount',
] %}
{% set unique_keys = [
    'vendor_id', 'pickup_location_id', 'dropoff_location_id',
    'date_hour_pickup_datetime', 'taxi_type', 'payment_type',
] %}

{{
    config(
        unique_key = unique_keys,
        incremental_strategy='merge',
        cluster_by=['vendor_id', 'pickup_location_id', 'dropoff_location_id', 'date_hour_pickup_datetime']
    )
}}

WITH

{{
    fact_aggregation_model(
        metrics=metrics,
        grain_columns=unique_keys
    )
}}

SELECT * FROM final_agg
