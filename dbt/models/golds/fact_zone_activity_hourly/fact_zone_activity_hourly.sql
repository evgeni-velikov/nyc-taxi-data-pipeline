{% set metrics = [
    'total_trips', 'total_passenger_count', 'total_trip_distance',
] %}
{% set unique_keys = [
    'vendor_id', 'pickup_location_id', 'dropoff_location_id',
    'date_hour_pickup_datetime' 'taxi_type', 'date_hour_dropoff_datetime',
] %}

{{ fact_aggregation_model(
    metrics,
    unique_keys
) }}
