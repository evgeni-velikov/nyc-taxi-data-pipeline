{{
    config(
        materialized='table',
        partition_by=["date"],
        cluster_by=["pickup_location_id", "dropoff_location_id", "vendor_id", "payment_type"]
    )
}}

WITH

-- Import

import_fact_revenue_hourly AS (
    SELECT *
    FROM {{ ref('fact_revenue_hourly') }}
),
import_dim_date_calendar AS (
    SELECT *
    FROM {{ ref('dim_date_calendar') }}
),
import_dim_taxi_zones AS (
    SELECT *
    FROM {{ ref('dim_taxi_zones') }}
),


-- Logic

marts_trips_revenue_hourly_res AS (
    SELECT
        fact.vendor_id,
        fact.pickup_location_id,
        fact.dropoff_location_id,
        fact.taxi_type,
        fact.payment_type,

        zones.borough,
        zones.zone,
        zones.service_zone,

        COALESCE(fact.total_trips, 0) AS total_trips,
        COALESCE(fact.total_fare_amount, 0) AS total_fare_amount,
        COALESCE(fact.total_service_charge_amount / NULLIF(fact.total_trips, 0), 0) AS avg_service_charge_amount,
        COALESCE(fact.total_tip_amount, 0) AS total_tip_amount,
        COALESCE(fact.total_tip_amount / NULLIF(fact.total_trips, 0), 0) AS avg_tip_amount,
        COALESCE(fact.total_tolls_amount, 0) AS total_tolls_amount,
        COALESCE(fact.total_tolls_amount / NULLIF(fact.total_trips, 0), 0) AS avg_tolls_amount,
        COALESCE(fact.total_amount, 0) AS total_amount,
        COALESCE(fact.total_amount / NULLIF(fact.total_trips, 0), 0) AS avg_total_amount,

        calendar.datetime_hour,
        calendar.date,
        calendar.year,
        calendar.fiscal_year,
        calendar.quarter,
        calendar.fiscal_quarter,
        calendar.month,
        calendar.month_name,
        calendar.week_of_year,
        calendar.day_of_week,
        calendar.is_weekend,
        calendar.day,
        calendar.hour
    FROM import_fact_revenue_hourly AS fact
    INNER JOIN import_dim_date_calendar AS calendar
        ON fact.date_hour_pickup_datetime = calendar.datetime_hour
    LEFT JOIN import_dim_taxi_zones AS zones
        ON fact.pickup_location_id = zones.location_id
)


-- Result

SELECT *
FROM marts_trips_revenue_hourly_res
