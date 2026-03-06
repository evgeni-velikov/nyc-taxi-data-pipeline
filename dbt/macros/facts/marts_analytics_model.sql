{% macro marts_analytics_model(fact_table, grain_columns, fact_columns, avg_divide_col) %}

    WITH

    -- Import

    fact AS (
        SELECT *
        FROM {{ ref(fact_table) }}
    ),
    calendar AS (
        SELECT *
        FROM {{ ref('dim_date_calendar') }}
    ),
    zones AS (
        SELECT *
        FROM {{ ref('vw_taxi_trip_zones') }}
    ),

    -- Logic

    marts_analytics_res AS (
        SELECT
            {% for col in grain_columns %}
            fact.{{ col }},
            {% endfor %},

            zones.Borough AS borough,
            zones.Zone AS zone,
            zones.service_zone,

            -- fact metrics
            COALESCE(fact.{{avg_divide_col}}, 0) AS {{ avg_divide_col }},
            {% for col in fact_columns %}
            COALESCE(fact.total_{{ col }}, 0) AS total_{{ col }},
            COALESCE(
                fact.total_{{ col }} / NULLIF(fact.{{ avg_divide_col }}, 0), 0
            ) AS avg_{{ col }},
            {% endfor %}

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
        FROM fact
        INNER JOIN calendar
            ON fact.date_hour_pickup_datetime = calendar.datetime_hour
        LEFT JOIN zones
            ON fact.pickup_location_id = zones.LocationID
    )


    -- Result

    SELECT *
    FROM marts_analytics_res

{% endmacro %}
