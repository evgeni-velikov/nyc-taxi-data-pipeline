{{ config(materialized='table') }}

WITH

dim_date_spine AS (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2010-01-01' as date)",
        end_date="cast('2050-12-31' as date)"
    ) }}

),

hours AS (
    SELECT explode(sequence(0,23)) AS hour
),

date_hours AS (
    SELECT
        d.date_day,
        h.hour,
        timestampadd(hour, h.hour, d.date_day) AS datetime_hour
    FROM dim_date_spine d
    CROSS JOIN hours h
),

compute_calendar AS (
    SELECT
        CAST(datetime_hour AS date) AS date,
        datetime_hour,

        CAST(date_format(datetime_hour, 'yyyyMMdd') AS INT) AS date_key,
        CAST(date_format(datetime_hour, 'yyyyMMddHH') AS BIGINT) AS hour_key,

        YEAR(datetime_hour) AS year,
        QUARTER(datetime_hour) AS quarter,
        MONTH(datetime_hour) AS month,
        date_format(datetime_hour, 'MMMM') AS month_name,
        WEEKOFYEAR(datetime_hour) AS week_of_year,
        DAY(datetime_hour) AS day,

        HOUR(datetime_hour) AS hour,

        DAYOFWEEK(datetime_hour) AS day_of_week,
        CASE WHEN DAYOFWEEK(datetime_hour) IN (1, 7) THEN true ELSE false END AS is_weekend,

        CASE
            WHEN MONTH(datetime_hour) >= 4 THEN YEAR(datetime_hour)
            ELSE YEAR(datetime_hour) - 1
        END AS fiscal_year,

        CASE
            WHEN MONTH(datetime_hour) BETWEEN 4 AND 6 THEN 1
            WHEN MONTH(datetime_hour) BETWEEN 7 AND 9 THEN 2
            WHEN MONTH(datetime_hour) BETWEEN 10 AND 12 THEN 3
            ELSE 4
        END AS fiscal_quarter
    FROM date_hours
),

dim_date_calendar_res AS (
    SELECT *
    FROM compute_calendar
)

SELECT * FROM dim_date_calendar_res
