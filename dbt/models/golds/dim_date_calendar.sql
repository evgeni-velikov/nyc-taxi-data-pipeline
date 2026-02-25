{{ config(materialized='table') }}


WITH

dim_date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2010-01-01' as date)",
        end_date="cast('2050-12-31' as date)"
    ) }}
),

dim_date_calendar_res AS (
    SELECT
        CAST(date_day AS date) AS date,
        CAST(date_format(date_day, 'yyyyMMdd') AS INT) AS date_key,
        YEAR(date_day) AS year,
        MONTH(date_day) AS month,
        DAY(date_day) AS day
    FROM dim_date_spine
)

SELECT * FROM dim_date_calendar_res
