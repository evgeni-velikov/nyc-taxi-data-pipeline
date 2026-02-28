{{ config(materialized='table') }}

WITH

dim_date_spine AS (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2010-01-01' as date)",
        end_date="cast('2050-12-31' as date)"
    ) }}

),

compute_calendar AS (
    SELECT
        CAST(date_day AS date) AS date,

        CAST(date_format(date_day, 'yyyyMMdd') AS INT) AS date_key,

        YEAR(date_day) AS year,
        QUARTER(date_day) AS quarter,
        MONTH(date_day) AS month,
        date_format(date_day, 'MMMM') AS month_name,
        WEEKOFYEAR(date_day) AS week_of_year,
        DAY(date_day) AS day,
        CAST(date_format(date_day, 'u') AS INT) AS day_of_week,

        CASE WHEN date_format(date_day, 'u') IN ('6','7') THEN true ELSE false END AS is_weekend,

        -- Fiscal year (April start)
        CASE
            WHEN MONTH(date_day) >= 4 THEN YEAR(date_day)
            ELSE YEAR(date_day) - 1
        END AS fiscal_year,

        CASE
            WHEN MONTH(date_day) BETWEEN 4 AND 6 THEN 1
            WHEN MONTH(date_day) BETWEEN 7 AND 9 THEN 2
            WHEN MONTH(date_day) BETWEEN 10 AND 12 THEN 3
            ELSE 4
        END AS fiscal_quarter

    FROM dim_date_spine
),

dim_date_calendar_res AS (
    SELECT
        date,
        date_key,
        year,
        quarter,
        month,
        month_name,
        week_of_year,
        day,
        day_of_week,
        is_weekend,
        fiscal_year,
        fiscal_quarter
    FROM compute_calendar
)

SELECT * FROM dim_date_calendar_res
