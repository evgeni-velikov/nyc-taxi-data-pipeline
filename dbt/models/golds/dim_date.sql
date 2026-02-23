{{ config(materialized='table', alias='dim_date') }}


WITH
dim_date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2010-01-01' as date)",
        end_date="cast('2050-12-31' as date)"
    ) }}
),
dim_date_res AS (
    SELECT
        date_day AS date,
        CAST(TO_CHAR(date_day, 'YYYYMMDD') AS INT) AS date_key,
        EXTRACT(year FROM date_day) AS year,
        EXTRACT(month FROM date_day) AS month,
        EXTRACT(day FROM date_day) AS day
    FROM dim_date_spine
)

SELECT * FROM dim_date_res
