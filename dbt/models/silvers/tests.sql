{{ config(materialized='incremental') }}

with import_yellow as (
    select *
    from {{ source('bronze', 'yellow_trip_data') }}
)

select *
from import_yellow
