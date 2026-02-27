{{ config(
    tags=['unit','silver','stg_fhv_trips','basic'],
    vars={'unit_test': true, 'mock_schema': 'mock', 'fhv_trip_data_model': 'input_fhv_trips_data'}
) }}

-- depends_on: {{ ref('input_fhv_trips_data') }}
{#-- depends_on: {{ ref('fhv_trip_data') }}#}
-- depends_on: {{ ref('expected_stg_fhv_trips') }}

{% call dbt_unit_testing.test(
    'stg_fhv_trips','basic logic',
    options={
        "vars": {
            'unit_test': true,
            'fhv_trip_data_model': 'input_fhv_trips_data'
        }
    }
) %}

{#  {% call dbt_unit_testing.mock_ref('fhv_trip_data') %}#}
{#      select * from {{ ref('input_fhv_trips_data') }}#}
{#  {% endcall %}#}

  {% call dbt_unit_testing.expect() %}
      select
{#        *#}
        'B00001' as dispatching_base_id
{#      from {{ ref('expected_stg_fhv_trips') }}#}
  {% endcall %}

{% endcall %}
