{{ config(
    tags=['unit','silver','stg_fhv_trips','basic'],
    vars={'dwh_updated_at': '2020-01-01 15:00:00', 'unit_test': true}
) }}

-- depends_on: {{ ref('input_fhv_trips_data') }}
-- depends_on: {{ ref('fhv_trip_data') }}
-- depends_on: {{ ref('expected_stg_fhv_trips') }}

{% call dbt_unit_testing.test('stg_fhv_trips','basic logic') %}

  {% call dbt_unit_testing.mock_ref('fhv_trip_data') %}
      select * from {{ ref('input_fhv_trips_data') }}
  {% endcall %}

  {% call dbt_unit_testing.expect() %}
      select * from {{ ref('expected_stg_fhv_trips') }}
  {% endcall %}

{% endcall %}
