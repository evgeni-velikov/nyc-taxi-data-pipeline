{{ config(
    tags=['unit','silver', 'stg_fhv_trips', 'basic'],
    vars={'dwh_updated_at': '2020-01-01 15:00:00'}
) }}

{% call dbt_unit_testing.test('stg_fhv_trip', 'basic logic') %}

  {% call dbt_unit_testing.mock_source(
        'raw',
        'fhv_trip',
        csv='tests/unit/silver/stg_fhv_trip/fixtures/input_fhv_trip_data.csv'
  ) %}
  {% endcall %}

  {% call dbt_unit_testing.expect(
        csv='tests/unit/silver/stg_fhv_trip/fixtures/expected.csv'
  ) %}
  {% endcall %}

{% endcall %}
