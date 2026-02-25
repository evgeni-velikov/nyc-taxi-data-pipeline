{{ config(
    tags=['unit','silver','stg_fhv_trip','incremental'],
    vars={'dwh_updated_at': '2024-01-01 15:00:00'}
) }}

{% call dbt_unit_testing.test('stg_fhv_trip', 'incremental insert') %}

  -- mock source (new batch)
  {% call dbt_unit_testing.mock_source(
        'raw',
        'fhv_trip',
        csv='tests/unit/silver/stg_fhv_trip/incremental/fixtures/new_batch.csv'
  ) %}
  {% endcall %}

  -- mock existing incremental table
  {% call dbt_unit_testing.mock_model(
        'stg_fhv_trip',
        csv='tests/unit/silver/stg_fhv_trip/incremental/fixtures/this.csv'
  ) %}
  {% endcall %}

  -- expected result
  {% call dbt_unit_testing.expect(
        csv='tests/unit/silver/stg_fhv_trip/incremental/fixtures/expected.csv'
  ) %}
  {% endcall %}

{% endcall %}
