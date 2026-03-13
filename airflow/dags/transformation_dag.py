from datetime import datetime

from airflow import DAG
from airflow.datasets import Dataset
from airflow.utils.task_group import TaskGroup

from utilities import (
    create_dbt_model_task,
    create_dbt_freshness_task,
    create_snowflake_export_task,
    send_notification_email
)


fhv_dataset = Dataset("bronze_fhv_trip_data")
green_dataset = Dataset("bronze_green_trip_data")
yellow_dataset = Dataset("bronze_yellow_trip_data")
taxi_trip_zone = Dataset("bronze_taxi_trip_zone")
date_calendar = Dataset("gold_dim_date_calendar")

with DAG(
    dag_id="transformation_dag",
    start_date=datetime(2024, 1, 1),
    schedule=[fhv_dataset, green_dataset, yellow_dataset, taxi_trip_zone, date_calendar],
    catchup=False,
    tags=["dbt", "transformation", "silver", "gold"],
    max_active_runs=1,
    max_active_tasks=4,
    # email_on_failure=True,
    # email="",
    # on_failure_callback=send_notification_email,
) as transformation_dag:

    freshness = create_dbt_freshness_task()

    stg_fhv_trips = create_dbt_model_task("silver", "stg_fhv_trips")
    stg_taxi_trips = create_dbt_model_task("silver", "stg_taxi_trips")

    int_taxi_trips_charges = create_dbt_model_task("silver", "int_taxi_trips_charges")
    int_taxi_trips_revenue = create_dbt_model_task("silver", "int_taxi_trips_revenue")
    int_taxi_trips_zone_activity = create_dbt_model_task("silver", "int_taxi_trips_zone_activity")

    stg_taxi_trips >> [
        int_taxi_trips_charges,
        int_taxi_trips_revenue,
        int_taxi_trips_zone_activity,
    ]

    freshness >> [stg_fhv_trips, stg_taxi_trips]

    fact_charges_hourly = create_dbt_model_task("gold", "fact_charges_hourly")
    fact_revenue_hourly = create_dbt_model_task("gold", "fact_revenue_hourly")
    fact_zone_activity_hourly = create_dbt_model_task("gold", "fact_zone_activity_hourly")

    int_taxi_trips_charges >> fact_charges_hourly
    int_taxi_trips_revenue >> fact_revenue_hourly
    int_taxi_trips_zone_activity >> fact_zone_activity_hourly
    stg_fhv_trips >> fact_zone_activity_hourly

    marts_trips_charges_hourly = create_dbt_model_task("gold", "marts_trips_charges_hourly")
    marts_trips_revenue_hourly = create_dbt_model_task("gold", "marts_trips_revenue_hourly")
    marts_trips_zone_activity_hourly = create_dbt_model_task("gold", "marts_trips_zone_activity_hourly")

    fact_zone_activity_hourly >> marts_trips_zone_activity_hourly
    fact_revenue_hourly >> marts_trips_revenue_hourly
    fact_charges_hourly >> marts_trips_charges_hourly

    snowflake_trips_charge_hourly = create_snowflake_export_task(
        task_id="snowflake_trips_charge_hourly",
        source_table="gold.marts_trips_charges_hourly",
        target_table="trips_charge_hourly",
        cluster_by=["date", "pickup_location_id"],
    )
    snowflake_trips_revenue_hourly = create_snowflake_export_task(
        task_id="snowflake_trips_revenue_hourly",
        source_table="gold.marts_trips_revenue_hourly",
        target_table="trips_revenue_hourly",
        cluster_by=["date", "pickup_location_id"],
    )
    snowflake_trips_zone_activity_hourly = create_snowflake_export_task(
        task_id="snowflake_trips_zone_activity_hourly",
        source_table="gold.marts_trips_zone_activity_hourly",
        target_table="trips_zone_activity_hourly",
        cluster_by=["date", "pickup_location_id"],
    )

    marts_trips_charges_hourly >> snowflake_trips_charge_hourly
    marts_trips_revenue_hourly >> snowflake_trips_revenue_hourly
    marts_trips_zone_activity_hourly >> snowflake_trips_zone_activity_hourly
