from datetime import datetime

from docker.types import Mount
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

from utilities import create_dbt_model_task, create_spark_task


with DAG(
    dag_id="ingestion_dag",
    start_date=datetime(2024, 1, 1),
    schedule="0 1 * * *",
    catchup=False,
    tags=["spark", "dbt", "bronze", "ingestion"],
    max_active_runs=1,
    max_active_tasks=3,
) as ingestion_dag:

    taxi_zone_ingestion_task = create_spark_task(task_id="dim_taxi_zones")
    taxi_trip_zone_view_task = create_dbt_model_task(schema='bronze', model_name='vw_taxi_trip_zones')
    taxi_zone_ingestion_task >> taxi_trip_zone_view_task

    ingestion_task = create_spark_task(task_id="ingestion")
    bronze_models = [
        "vw_fhv_trip_data",
        "vw_green_trip_data",
        "vw_yellow_trip_data",
    ]
    bronze_tasks = [create_dbt_model_task(schema='bronze', model_name=model) for model in bronze_models]
    ingestion_task >> bronze_tasks
