from datetime import datetime
from docker.types import Mount
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

from utilities import create_dbt_model_task


with DAG(
    dag_id="dim_yearly_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@yearly",
    catchup=False,
    tags=["dbt", "dim"],
    max_active_runs=1,
    max_active_tasks=2,
) as dim_yearly_dag:

    create_dbt_model_task(model_name="dim_date_calendar", schema="gold")
