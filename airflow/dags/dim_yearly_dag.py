from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

with DAG(
    dag_id="dim_yearly_gold",
    start_date=datetime(2024,2,23),
    schedule="@yearly",
        catchup=False,
) as dag:

    dbt_run = DockerOperator(
        task_id="dim_date_calendar",
        image="data-dbt:latest",
        command="dbt build --select dim_date_calendar",
        working_dir="/dbt",
        auto_remove=True,
        retries=3,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(hours=1)
    )
