import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


DBT_VOLUME = os.getenv("DBT_PROJECT_VOLUME", "dbt_project")
DBT_PROFILES_VOLUME = os.getenv("DBT_PROFILES_VOLUME", "dbt_profiles")
DOCKER_NETWORK = os.getenv("DOCKER_NETWORK", "nyc-taxi-data-pipeline")
DBT_TARGET = os.getenv("DBT_TARGET", "prod")

default_args = {
    "execution_timeout": timedelta(minutes=30),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dim_yearly_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@yearly",
    catchup=False,
    tags=["dbt"],
    default_args=default_args,
) as dag:

    dbt_run = DockerOperator(
        task_id="dim_date_calendar",
        image="data-dbt:latest",
        force_pull=False,
        command=["bash", "-c", f"dbt deps && dbt run --target {DBT_TARGET} --select dim_date_calendar"],
        docker_url="unix://var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        working_dir="/dbt",
        auto_remove=True,
        mount_tmp_dir=False,
        tty=True,
        environment={
            "PYTHONUNBUFFERED": "1",
        },
        mounts=[
            Mount(source=DBT_VOLUME, target="/dbt", type="volume"),
            Mount(source=DBT_PROFILES_VOLUME, target="/root/.dbt", type="volume"),
        ],
    )
