from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime


with DAG(
    dag_id="dim_yearly_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dbt"],
):

    dbt_run = DockerOperator(
        task_id="dim_date_calendar",
        image="data-dbt:latest",
        force_pull=False,
        command=["bash", "-c", "dbt deps && dbt run --select dim_date_calendar"],
        docker_url="unix://var/run/docker.sock",
        network_mode="nyc-taxi-data-pipeline",
        working_dir="/dbt",
        auto_remove=True,
        mount_tmp_dir=False,
        tty=True,  # Better logs
        environment={
            "PYTHONUNBUFFERED": "1",  # realtime dbt logs
        },
        mounts=[
            Mount(
                source="/Users/velikov/Projects/nyc-taxi-data-pipeline/dbt",
                target="/dbt",
                type="bind",
            ),
            Mount(
                source="/Users/velikov/Projects/nyc-taxi-data-pipeline/dbt/profiles",
                target="/root/.dbt",
                type="bind",
            ),
        ],
    )
