import os
from datetime import datetime, timedelta
from docker.types import Mount

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

SPARK_IMAGE = os.getenv("SPARK_IMAGE", "nyc-taxi-data-pipeline-spark-master")
SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
DOCKER_NETWORK = os.getenv("DOCKER_NETWORK", "nyc-taxi-data-pipeline")
HOST_PROJECT_PATH= os.getenv("HOST_PROJECT_PATH", "/Users/velikov/Projects/nyc-taxi-data-pipeline")
environments = {
    "PYTHONUNBUFFERED": "1",  # realtime dbt logs
    "DBT_USER": "evgeni",
}

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
        command=["bash", "-c", "dbt deps && dbt run --target prod --select dim_date_calendar"],
        docker_url="unix://var/run/docker.sock",
        network_mode="nyc-taxi-data-pipeline",
        working_dir="/dbt",
        auto_remove=True,
        mount_tmp_dir=False,
        tty=True,  # Better logs
        environment=environments,
        mounts=[
            Mount(source=f"{HOST_PROJECT_PATH}/dbt", target="/dbt", type="bind"),
            Mount(source=f"{HOST_PROJECT_PATH}/dbt/profiles", target="/root/.dbt", type="bind"),
        ],
    )

    spark_test = DockerOperator(
        task_id="test_dim_date",
        image=SPARK_IMAGE,
        docker_url="unix://var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        auto_remove=False,  # keep container for logs/debug
        mount_tmp_dir=False,  # avoid airflow tmp mount bug
        tty=False,
        command=[
            "spark-submit",
            "--master",
            SPARK_MASTER,
            "/app/src/test_two.py",
        ],
        environment=environments,
        mounts=[
            Mount(source=f"{HOST_PROJECT_PATH}/src", target="/app/src", type="bind"),
            Mount(source=f"{HOST_PROJECT_PATH}/warehouse", target="/warehouse", type="bind"),
        ],
    )
