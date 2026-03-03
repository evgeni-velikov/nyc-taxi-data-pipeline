import os
from datetime import datetime

from docker.types import Mount
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

from ..common import create_dbt_model_task


SPARK_IMAGE = os.getenv("SPARK_IMAGE", "nyc-taxi-data-pipeline-spark-master")
SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
DOCKER_NETWORK = os.getenv("DOCKER_NETWORK", "nyc-taxi-data-pipeline")
HOST_PROJECT_PATH= os.getenv("HOST_PROJECT_PATH", "/Users/velikov/Projects/nyc-taxi-data-pipeline")
environments = {
    "PYTHONUNBUFFERED": "1",
    "DBT_USER": "evgeni",
}

COMMON_DOCKER_ARGS = {
    "docker_url": "unix://var/run/docker.sock",
    "network_mode": DOCKER_NETWORK,
    "mount_tmp_dir": False,
    "environment": environments,
}

with DAG(
    dag_id="ingestion_dag",
    start_date=datetime(2024, 1, 1),
    schedule="0 1 * * *",
    catchup=False,
    tags=["spark", "dbt", "bronze", "ingestion"],
) as ingestion_dag:

    spark_task = DockerOperator(
        task_id="ingestion_data",
        image=SPARK_IMAGE,
        command=[
            "spark-submit",
            "--master",
            SPARK_MASTER,
            "/app/src/jobs/ingestion.py",
        ],
        mounts=[
            Mount(f"{HOST_PROJECT_PATH}/src", "/app/src", "bind"),
            Mount(f"{HOST_PROJECT_PATH}/warehouse", "/warehouse", "bind"),
        ],
        **COMMON_DOCKER_ARGS,
    )

    bronze_models = [
        "fhv_trip_data",
        "green_trip_data",
        "yellow_trip_data",
    ]
    bronze_tasks = [create_dbt_model_task(schema='bronze', model_name=model) for model in bronze_models]
    spark_task >> bronze_tasks
