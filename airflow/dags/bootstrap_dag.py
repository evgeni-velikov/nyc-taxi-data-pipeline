from datetime import datetime
from docker.types import Mount
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

from ..common.constants import SPARK_IMAGE, COMMON_DOCKER_ARGS, SPARK_MASTER, HOST_PROJECT_PATH


with DAG(
    dag_id="bootstrap_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["spark", "bootstrap"],
) as bootstrap_dag:

    spark_task = DockerOperator(
        task_id="bootstrap_data",
        image=SPARK_IMAGE,
        command=[
            "spark-submit",
            "--master",
            SPARK_MASTER,
            "/app/src/jobs/bootstrap.py",
        ],
        mounts=[
            Mount(source=f"{HOST_PROJECT_PATH}/src", target="/app/src", type="bind"),
            Mount(source=f"{HOST_PROJECT_PATH}/warehouse", target="/warehouse", type="bind"),
        ],
        **COMMON_DOCKER_ARGS
    )
