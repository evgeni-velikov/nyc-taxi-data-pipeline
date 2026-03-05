from datetime import datetime
from docker.types import Mount
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

from utilities import create_spark_task


with DAG(
    dag_id="bootstrap_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["spark", "bootstrap"],
) as bootstrap_dag:

    create_spark_task(task_id="bootstrap")
