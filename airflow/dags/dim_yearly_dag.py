from datetime import datetime
from docker.types import Mount
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

from ..common.constants import COMMON_DOCKER_ARGS, HOST_PROJECT_PATH


with DAG(
    dag_id="dim_yearly_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@yearly",
    catchup=False,
    tags=["dbt", "dim"],
) as dim_yearly_dag:

    dbt_run = DockerOperator(
        task_id="dim_date_calendar",
        image="data-dbt:latest",
        command=["bash", "-c", "dbt deps && dbt run --target prod --select dim_date_calendar"],
        working_dir="/dbt",
        mounts=[
            Mount(source=f"{HOST_PROJECT_PATH}/dbt", target="/dbt", type="bind"),
            Mount(source=f"{HOST_PROJECT_PATH}/dbt/profiles", target="/root/.dbt", type="bind"),
        ],
        **COMMON_DOCKER_ARGS,
    )
