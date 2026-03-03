from datetime import timedelta
from docker.types import Mount
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sensors.external_task import ExternalTaskSensor

from .constants import HOST_PROJECT_PATH, COMMON_DOCKER_ARGS


def send_notification_email(context: dict):
    subject = f"DAG Failed: {context['dag'].dag_id}"
    body = f"""
        DAG: {context['dag'].dag_id}
        Task: {context['task_instance'].task_id}
        Execution date: {context['execution_date']}
        Log: {context['task_instance'].log_url}
        """
    send_email(
        to=["your@email.com"],
        subject=subject,
        html_content=body,
    )


def create_external_sensor(task_id: str, external_task_id: str, external_dag_id: str):
    return ExternalTaskSensor(
        task_id=task_id,
        external_dag_id=external_dag_id,
        external_task_id=external_task_id,
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=60,
        timeout=60 * 60 * 6,
    )


def create_dbt_model_task(schema: str, model_name: str):
    return DockerOperator(
        task_id=f"{schema}.{model_name}",
        image="data-dbt:latest",
        working_dir="/dbt",
        command=[f"dbt run --target prod --select {model_name}"],
        retries=2,
        retry_delay=timedelta(minutes=5),
        mounts=[
            Mount(f"{HOST_PROJECT_PATH}/dbt", "/dbt", "bind"),
            Mount(f"{HOST_PROJECT_PATH}/dbt/profiles", "/root/.dbt", "bind"),
        ],
        **COMMON_DOCKER_ARGS,
    )


def create_dbt_freshness_task():
    return DockerOperator(
        task_id="dbt_source_freshness",
        image="data-dbt:latest",
        working_dir="/dbt",
        command=["dbt", "source", "freshness", "--target", "prod"],
        mounts=[
            Mount(f"{HOST_PROJECT_PATH}/dbt", "/dbt", "bind"),
            Mount(f"{HOST_PROJECT_PATH}/dbt/profiles", "/root/.dbt", "bind"),
        ],
        **COMMON_DOCKER_ARGS,
    )