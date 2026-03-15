import os

from docker.types import Mount
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sensors.external_task import ExternalTaskSensor

from constants import (
    HOST_PROJECT_PATH,
    COMMON_DOCKER_ARGS,
    ENVIRONMENT_DOCKER_ARGS,
    SPARK_IMAGE,
    SPARK_MASTER
)


def send_notification_email(context: dict):
    subject = f"DAG Failed: {context['dag'].dag_id}"
    body = f"""
        DAG: {context['dag'].dag_id}
        Task: {context['task_instance'].task_id}
        Execution date: {context['execution_date']}
        Log: {context['task_instance'].log_url}
        """
    # send_email(
    #     to=["your@email.com"],
    #     subject=subject,
    #     html_content=body,
    # )


def create_snowflake_export_task(task_id, source_table, target_table, cluster_by: list = None):
    return DockerOperator(
        task_id=task_id,
        image=SPARK_IMAGE,  # твоят spark image
        api_version="auto",
        command=[
            "spark-submit",
            "--master", SPARK_MASTER,  # не хардкоднато
            "/app/src/jobs/export_to_snowflake.py",
            "--source_table", source_table,
            "--target_table", target_table,
            "--cluster_by", *(cluster_by or []),
        ],
        environment={
            "SNOWFLAKE_ACCOUNT": os.environ.get("SNOWFLAKE_ACCOUNT"),
            "SNOWFLAKE_USER": os.environ.get("SNOWFLAKE_USER"),
            "SNOWFLAKE_PASSWORD": os.environ.get("SNOWFLAKE_PASSWORD"),
            "SNOWFLAKE_DATABASE": os.environ.get("SNOWFLAKE_DATABASE"),
            "SNOWFLAKE_WAREHOUSE": os.environ.get("SNOWFLAKE_WAREHOUSE"),
            "SNOWFLAKE_SCHEMA": os.environ.get("SNOWFLAKE_SCHEMA", "NYC_TAXI"),
            "SNOWFLAKE_ROLE": os.environ.get("SNOWFLAKE_ROLE"),
            **ENVIRONMENT_DOCKER_ARGS,
        },
        mounts=[
            Mount(source=f"{HOST_PROJECT_PATH}/src", target="/app/src", type="bind"),
            Mount(source=f"{HOST_PROJECT_PATH}/warehouse", target="/warehouse", type="bind"),
        ],
        **COMMON_DOCKER_ARGS,
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


def create_spark_task(task_id: str):
    return DockerOperator(
        task_id=f"{task_id}_data",
        image=SPARK_IMAGE,
        command=["spark-submit", "--master", SPARK_MASTER, f"/app/src/jobs/{task_id}.py"],
        environment={
            "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID"),
            "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY"),
            "S3_ENDPOINT": os.environ.get("S3_ENDPOINT"),
            **ENVIRONMENT_DOCKER_ARGS
        },
        mounts=[
            Mount(source=f"{HOST_PROJECT_PATH}/src", target="/app/src", type="bind"),
            Mount(source=f"{HOST_PROJECT_PATH}/warehouse", target="/warehouse", type="bind"),
        ],
        **COMMON_DOCKER_ARGS
    )

def create_dbt_model_task(schema: str, model_name: str, outlets: list = None):
    return DockerOperator(
        task_id=f"{schema}_{model_name}",
        image="data-dbt:latest",
        working_dir="/dbt",
        command=["dbt", "build", "--target", "prod", "--select", model_name],
        # retries=2,
        # retry_delay=timedelta(minutes=5),
        environment=ENVIRONMENT_DOCKER_ARGS,
        outlets=outlets or [],
        mounts=[
            Mount(source=f"{HOST_PROJECT_PATH}/dbt", target="/dbt", type="bind"),
            Mount(source=f"{HOST_PROJECT_PATH}/dbt/profiles", target="/root/.dbt", type="bind"),
        ],
        **COMMON_DOCKER_ARGS,
    )


def create_dbt_freshness_task():
    return DockerOperator(
        task_id="dbt_source_freshness",
        image="data-dbt:latest",
        working_dir="/dbt",
        command=["dbt", "source", "freshness", "--target", "prod"],
        environment=ENVIRONMENT_DOCKER_ARGS,
        mounts=[
            Mount(source=f"{HOST_PROJECT_PATH}/dbt", target="/dbt", type="bind"),
            Mount(source=f"{HOST_PROJECT_PATH}/dbt/profiles", target="/root/.dbt", type="bind"),
        ],
        **COMMON_DOCKER_ARGS,
    )