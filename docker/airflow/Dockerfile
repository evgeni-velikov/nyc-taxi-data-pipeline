FROM apache/airflow:2.9.3

USER root
RUN pip install dbt-core dbt-spark[PyHive] pyspark boto3
USER airflow
