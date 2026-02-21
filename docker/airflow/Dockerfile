FROM apache/airflow:2.9.3

COPY requirements /tmp/requirements
USER airflow
RUN pip install --no-cache-dir -r /tmp/requirements/airflow.txt
