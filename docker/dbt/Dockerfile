FROM python:3.12-slim

RUN apt-get update && apt-get install -y git

COPY requirements /tmp/requirements
RUN pip install --no-cache-dir -r /tmp/requirements/dbt.txt

WORKDIR /dbt

COPY dbt/ /dbt
COPY dbt/profiles /root/.dbt

RUN dbt deps
