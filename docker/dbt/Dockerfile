FROM python:3.12-slim

COPY requirements /tmp/requirements
RUN pip install --no-cache-dir -r /tmp/requirements/dbt.txt

WORKDIR /dbt
