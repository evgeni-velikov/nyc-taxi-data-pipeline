# NYC Taxi Data Pipeline

![Docker](https://img.shields.io/badge/docker-compose-blue)
![Spark](https://img.shields.io/badge/apache-spark-orange)
![dbt](https://img.shields.io/badge/dbt-transformations-orange)
![Airflow](https://img.shields.io/badge/apache-airflow-red)
![Snowflake](https://img.shields.io/badge/snowflake-serving-29B5E8)
![License](https://img.shields.io/badge/license-MIT-green)

A lakehouse-style data pipeline that ingests NYC Taxi datasets from S3,
processes them with Apache Spark, and builds curated analytics tables using dbt.
The stack runs both locally via Docker Compose and on AWS (EC2 + RDS + S3),
making it suitable for development, experimentation, and data engineering portfolio projects.

---

## Motivation

The purpose of this project is to demonstrate practical experience with 
modern data engineering tools by implementing a complete local data platform.

It showcases a **lakehouse-style data pipeline** built with common production components:

* distributed compute (**Apache Spark**)
* workflow orchestration (**Apache Airflow**)
* transformation layer (**dbt**)
* metadata management (**Hive Metastore**)
* analytical serving (**Snowflake**)

The stack mirrors real-world data engineering architectures while 
remaining lightweight enough to run locally via Docker.


---

## What's included

- **Apache Airflow** — orchestration and scheduling
- **Apache Spark** — ingestion and compute
- **dbt** — transformations (Silver/Gold) and tests
- **Hive Metastore (MySQL-backed)** — table metadata persistence
- **Snowflake** — serving layer for analytical queries (marts pushed via Spark connector)

---

## Architecture Diagram

![Architecture Diagram](docs/architecture.png)

---

## Architecture (high level)

1. **Ingestion (Spark jobs)** loads raw datasets into the **Bronze** layer.
2. **Transformations (dbt)** build curated **Silver** (staging/intermediate) and **Gold** (facts/marts) models.
3. **Airflow** coordinates execution and dependencies between ingestion and transformation steps.
4. The platform can run both **locally** (Docker Compose) and on **AWS** (EC2 + RDS + S3 + IAM + VPC).
5. **Gold mart tables** are exported to **Snowflake** via Spark Snowflake connector for fast analytical serving.

---

## Data Source (AWS S3)

This project uses Parquet datasets obtained from the official
NYC Taxi & Limousine Commission (TLC) Trip Record Data portal:

https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

### Dataset Coverage

* **Years:** 2020–2024
* **Format:** Parquet
* **Source Storage:** AWS S3

The raw NYC TLC datasets are stored as **Parquet files in an AWS S3 bucket**.

Access is provided via a dedicated **IAM user** with a **read-only policy scoped to a single bucket** 
(principle of the least privilege).
The pipeline uses these credentials to read Parquet objects from S3 during the ingestion process.

### Configure S3 Credentials

1. Request (or create) **IAM access keys** for the read-only user.
2. Add the credentials to your `.env` file (based on `.env.example`).
3. Keep the credentials local and **never commit them to the repository**.
4. On AWS EC2: credentials are provided via IAM Instance Profile (no hardcoded keys required)

> This repository does **not include any credentials**. 
> Use environment variables or a proper secrets management solution for production deployments.

---

## Project Structure

The repository is organized as follows:

```
├── airflow/
│   └── dags/                    # Airflow DAG definitions
│
├── dbt/
│   ├── models/                  # Bronze / Silver / Gold models
│   ├── macros/                  # dbt macros
│   └── tests/                   # dbt tests
│
├── src/
│   └── jobs/                    # Spark bootstrap / ingestion / taxi_zones jobs
│
├── scripts/
│   └── download-drivers.sh      # JDBC drivers for Hive metastore
│
├── warehouse/                   # Local lakehouse storage
│
├── docker/                      # Dockerfiles
│
├── requirements/                # Python requirements for every docker container
│
├── .env.example                 # Environment variables template
└── docker-compose.yml           # Platform services
```

---

## Prerequisites

- AWS account (for S3 access)
- Snowflake account (for the serving layer)
- Docker + Docker Compose
- Git
- Unix-like shell (macOS/Linux). The project was built on macOS but should work on Linux as well.

---

## Quickstart (first-time setup)

```bash
# Clone the repo
git clone https://github.com/evgeni-velikov/nyc-taxi-data-pipeline.git
cd nyc-taxi-data-pipeline

# Download drivers (required for metastore connectivity)
./scripts/download-drivers.sh

# Local warehouse directory (mounted into containers)
mkdir -p warehouse
sudo chmod 777 warehouse

# Environment variables (add your S3 IAM credentials here as well)
cp .env.example .env

# Build images and start the platform
docker compose build

# Local mode (includes MySQL metastore + Airflow Postgres)
docker compose --profile local up -d

# AWS mode (uses RDS for metastore and Airflow backend — no local DB containers)
docker compose up -d

# Wait ~40-50 seconds and check containers
docker-compose ps

# Install dbt dependencies
docker compose run --rm dbt dbt deps
```

The `--profile local` flag starts the MySQL and Postgres containers used as local substitutes for RDS.
On AWS, these are provided by RDS and the profile is not needed.

The setup steps above are typically required only once (unless you wipe volumes).

---

## Start / stop

After the initial setup:

```bash
# Local mode (includes MySQL metastore + Airflow Postgres)
docker compose --profile local up -d

# AWS mode (uses RDS for metastore and Airflow backend — no local DB containers)
docker compose up -d

docker compose down
```

---

## Verify dbt Connection

After the platform is running, verify that dbt can connect to Spark.

```bash
docker compose run --rm dbt dbt debug
```

Expected output:

* profiles.yml file is valid
* dbt_project.yml file is valid
* Connection test succeeds

If successful, the transformation layer is correctly configured.

---

## Reset environment (clean state)

Use this if metadata/storage becomes inconsistent, or you want a full reset.

```bash
docker compose down -v
docker compose build
docker compose --profile local up -d
```

---

## Access UIs

### Local Platform
* **Airflow UI:** http://localhost:8081
* **Spark Master UI:** http://localhost:8080

### AWS Platform
Airflow UI: http://<EC2-PUBLIC-IP>:8081
Spark Master UI: http://<EC2-PUBLIC-IP>:8080

**Airflow default credentials**
```
username: admin 
password: admin
```

## Airflow DAGs

This project orchestrates ingestion and transformations via Airflow DAGs located in `airflow/dags/`.

Typical flow:

- **bootstrap DAG**: one-time initialization tasks (project-specific)
- **ingestion DAG**: loads raw NYC Taxi datasets into the Bronze layer
- **transformation DAG**: runs dbt freshness checks and builds Silver/Gold models
- **dim_yearly DAG**: builds/refreshes time-based dimensions (project-specific)

---

## dbt project

dbt code lives under `dbt/`.

### Layers

- **Bronze**: source-aligned views over ingested raw tables
- **Silver**: cleaned/standardized staging + intermediate models
- **Gold**: facts and marts for analytics/BI

### Common commands

Before running dbt models, make sure the **Bronze** layer is available:

1. Open **Airflow UI**: http://localhost:8081
2. Trigger **`bootstrap_dag`** (one-time initialization of Bronze tables)
3. Trigger **`ingestion_dag`** (loads new raw data into Bronze, if applicable)

Then run dbt:

```bash
docker compose run --rm dbt dbt run --select stg_fhv_trips
# or
docker compose run --rm dbt dbt build --select stg_fhv_trips
```

Successful execution confirms:

* Spark Thrift is reachable
* dbt can run transformations
* Tables are created in the target schema

---

## Snowflake Serving Layer

After dbt builds the Gold marts in Spark/Hive, the pipeline exports them to Snowflake
using the Spark Snowflake connector. This enables fast analytical queries without loading Spark.

Before running the export pipeline, make sure your Snowflake environment has the correct database and schema.
You can create them using the Snowflake web UI or SQL commands:

```sql
-- Create database if it doesn't exist
CREATE DATABASE IF NOT EXISTS TAXI_TRIPS;

-- Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS TAXI_TRIPS.NYC_TAXI;
```

Make sure `SNOWFLAKE_DATABASE` and `SNOWFLAKE_SCHEMA` in your `.env` match the database and schema you create above.

### Tables exported to Snowflake

| Snowflake Table | Source (Gold) |
|---|---|
| `TRIPS_CHARGE_HOURLY` | `gold.marts_trips_charges_hourly` |
| `TRIPS_REVENUE_HOURLY` | `gold.marts_trips_revenue_hourly` |
| `TRIPS_ZONE_ACTIVITY_HOURLY` | `gold.marts_trips_zone_activity_hourly` |

### Configure Snowflake credentials

Add the following to your `.env` file (see `.env.example`):

```env
SNOWFLAKE_ACCOUNT=<your_account_identifier>
SNOWFLAKE_USER=<your_user>
SNOWFLAKE_PASSWORD=<your_password>
SNOWFLAKE_ROLE=<your_role>
SNOWFLAKE_DATABASE=TAXI_TRIPS
SNOWFLAKE_WAREHOUSE=<your_warehouse>
SNOWFLAKE_SCHEMA=NYC_TAXI
```

The export runs automatically as part of the **transformation DAG** in Airflow,
after the Gold models are built.

---

## Incremental Check

Run dbt twice:

```bash
docker compose run --rm dbt dbt run --select stg_fhv_trips
docker compose run --rm dbt dbt run --select stg_fhv_trips
```

---

## Troubleshooting

### dbt cannot connect to Spark (Thrift)
- Ensure the platform is up: `docker compose ps`
- Re-run: `docker compose run --rm dbt dbt debug`
- If Spark Thrift is still initializing, wait ~30–60 seconds and retry.

### Metastore / schema issues
If tables exist but queries fail or state is inconsistent:

```bash
docker compose down -v
docker compose up -d --build
```

---

### Connect to Spark SQL (via Spark Thrift Server)

To open a Spark SQL interactive session inside the running container:

```bash
docker compose exec spark-thrift spark-sql
```

This command executes the `spark-sql` CLI inside the `spark-thrift` container, 
allowing you to run Spark SQL queries directly against the configured Spark environment.

After connecting, you can execute SQL queries such as:

```sql
SHOW DATABASES;
USE silver;
SHOW TABLES;
SELECT COUNT(*) AS c FROM silver.stg_fhv_trips;
```

## Design Decisions

- **Split unpivot model by metric group**: `taxi_trips_unpivot` is split into 3 separate models
  (`int_taxi_trips_charges`, `int_taxi_trips_revenue`, `int_taxi_trips_zone_activity`) to enable
  simple `partition_by=['pickup_month']` and avoid intermediate fact tables.

- **Multi-engine serving pattern**: marts are materialized as `table` in Spark/Hive for incremental
  compute, and pushed to **Snowflake as table** for BI tooling — Spark handles heavy transformations,
  Snowflake handles fast analytical queries.

## Future Improvements

Possible extensions and optimizations of this project:

### Data Quality
- extend data quality validation with additional **dbt tests** (beyond current `not_null` and `unique` checks)
- introduce **unit tests for transformation logic** to validate business rules

### Infrastructure
- migrate to AWS EMR + MWAA for production-grade deployment
- add **data alerting** (e.g. anomaly detection on metric values or row counts per partition)
