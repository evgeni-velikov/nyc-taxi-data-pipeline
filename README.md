## 🐳 Local Development — Build & Run

This project runs a complete lakehouse-style data platform locally using **Docker Compose**.

Included services:

* Apache Airflow — orchestration
* Apache Spark — ingestion & compute
* dbt — transformation (Silver & Gold)
* Hive Metastore — table metadata
* MinIO — S3-compatible object storage

---

## 🚀 First-time setup

Create a local directory for warehouse data if not exists:

```bash
mkdir warehouse
```

Build and run

```bash
docker compose build
docker compose run dbt dbt deps
docker compose up -d
```

This step is required only once.

---

## ▶️ Start the platform

After the initial setup:

```bash
docker compose up -d
```

---

## 🔄 Reset environment (clean state)

Remove containers, volumes, and rebuild everything:

```bash
docker compose down -v
docker compose build
docker compose up -d
```

Use this if metadata, Spark state, or storage becomes inconsistent.

---

## 🌐 Access UIs

* **Airflow UI:** http://localhost:8081
* **Spark Master UI:** http://localhost:8080
* **MinIO Console:** http://localhost:9001

**Airflow default credentials**

```
admin / admin
```

Spark and MinIO UIs are primarily used for debugging and data inspection.

---

## ✅ Verify dbt Connection

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

## ▶️ Run a Model (Smoke Test)

Execute a single model to validate end-to-end execution:

```bash
docker compose run --rm dbt dbt run --select <model_name>
```

Successful execution confirms:

* Spark Thrift is reachable
* dbt can run transformations
* Tables are created in the target schema

---

## 🔁 Incremental Check

Run dbt twice:

```bash
docker compose run --rm dbt dbt run
docker compose run --rm dbt dbt run
```

The second run should be faster and process only new data, confirming incremental behavior.

---

## 🧱 Platform Overview

Pipeline flow:

**Ingestion (Spark)** → Bronze (object storage)
**Transformation (dbt)** → Silver → Gold
**Orchestration (Airflow)** coordinates execution

---

## 🧠 Notes

* Airflow manages scheduling, dependencies, and retries
* Spark performs ingestion and heavy transformations
* dbt defines transformation logic and lineage
* MinIO provides S3-compatible storage for local development
* Hive Metastore persists table definitions across runs

This setup mirrors a typical production lakehouse architecture while remaining lightweight for local development.

---

## 🧪 Development Tips

* Rebuild images after Dockerfile changes
* Use `dbt debug` as the primary health check
* Monitor Spark UI for parallel jobs and resource usage
* Use incremental models to avoid full recomputation
* Reset the environment if schema or storage becomes inconsistent
