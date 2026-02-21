## 🐳 Local Development — Build & Run

This project uses Docker Compose to run the full data platform locally
(Airflow, Spark, Hive Metastore, dbt, and MinIO).

---

### 🚀 First-time setup

Build images and initialize the Airflow metadata database.

```bash
docker compose build

docker compose run --rm airflow airflow db init

docker compose run --rm airflow airflow users create \
  --username admin \
  --password admin \
  --firstname admin \
  --lastname admin \
  --role Admin \
  --email admin@example.com

docker compose up -d
```

This step is required only once.

---

### ▶️ Start the platform

After the initial setup:

```bash
docker compose up -d
```

---

### 🔄 Reset environment (clean state)

Remove containers and volumes, then rebuild:

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

---

## 🧠 Notes

* Airflow orchestrates the pipeline
* Spark performs ingestion and heavy transformations
* dbt handles Silver and Gold modeling
* MinIO simulates S3 for local development
* Hive Metastore persists table metadata

Spark and MinIO UIs are mainly used for debugging and data inspection.
