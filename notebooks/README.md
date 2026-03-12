# Local Notebook with Docker Spark Cluster

## Goal
Run Jupyter locally (from Cursor) and execute Spark code on the Docker Compose Spark cluster.

## Prerequisites
- Docker Compose stack is up (`spark-master`, `spark-worker` running).
- Java 17+ installed on host (required by local `pyspark` driver).
- AWS profile `gba-admin` exists in `./.local/aws`.

## Install local notebook dependencies
```bash
uv sync --dev
```

## Start infra
```bash
docker compose up -d spark-master spark-worker
```

## Start Jupyter
```bash
make notebook
```

Open:
- `notebooks/spark_cluster_playground.ipynb`

## Notes
- Notebook Spark master is `spark://localhost:7077`.
- Driver runs on your host (local Jupyter kernel), executors run in Docker worker.
- AWS creds are propagated for both driver and executors in `notebooks/spark_session.py`.
