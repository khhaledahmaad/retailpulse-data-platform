# Build and Start RetailPulse

## 1. Goal

This document reproduces a runnable RetailPulse environment from the repository structure. Commands are written for Windows CMD from the repository root unless stated otherwise.

A clean clone provides source/configuration, not runtime data. Kafka/PostgreSQL Docker volumes and `data_lake/` contents are runtime state and are not stored in Git.

## 2. Relevant repository structure

```text
retailpulse-data-platform/
├── .env.example
├── docker-compose.yml
├── requirements-dev.txt
├── airflow/
│   ├── Dockerfile
│   └── dags/retailpulse_warehouse_pipeline.py
├── producer/
│   └── src/producer.py
├── spark/
│   ├── common/
│   ├── jobs/stream_orders_to_lake.py
│   └── tools/check_order_quality_parity.py
├── warehouse/
│   ├── init/001_create_warehouse.sql
│   ├── loader/load_orders.py
│   ├── monitoring/
│   ├── tools/
│   └── dbt/retailpulse/
├── data_lake/
│   ├── bronze/
│   ├── silver/
│   ├── quarantine/
│   └── checkpoints/
└── docs/
```

## 3. Prerequisites

- Windows with Docker Desktop using Linux containers.
- Git.
- Python 3.10-compatible local environment for the repository tooling/tests.
- Network access to pull the configured Docker images and Spark Kafka package when not already cached.

## 4. Create the local Python environment

From the repository root:

```cmd
python -m venv .venv
.venv\Scripts\activate
python -m pip install --upgrade pip
pip install -r requirements-dev.txt
```

Verify:

```cmd
python --version
ruff --version
pytest --version
```

## 5. Create `.env`

Copy the committed placeholder contract:

```cmd
copy .env.example .env
```

Fill the required values locally. Do **not** commit `.env`.

Current settings represented by `.env.example`:

```text
MAX_LAG_ROWS
MAX_LOAD_AGE_MINUTES

MAILTRAP_HOST
MAILTRAP_PORT
MAILTRAP_USERNAME
MAILTRAP_PASSWORD
ALERT_EMAIL_FROM
ALERT_EMAIL_TO

POSTGRES_DB
POSTGRES_USER
POSTGRES_PASSWORD

AIRFLOW_DB_NAME
AIRFLOW_DB_USER
AIRFLOW_DB_PASSWORD
AIRFLOW_JWT_SECRET
```

Core database/Airflow values are required by Compose. Alerting values are needed if incident/recovery email delivery is to be exercised.

## 6. Create the ignored dbt profile

`warehouse/dbt/retailpulse/profiles.yml` is intentionally git-ignored because credentials are environment-driven. Create it locally with:

```yaml
retailpulse:
  target: dev

  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      user: "{{ env_var('POSTGRES_USER') }}"
      password: "{{ env_var('POSTGRES_PASSWORD') }}"
      dbname: "{{ env_var('POSTGRES_DB') }}"
      schema: analytics
      threads: 4

    airflow:
      type: postgres
      host: postgres
      port: 5432
      user: "{{ env_var('POSTGRES_USER') }}"
      password: "{{ env_var('POSTGRES_PASSWORD') }}"
      dbname: "{{ env_var('POSTGRES_DB') }}"
      schema: analytics
      threads: 4
```

The file path must be:

```text
warehouse/dbt/retailpulse/profiles.yml
```

Local dbt does not automatically read project `.env`, so invoke it through dotenv:

```cmd
python -m dotenv run -- dbt debug ^
  --project-dir warehouse\dbt\retailpulse ^
  --profiles-dir warehouse\dbt\retailpulse ^
  --target dev
```

## 7. Validate Compose without exposing secrets

```cmd
docker compose config --quiet
```

Do not routinely run plain `docker compose config` because it renders interpolated environment values to stdout.

## 8. Start infrastructure

```cmd
docker compose up -d --build
```

Inspect:

```cmd
docker compose ps
```

Expected critical states include:

```text
retailpulse-postgres              healthy
retailpulse-airflow-db            healthy
retailpulse-kafka                 healthy
retailpulse-spark-master          healthy
retailpulse-airflow-api-server    healthy
retailpulse-spark-worker          running
retailpulse-airflow-scheduler     running
retailpulse-airflow-dag-processor running
retailpulse-kafka-ui              running
```

### Cold-start lifecycle note

The final production-readiness setup uses explicit Compose-controlled startup for dependent Airflow services rather than relying on independent container auto-restart. This was validated after a full Windows/Docker reboot: the first `docker compose up -d` completed successfully.

## 9. Bootstrap the warehouse on a fresh PostgreSQL volume

The SQL is not mounted into `docker-entrypoint-initdb.d`, so a fresh database requires explicit bootstrap:

```cmd
docker compose exec -T postgres sh -lc "psql -v ON_ERROR_STOP=1 -U $POSTGRES_USER -d $POSTGRES_DB" < warehouse\init\001_create_warehouse.sql
```

Implementation:

```text
warehouse/init/001_create_warehouse.sql
```

It creates:

```text
raw schema
control schema
raw.orders
control.loaded_files
control.loader_watermarks
control.pipeline_metrics
control.event_reprocessing_log
control.pipeline_runs
control.pipeline_incidents
supporting indexes
```

The script is designed to be safe to rerun against existing control objects.

## 10. Confirm Airflow DAG discovery

```cmd
docker compose exec airflow-scheduler airflow dags list
```

Expected DAG:

```text
retailpulse_warehouse_pipeline
```

DAG implementation:

```text
airflow/dags/retailpulse_warehouse_pipeline.py
```

## 11. Start the Spark lake application

Starting Spark master/worker containers does **not** automatically start the RetailPulse streaming application. After every Docker/PC restart, verify it and start it if absent.

Canonical job:

```text
spark/jobs/stream_orders_to_lake.py
```

Detached start:

```cmd
docker compose exec -d -e PYTHONPATH=/opt/retailpulse spark-master /opt/spark/bin/spark-submit ^
  --master spark://spark-master:7077 ^
  --conf spark.jars.ivy=/tmp/.ivy2 ^
  --conf spark.executorEnv.PYTHONPATH=/opt/retailpulse ^
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3 ^
  /opt/retailpulse/spark/jobs/stream_orders_to_lake.py
```

Verify:

```cmd
docker compose exec spark-master sh -lc "ps -ef | grep stream_orders_to_lake.py | grep -v grep"
```

If it prints the SparkSubmit and Python job processes, the stream is running.

## 12. Produce events

Reference source adapter:

```text
producer/src/producer.py
```

Small finite smoke batch:

```cmd
python -m producer.src.producer --count 20 --interval 0 --quiet
```

Continuous default mode:

```cmd
python -m producer.src.producer
```

Stop continuous mode with `Ctrl+C`.

## 13. Let Airflow process the warehouse

The DAG schedule is every 10 minutes. It will:

```text
loader
→ Raw validation
→ dbt build
→ health
→ run completion/failure lineage
```

For a manual verification run, use the Airflow UI at:

```text
http://localhost:8083
```

## 14. Validate end-to-end health

```cmd
python -m warehouse.monitoring.check_pipeline_health --strict
```

A stable system should finish:

```text
Status: HEALTHY
```

## 15. Start the operations dashboard

Implementation:

```text
warehouse/monitoring/operations_dashboard.py
```

Start:

```cmd
python -m warehouse.monitoring.operations_dashboard
```

Open:

```text
http://127.0.0.1:8084
```

## 16. Local service URLs

```text
Kafka UI        http://localhost:8080
Spark Master    http://localhost:8081
Airflow         http://localhost:8083
Operations      http://127.0.0.1:8084
```

## 17. Initial quality gate

```cmd
ruff check .
pytest -v
python -m dotenv run -- dbt build ^
  --project-dir warehouse\dbt\retailpulse ^
  --profiles-dir warehouse\dbt\retailpulse ^
  --target dev

docker compose config --quiet
python -m warehouse.monitoring.check_pipeline_health --strict
```

## 18. Safe shutdown

Stop containers but preserve named volumes:

```cmd
docker compose down
```

Do **not** use `docker compose down -v` unless intentionally destroying Kafka/PostgreSQL/Airflow persisted volumes.

The Spark application will need to be started again after the next infrastructure startup.
