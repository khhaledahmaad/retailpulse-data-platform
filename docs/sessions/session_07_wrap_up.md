# RetailPulse — Session 7 Wrap-Up

**Date:** 11 August 2026  
**Session:** 7  
**Focus:** Airflow orchestration, successful end-to-end downstream pipeline, and Docker operational understanding

## Session Goal

Add Apache Airflow to orchestrate the downstream RetailPulse workflow:

```text
Silver Parquet
    ↓
Incremental PostgreSQL loader
    ↓
Warehouse validation
    ↓
dbt build
    ↓
Pipeline metrics
```

The aim was to stop running the warehouse loader and dbt manually and instead manage them as an observable, retryable Airflow DAG.

---

## Final Working Architecture

The successful downstream architecture is:

```text
Kafka
  ↓
Spark Structured Streaming
  ↓
Silver Parquet
  ↓
Airflow DAG
  ├── run_incremental_loader
  ├── validate_raw_orders
  ├── run_dbt_build
  └── record_pipeline_metrics
```

Airflow does not manage the continuous Spark stream.

Instead:

```text
Kafka → Spark
```

runs continuously, while Airflow handles the bounded downstream workflow:

```text
Silver → PostgreSQL → dbt
```

---

# 1. Airflow Docker Services

The working Airflow deployment uses separate Docker services for:

```text
airflow-db
airflow-api-server
airflow-scheduler
airflow-dag-processor
airflow-init
```

Airflow uses its own PostgreSQL metadata database:

```text
airflow-db
```

while RetailPulse application data remains in:

```text
postgres
```

These databases have different responsibilities:

```text
airflow-db
→ DAG runs
→ task states
→ scheduler metadata
→ Airflow internal state

postgres
→ raw.orders
→ loader control tables
→ analytics schema
→ dbt models
```

---

# 2. Airflow Custom Docker Image

The Airflow image was extended so Airflow tasks have the same dependencies required by the RetailPulse downstream pipeline.

`airflow/Dockerfile`:

```dockerfile
FROM apache/airflow:3.3.0-python3.12

USER airflow

RUN pip install --no-cache-dir \
    "psycopg[binary]" \
    pyarrow \
    dbt-postgres
```

This gives Airflow access to:

```text
psycopg
→ PostgreSQL communication

pyarrow
→ Parquet support for the warehouse loader

dbt-postgres
→ dbt transformations against PostgreSQL
```

---

# 3. Important Airflow 3 Configuration

The working Airflow configuration includes a shared Execution API URL:

```yaml
AIRFLOW__CORE__EXECUTION_API_SERVER_URL: http://airflow-api-server:8080/execution/
```

This allows tasks launched through LocalExecutor to communicate correctly with the Airflow API server.

The scheduler therefore communicates with:

```text
airflow-api-server:8080
```

inside the Docker network rather than using host-local addressing.

A shared JWT secret is also configured across the Airflow components:

```yaml
AIRFLOW__API_AUTH__JWT_SECRET: <shared-secret>
```

The same secret is available to both:

```text
airflow-scheduler
airflow-api-server
```

so internal task-authentication tokens can be generated and validated consistently.

---

# 4. Executor

The final setup uses:

```text
LocalExecutor
```

Configured with:

```yaml
AIRFLOW__CORE__EXECUTOR: LocalExecutor
```

This is suitable for the current single-machine RetailPulse portfolio environment.

Verification command:

```cmd
docker compose exec airflow-scheduler airflow config get-value core executor
```

Expected:

```text
LocalExecutor
```

---

# 5. Airflow API Connectivity

The final configuration was verified from inside the scheduler container.

Execution API URL:

```cmd
docker compose exec airflow-scheduler airflow config get-value core execution_api_server_url
```

Expected:

```text
http://airflow-api-server:8080/execution/
```

Network/API health was verified with:

```cmd
docker compose exec airflow-scheduler curl -i http://airflow-api-server:8080/api/v2/monitor/health
```

The Airflow health endpoint returned successfully.

---

# 6. DAG

The RetailPulse orchestration DAG is:

```text
retailpulse_warehouse_pipeline
```

Main flow:

```text
run_incremental_loader
        ↓
validate_raw_orders
        ↓
run_dbt_build
        ↓
record_pipeline_metrics
```

The DAG is mounted into Airflow from:

```text
airflow/dags/
```

---

# 7. Incremental Loader Task

The first task executes the existing warehouse loader:

```text
warehouse/loader/load_orders.py
```

Conceptually:

```text
Silver Parquet
      ↓
watermark-based partition discovery
      ↓
new files only
      ↓
raw.orders
```

The loader remains idempotent through:

```text
control.loader_watermarks
control.loaded_files
raw.orders.event_id primary key
```

So Airflow can safely run it repeatedly.

---

# 8. Warehouse Validation Task

After the loader succeeds, Airflow validates the warehouse.

The validation checks:

```text
raw.orders row count
latest loaded_at timestamp
```

and fails the pipeline if the warehouse contains no rows.

This creates an explicit quality gate between ingestion and analytics:

```text
loader
  ↓
validation
  ↓
dbt
```

---

# 9. dbt Task

The next task runs:

```text
dbt build
```

against the RetailPulse dbt project.

The dbt pipeline is:

```text
raw.orders
    ↓
stg_orders
    ↓
fct_orders
    ↓
mart_daily_sales
```

Current materialisation strategy:

```text
stg_orders
→ view

fct_orders
→ incremental

mart_daily_sales
→ table
```

So each Airflow run does not rebuild the large event-level fact table from scratch.

---

# 10. dbt Docker Profile

dbt inside Airflow must connect to PostgreSQL using the Docker service name:

```text
postgres
```

rather than:

```text
localhost
```

The Airflow-specific dbt profile therefore uses:

```yaml
host: postgres
port: 5432
dbname: retailpulse
schema: analytics
```

This profile is mounted into the Airflow container and used during the DAG's dbt task.

---

# 11. Metrics Task

The final task records/logs pipeline-level information such as:

```text
raw.orders row count
latest warehouse load timestamp
pipeline completion
```

This provides a simple operational summary after successful execution.

---

# 12. Successful End-to-End Run

The final DAG run completed successfully:

```text
run_incremental_loader   ✅
validate_raw_orders      ✅
run_dbt_build            ✅
record_pipeline_metrics  ✅
```

This proves that the following components now work together:

```text
Airflow scheduler
Airflow LocalExecutor
Airflow API server
RetailPulse PostgreSQL
Silver Parquet
Python warehouse loader
dbt
Airflow task dependencies
Airflow retries/logging
```

---

# 13. UI Port Allocation

The local development interfaces are now separated cleanly:

```text
Kafka UI
→ http://localhost:8080

Spark UI
→ http://localhost:8081

dbt Docs
→ http://localhost:8082

Airflow
→ http://localhost:8083
```

---

# 14. Useful Airflow Commands

List DAGs:

```cmd
docker compose exec airflow-scheduler airflow dags list
```

This is a DAG-discovery/parsing check.

It confirms that Airflow can see:

```text
retailpulse_warehouse_pipeline
```

It is not specifically tied to dbt configuration.

The dbt profile is required when the DAG reaches:

```text
run_dbt_build
```

---

# 15. Docker Concepts Consolidated During the Session

## Docker Image

An image is the blueprint used to create containers.

Example:

```text
apache/airflow image
    ↓
custom Dockerfile
    ↓
RetailPulse Airflow image
```

---

## Docker Container

A container is a running instance of an image.

Example:

```text
Airflow image
    ↓
airflow-scheduler container
```

---

## Docker Volume

A volume is persistent storage that lives separately from containers.

Example:

```yaml
postgres_data:/var/lib/postgresql/data
```

This allows database data to survive container recreation.

Conceptually:

```text
container
→ disposable runtime

volume
→ persistent data
```

---

## docker build

Builds an image from a Dockerfile.

Example:

```cmd
docker build -t my-image .
```

Conceptually:

```text
Dockerfile
   ↓
docker build
   ↓
Docker image
```

---

## docker compose build

Builds images for services in `docker-compose.yml` that use:

```yaml
build:
```

It does not rebuild services that simply reference existing images unless those services also define build instructions.

---

## docker compose up

Creates/recreates and starts the Compose services.

Typical command:

```cmd
docker compose up -d
```

`-d` runs the containers in the background.

---

## docker compose down

Stops and removes the Compose containers and network:

```cmd
docker compose down
```

Named volumes normally remain.

Therefore persistent PostgreSQL data survives a normal `down`.

---

## docker compose down -v

This additionally removes named volumes.

Example:

```cmd
docker compose down -v
```

This should be treated carefully because it can remove persistent database state.

---

## Docker vs Docker Compose

Docker manages containers/images directly.

Examples:

```cmd
docker build
docker run
docker ps
docker images
docker volume ls
```

Docker Compose manages a multi-container application defined in:

```text
docker-compose.yml
```

Examples:

```cmd
docker compose build
docker compose up
docker compose down
docker compose ps
```

For RetailPulse:

```text
Docker
→ actually runs PostgreSQL, Kafka, Spark and Airflow containers

Docker Compose
→ defines how the entire stack works together
```

---

# 16. Final Mental Model

```text
Dockerfile
    ↓
docker build / docker compose build
    ↓
IMAGE
    ↓
docker compose up
    ↓
CONTAINER
    ↓
VOLUME
    ↓
persistent data
```

For RetailPulse specifically:

```text
docker-compose.yml
      ↓
Docker Compose
      ↓
PostgreSQL + Kafka + Spark + Airflow
      ↓
coordinated local data platform
```

---

# 17. Git Commit

Recommended Session 7 commit:

```cmd
git add .
git commit -m "Add Airflow orchestration for warehouse and dbt pipeline"
git push origin main
```

---

# Session 7 Completion Checklist

- [x] Custom Airflow image created
- [x] Airflow metadata PostgreSQL configured
- [x] LocalExecutor configured
- [x] Airflow API server configured
- [x] Airflow scheduler configured
- [x] DAG processor configured
- [x] Shared Execution API URL configured
- [x] Shared Airflow JWT secret configured
- [x] Airflow health endpoint reachable
- [x] DAG visible in Airflow
- [x] Incremental loader runs through Airflow
- [x] Warehouse validation succeeds
- [x] dbt build succeeds from Airflow
- [x] Pipeline metrics task succeeds
- [x] Full DAG completes successfully
- [x] Airflow UI available on port 8083
- [x] Docker image/container/volume concepts clarified
- [x] Docker vs Docker Compose clarified

---

## Current RetailPulse Platform

At the end of Session 7:

```text
Python Producer
      ↓
Kafka
      ↓
Spark Structured Streaming
      ↓
Bronze / Silver / Quarantine
      ↓
Incremental PostgreSQL Loader
      ↓
raw.orders
      ↓
dbt
      ↓
staging / fact / mart
      ↓
Airflow orchestration
```

RetailPulse now has a complete local event-driven data-engineering pipeline with streaming ingestion, lakehouse layers, incremental warehouse loading, analytics engineering, testing, and workflow orchestration.

**Session 7 status: Complete**
