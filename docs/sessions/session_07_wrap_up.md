# RetailPulse — Session 7 Wrap-Up

**Date:** 11 August 2026  
**Session:** 7  
**Focus:** Airflow orchestration, restart-safe Docker infrastructure, Kafka persistence, and end-to-end downstream pipeline execution

## Session Goal

Add Apache Airflow to orchestrate the downstream RetailPulse workflow and make the local platform restart-safe.

Target flow:

```text
Kafka
  ↓
Spark Structured Streaming
  ↓
Silver Parquet
  ↓
Airflow
  ↓
Incremental PostgreSQL loader
  ↓
Warehouse validation
  ↓
dbt build
  ↓
Pipeline metrics
```

The objective was to stop manually chaining the downstream steps and instead manage them through one observable, retryable Airflow DAG.

---

## Final Working Architecture

```text
Python Producer
      ↓
Kafka
      ↓
Spark Structured Streaming
      ↓
Bronze / Silver / Quarantine
      ↓
Airflow DAG
      ├── run_incremental_loader
      ├── validate_raw_orders
      ├── run_dbt_build
      └── record_pipeline_metrics
```

Airflow does not manage the long-running Spark stream.

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

The working deployment uses:

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

RetailPulse application data remains in:

```text
postgres
```

Responsibilities:

```text
airflow-db
→ DAG runs
→ task states
→ scheduler metadata
→ Airflow internal state

postgres
→ raw.orders
→ control tables
→ analytics schema
→ dbt models
```

---

# 2. Airflow Custom Image

`airflow/Dockerfile`:

```dockerfile
FROM apache/airflow:3.3.0-python3.12

USER airflow

RUN pip install --no-cache-dir \
    "psycopg[binary]" \
    pyarrow \
    dbt-postgres
```

Dependencies:

```text
psycopg
→ PostgreSQL communication

pyarrow
→ Parquet support for the warehouse loader

dbt-postgres
→ dbt transformations against PostgreSQL
```

---

# 3. Airflow Execution Configuration

The working Airflow configuration includes:

```yaml
AIRFLOW__CORE__EXECUTOR: LocalExecutor
```

and:

```yaml
AIRFLOW__CORE__EXECUTION_API_SERVER_URL: http://airflow-api-server:8080/execution/
```

This allows LocalExecutor task subprocesses to communicate with the Airflow API server over the internal Docker network.

A shared JWT secret is also configured:

```yaml
AIRFLOW__API_AUTH__JWT_SECRET: <shared-secret>
```

The scheduler and API server use the same value so internal task authentication succeeds.

---

# 4. Airflow DAG

The orchestration DAG is:

```text
retailpulse_warehouse_pipeline
```

Task flow:

```text
run_incremental_loader
        ↓
validate_raw_orders
        ↓
run_dbt_build
        ↓
record_pipeline_metrics
```

---

# 5. Incremental Loader Task

The first task executes:

```text
warehouse/loader/load_orders.py
```

Flow:

```text
Silver Parquet
      ↓
watermark-based partition discovery
      ↓
new files only
      ↓
raw.orders
```

Idempotency is maintained through:

```text
control.loader_watermarks
control.loaded_files
raw.orders.event_id primary key
```

---

# 6. Warehouse Validation

The validation task checks:

```text
raw.orders row count
latest loaded_at timestamp
```

and prevents dbt from running if the warehouse is empty.

This provides an explicit quality gate:

```text
loader
  ↓
validation
  ↓
dbt
```

---

# 7. dbt Task

Airflow runs:

```text
dbt build
```

against:

```text
warehouse/dbt/retailpulse
```

Current dbt flow:

```text
raw.orders
    ↓
stg_orders
    ↓
fct_orders
    ↓
mart_daily_sales
```

Materialisation strategy:

```text
stg_orders
→ view

fct_orders
→ incremental

mart_daily_sales
→ table
```

---

# 8. dbt Docker Profile

Inside Docker, dbt connects to PostgreSQL using:

```text
host: postgres
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

---

# 9. Successful Airflow Run

The final DAG completed successfully:

```text
run_incremental_loader   ✅
validate_raw_orders      ✅
run_dbt_build            ✅
record_pipeline_metrics  ✅
```

This proves successful integration across:

```text
Airflow scheduler
LocalExecutor
Airflow API server
PostgreSQL
Silver Parquet
warehouse loader
dbt
Airflow retries
Airflow logs
task dependencies
```

---

# 10. Kafka Persistence

Kafka originally ran without persistent broker storage.

That meant:

```text
docker compose down
docker compose up -d
```

could recreate Kafka with fresh topic/offset state while Spark checkpoints remained on disk.

This could create a mismatch:

```text
Spark checkpoint
→ remembers old Kafka offsets

fresh Kafka container
→ old offsets no longer exist
```

The permanent fix was to persist Kafka's KRaft log directory.

Kafka now uses:

```yaml
volumes:
  - kafka_data:/tmp/kraft-combined-logs
```

and:

```yaml
KAFKA_LOG_DIRS: /tmp/kraft-combined-logs
```

The named volume is declared as:

```yaml
volumes:
  postgres_data:
  airflow_db_data:
  kafka_data:
```

Kafka broker state, topics, partition logs, and offsets now survive ordinary container recreation.

---

# 11. Kafka Volume Ownership Fix

The Apache Kafka container runs as:

```text
uid=1000(appuser)
gid=1000(appuser)
```

The newly created Docker volume initially caused a write-permission problem.

The permanent fix was to add a one-time `kafka-init` service:

```yaml
kafka-init:
  image: alpine:3.20
  user: "0:0"

  volumes:
    - kafka_data:/tmp/kraft-combined-logs

  command:
    - /bin/sh
    - -c
    - |
      mkdir -p /tmp/kraft-combined-logs
      chown -R 1000:1000 /tmp/kraft-combined-logs

  restart: "no"
```

Kafka waits for it:

```yaml
depends_on:
  kafka-init:
    condition: service_completed_successfully
```

Final startup sequence:

```text
kafka_data volume
      ↓
kafka-init
      ↓
chown 1000:1000
      ↓
kafka-init exits 0
      ↓
Kafka starts
```

---

# 12. Fixed KRaft Cluster ID

Kafka now uses a fixed cluster ID:

```yaml
CLUSTER_ID: 5L6g3nShT-eMCtK--X86sw
```

This keeps the KRaft metadata identity stable across restarts.

---

# 13. One-Time Topic Recreation

Because the persistent Kafka volume was new, the `orders` topic had to be recreated once.

Command:

```cmd
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh ^
  --bootstrap-server localhost:9092 ^
  --create ^
  --topic orders ^
  --partitions 3 ^
  --replication-factor 1
```

Verification:

```cmd
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh ^
  --bootstrap-server localhost:9092 ^
  --list
```

Expected:

```text
orders
```

After this migration, the topic should survive normal Compose restarts.

---

# 14. Restart-Safe State

The important persistent components are now:

```text
PostgreSQL
→ postgres_data

Airflow metadata
→ airflow_db_data

Kafka broker state
→ kafka_data

Spark stream checkpoints
→ host-mounted data_lake/checkpoints
```

This means:

```cmd
docker compose down
docker compose up -d
```

can recreate containers while preserving the important platform state.

---

# 15. Important `down` Behavior

Safe for normal development:

```cmd
docker compose down
```

This stops/removes containers and the Compose network but preserves named volumes.

Then:

```cmd
docker compose up -d
```

recreates and starts the stack using the existing persistent data.

Use caution with:

```cmd
docker compose down -v
```

because `-v` removes named volumes, including:

```text
postgres_data
airflow_db_data
kafka_data
```

That would intentionally remove persistent state.

---

# 16. UI Ports

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

# 17. Useful Airflow Commands

List DAGs:

```cmd
docker compose exec airflow-scheduler airflow dags list
```

Check executor:

```cmd
docker compose exec airflow-scheduler airflow config get-value core executor
```

Expected:

```text
LocalExecutor
```

Check Execution API URL:

```cmd
docker compose exec airflow-scheduler airflow config get-value core execution_api_server_url
```

Expected:

```text
http://airflow-api-server:8080/execution/
```

---

# 18. Useful Kafka Commands

List topics:

```cmd
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh ^
  --bootstrap-server localhost:9092 ^
  --list
```

Check Kafka container state:

```cmd
docker compose ps -a
```

Inspect Kafka logs:

```cmd
docker compose logs kafka --tail 100
```

---

# 19. Docker Concepts Consolidated

## Image

Blueprint used to create containers.

```text
Dockerfile
   ↓
docker build
   ↓
image
```

## Container

Running instance of an image.

## Volume

Persistent storage that survives container recreation.

## `docker build`

Builds one image from a Dockerfile.

## `docker compose build`

Builds services in `docker-compose.yml` that define `build:`.

## `docker compose up`

Creates/recreates and starts Compose services.

## `docker compose down`

Stops and removes Compose containers and network.

Named volumes normally survive.

## Docker vs Docker Compose

```text
Docker
→ manages images/containers directly

Docker Compose
→ coordinates the multi-container RetailPulse stack
```

---

# 20. Final Platform State

```text
Python Producer
      ↓
Kafka
      ↓
persistent kafka_data volume
      ↓
Spark Structured Streaming
      ↓
Bronze / Silver / Quarantine
      ↓
persistent Spark checkpoints
      ↓
Airflow
      ↓
incremental PostgreSQL loader
      ↓
raw.orders
      ↓
dbt staging / fact / mart
      ↓
pipeline metrics
```

The platform now supports:

```text
streaming ingestion
lakehouse layering
checkpoint recovery
persistent Kafka state
incremental warehouse loading
dbt transformations
data-quality tests
Airflow orchestration
retries and logs
restart-safe Docker infrastructure
```

---

# Session 7 Completion Checklist

- [x] Custom Airflow image
- [x] Airflow metadata database
- [x] LocalExecutor
- [x] Airflow API server
- [x] DAG processor
- [x] shared Execution API URL
- [x] shared Airflow JWT secret
- [x] Airflow DAG visible
- [x] incremental loader task succeeds
- [x] warehouse validation succeeds
- [x] dbt build succeeds
- [x] pipeline metrics succeeds
- [x] full Airflow DAG succeeds
- [x] Kafka persistent named volume
- [x] Kafka KRaft log directory persisted
- [x] Kafka volume ownership handled by `kafka-init`
- [x] fixed KRaft cluster ID
- [x] `orders` topic recreated once
- [x] Kafka state survives ordinary Compose recreation
- [x] Spark checkpoints remain persistent
- [x] PostgreSQL state remains persistent
- [x] Airflow metadata remains persistent
- [x] Docker/Compose concepts clarified

**Session 7 status: Complete**

---

# 18. Final Regression Validation — 12 August 2026

After the Airflow infrastructure fixes, the downstream pipeline was exercised through repeated regression runs.

Two subsequent Airflow DAG runs completed without task failure:

```text
run_incremental_loader   ✅
validate_raw_orders      ✅
run_dbt_build            ✅
record_pipeline_metrics  ✅
```

The test specifically validated repeated incremental execution rather than only a first successful DAG run.

Expected behaviour was confirmed:

```text
new Kafka events
      ↓
Spark writes new Silver files
      ↓
Airflow loader scans watermark/current partitions
      ↓
already-loaded files skipped
      ↓
only new files loaded
      ↓
dbt incremental fact processes new warehouse rows
```

This demonstrates that the DAG can be safely triggered repeatedly without reloading historical Silver files.

---

# 19. Restart / Persistence Regression

The platform's normal restart model is:

```cmd
docker compose down
docker compose up -d
```

The final architecture preserves:

```text
Kafka broker data       → kafka_data
PostgreSQL data         → postgres_data
Airflow metadata        → airflow_db_data
Spark lake files        → host-mounted data_lake/
Spark checkpoints       → host-mounted data_lake/checkpoints/
```

`docker compose down -v` remains destructive to named volumes and is not part of normal operation.

The Kafka persistence fix and existing Spark checkpoints are now aligned so ordinary Compose recreation does not create the previous broker/checkpoint offset mismatch.

---

# 20. Final End-to-End Reconciliation

A final count discrepancy investigation compared the Kafka UI, Kafka broker offsets, Bronze, Silver, Quarantine, and downstream analytics.

Kafka UI displayed:

```text
117 messages consumed
```

but this was not treated as the authoritative number of current topic records.

The Kafka broker reported:

```text
orders:0:43
orders:1:31
orders:2:34
```

Therefore:

```text
43 + 31 + 34 = 108 current Kafka records
```

Spark Bronze:

```text
partition 0 → offsets 0–42 → 43 rows
partition 1 → offsets 0–30 → 31 rows
partition 2 → offsets 0–33 → 34 rows
```

Spark Silver had the same partition/offset ranges.

Final reconciliation:

```text
Kafka broker records = 108
Bronze               = 108
Silver               = 108
Quarantine           =   0
Warehouse / dbt      = 108
```

This proves that the current clean lineage is fully reconciled and contains no missing Kafka offsets between the broker and Bronze.

The Kafka UI's message-browser/export was also paginated, so its `messages consumed` display should not be used as the primary end-to-end row-count metric.

Authoritative Kafka check:

```cmd
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh ^
  --bootstrap-server localhost:9092 ^
  --topic orders
```

Spark reconciliation check:

```python
from pyspark.sql import functions as F

bronze.groupBy("partition").agg(
    F.min("offset").alias("min_offset"),
    F.max("offset").alias("max_offset"),
    F.count("*").alias("rows"),
).orderBy("partition").show()
```

---

# 21. Final Session 7 Validation Checklist

- [x] Airflow DAG succeeds end to end
- [x] repeated Airflow regression runs succeed
- [x] incremental loader remains idempotent
- [x] dbt incremental build succeeds from Airflow
- [x] Kafka broker state is persistent
- [x] Spark checkpoints are persistent
- [x] ordinary Compose restart model is understood
- [x] Kafka broker offsets reconciled by partition
- [x] Bronze offsets continuous from zero
- [x] Silver exactly matches Bronze
- [x] Quarantine contains zero current test records
- [x] warehouse/dbt row count matches current 108-row lineage
- [x] Kafka UI count discrepancy explained
- [x] end-to-end current lineage reconciled successfully

**Validated platform checkpoint before Session 8: 108 current-lineage order events end to end.**
