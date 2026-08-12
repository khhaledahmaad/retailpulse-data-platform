# RetailPulse — Session 7 Reproduction Runbook

**Goal:** Add Airflow orchestration and make the platform restart-safe, including Kafka persistence.

## 1. Create Airflow image

File:

```text
airflow/Dockerfile
```

```dockerfile
FROM apache/airflow:3.3.0-python3.12

USER airflow

RUN pip install --no-cache-dir \
    "psycopg[binary]" \
    pyarrow \
    dbt-postgres
```

## 2. Add Airflow services

Required:

```text
airflow-db
airflow-init
airflow-api-server
airflow-scheduler
airflow-dag-processor
```

Airflow metadata database:

```text
airflow-db:5432
```

RetailPulse data database inside Docker:

```text
postgres:5432
```

Mount:

```text
./airflow/dags  → /opt/airflow/dags
./airflow/logs  → /opt/airflow/logs
./warehouse     → /opt/retailpulse/warehouse
./data_lake     → /opt/retailpulse/data_lake
```

## 3. Critical Airflow configuration

```yaml
AIRFLOW__CORE__EXECUTOR: LocalExecutor
AIRFLOW__CORE__LOAD_EXAMPLES: "false"
AIRFLOW__CORE__DAGS_FOLDER: /opt/airflow/dags
AIRFLOW__CORE__EXECUTION_API_SERVER_URL: http://airflow-api-server:8080/execution/
AIRFLOW__API_AUTH__JWT_SECRET: <one shared fixed secret>
AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS: airflow:admin
```

## 4. Build and initialise Airflow

Validate Compose:

```cmd
docker compose config
```

Build:

```cmd
docker compose build
```

Run metadata migration/init:

```cmd
docker compose up airflow-init
```

Start everything:

```cmd
docker compose up -d
docker compose ps -a
```

Airflow:

```text
http://localhost:8083
```

## 5. Create DAG

File:

```text
airflow/dags/retailpulse_warehouse_pipeline.py
```

Flow:

```text
run_incremental_loader
        ↓
validate_raw_orders
        ↓
run_dbt_build
        ↓
record_pipeline_metrics
```

Schedule:

```text
*/10 * * * *
```

Use:

```text
catchup=False
max_active_runs=1
```

Loader command:

```text
cd /opt/retailpulse && python warehouse/loader/load_orders.py
```

dbt command must explicitly use Docker target:

```text
cd /opt/retailpulse/warehouse/dbt/retailpulse &&
dbt build --target airflow --profiles-dir /opt/retailpulse/warehouse/dbt/retailpulse
```

## 6. Validate Airflow itself

List DAGs:

```cmd
docker compose exec airflow-scheduler airflow dags list
```

Executor:

```cmd
docker compose exec airflow-scheduler airflow config get-value core executor
```

Expected:

```text
LocalExecutor
```

Execution API:

```cmd
docker compose exec airflow-scheduler airflow config get-value core execution_api_server_url
```

Expected:

```text
http://airflow-api-server:8080/execution/
```

Health:

```cmd
docker compose exec airflow-scheduler curl -i http://airflow-api-server:8080/api/v2/monitor/health
```

Logs:

```cmd
docker compose logs airflow-scheduler --tail 200
```

## 7. Add persistent Kafka storage

Named volume:

```yaml
kafka_data:
```

Kafka mount:

```yaml
- kafka_data:/tmp/kraft-combined-logs
```

Kafka:

```yaml
CLUSTER_ID: 5L6g3nShT-eMCtK--X86sw
KAFKA_LOG_DIRS: /tmp/kraft-combined-logs
```

Add one-shot ownership init:

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

Kafka depends on successful `kafka-init`.

## 8. Validate Kafka persistence

```cmd
docker compose down
docker compose up -d
docker compose ps -a
```

Topic must still exist:

```cmd
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh ^
  --bootstrap-server localhost:9092 ^
  --list
```

Offsets must remain:

```cmd
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh ^
  --bootstrap-server localhost:9092 ^
  --topic orders
```

Do not use:

```cmd
docker compose down -v
```

for normal shutdown.

## 9. End-to-end regression

### A. Kafka

```cmd
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh ^
  --bootstrap-server localhost:9092 ^
  --topic orders
```

### B. Bronze/Silver/Quarantine

Stop the long-running stream if needed, then:

```cmd
docker compose exec spark-master /opt/spark/bin/pyspark ^
  --master spark://spark-master:7077
```

Inside:

```python
bronze = spark.read.parquet("/opt/retailpulse/data_lake/bronze/orders")
silver = spark.read.parquet("/opt/retailpulse/data_lake/silver/orders")
quarantine = spark.read.parquet("/opt/retailpulse/data_lake/quarantine/orders")

print("Bronze:", bronze.count())
print("Silver:", silver.count())
print("Quarantine:", quarantine.count())
```

Partition reconciliation:

```python
from pyspark.sql import functions as F

bronze.groupBy("partition").agg(
    F.min("offset").alias("min_offset"),
    F.max("offset").alias("max_offset"),
    F.count("*").alias("rows"),
).orderBy("partition").show()
```

### C. Warehouse

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "SELECT COUNT(*) AS raw_orders FROM raw.orders;"
```

### D. dbt event fact

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "SELECT COUNT(*) AS fact_orders FROM analytics.fct_orders;"
```

### E. Gold mart

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "SELECT SUM(order_count) AS gold_order_count FROM analytics.mart_daily_sales;"
```

Inspect mart:

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "SELECT * FROM analytics.mart_daily_sales ORDER BY order_date;"
```

### F. Airflow

Trigger from UI or allow schedule to run.

All tasks should succeed:

```text
run_incremental_loader
validate_raw_orders
run_dbt_build
record_pipeline_metrics
```

Run it again with no new Silver data.

Expected:

```text
loader remains idempotent
fact count does not duplicate
Gold totals do not duplicate
```

Then produce new events, allow Spark to write them, run Airflow again.

Expected:

```text
new Kafka data
→ new Silver files
→ only new files loaded
→ incremental fact grows
→ Gold mart refreshes
```

## 10. Final UI map

```text
Kafka UI  → http://localhost:8080
Spark UI  → http://localhost:8081
dbt Docs  → http://localhost:8082
Airflow    → http://localhost:8083
```

## Session 7 validation gate

```text
[ ] Airflow services healthy
[ ] DAG discovered
[ ] LocalExecutor confirmed
[ ] Execution API URL confirmed
[ ] full DAG succeeds
[ ] second DAG run remains idempotent
[ ] Kafka topic survives down/up
[ ] Kafka offsets survive down/up
[ ] Bronze/Silver counts available
[ ] warehouse raw count available
[ ] dbt fact count available
[ ] Gold SUM(order_count) reconciles
[ ] end-to-end current lineage validated
```
