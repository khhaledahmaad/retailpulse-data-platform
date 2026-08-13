# RetailPulse — Session 09 Reproduction Runbook

**Session:** 09  
**Focus:** Observability and data-quality monitoring  
**Goal:** Add a production-style pipeline health check that reconciles the logical Spark lake with Raw, Fact, and Gold layers, checks freshness, records audit snapshots, and runs automatically in Airflow.

## 1. Target Health Model

Session 09 monitors only meaningful logical pipeline metrics:

```text
Bronze
Silver
Quarantine
Raw
Fact
Gold
Latest load
Status
Details
```

Core reconciliation rules:

```text
Bronze = Silver + Quarantine
Silver = Raw
Raw = Fact
Fact = Gold
latest load must be fresh
```

Current validated healthy lineage:

```text
Bronze       127
Silver       127
Quarantine     0
Raw          127
Fact         127
Gold         127

Status: HEALTHY
```

## 2. Add the Pipeline Metrics Audit Table

Update:

```text
warehouse/init/001_create_warehouse.sql
```

Add:

```sql
CREATE TABLE IF NOT EXISTS control.pipeline_metrics (
    metric_id BIGSERIAL PRIMARY KEY,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    bronze_rows BIGINT,
    silver_rows BIGINT,
    quarantine_rows BIGINT,

    raw_orders BIGINT,
    fact_orders BIGINT,
    gold_order_count BIGINT,

    latest_loaded_at TIMESTAMPTZ,

    status TEXT NOT NULL,
    details TEXT
);
```

Apply:

```cmd
docker compose exec -T postgres psql -U retailpulse -d retailpulse < warehouse\init\001_create_warehouse.sql
```

Verify:

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "\d control.pipeline_metrics"
```

Check current rows:

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "SELECT * FROM control.pipeline_metrics ORDER BY metric_id DESC LIMIT 5;"
```

## 3. Create Monitoring Package

Create:

```text
warehouse/
└── monitoring/
    ├── __init__.py
    └── check_pipeline_health.py
```

Commands:

```cmd
mkdir warehouse\monitoring
type nul > warehouse\monitoring\__init__.py
```

## 4. Important Spark Sink Semantics

Bronze, Silver, and Quarantine are Spark Structured Streaming file sinks.

Do **not** count every physical Parquet file recursively with PyArrow.

Spark maintains committed sink state under:

```text
data_lake/bronze/orders/_spark_metadata
data_lake/silver/orders/_spark_metadata
data_lake/quarantine/orders/_spark_metadata
```

The health checker must use the Spark-committed logical file set represented by `_spark_metadata`.

Validated logical counts:

```text
Spark Bronze count = 127
Spark Silver count = 127
Spark Quarantine count = 0
```

Physical/uncommitted files are intentionally not exposed as monitoring metrics.

## 5. Health Checker

Create:

```text
warehouse/monitoring/check_pipeline_health.py
```

Use the final Session 09 implementation that:

```text
- reconstructs Spark-committed files from _spark_metadata
- counts rows from committed Parquet files only
- collects Raw / Fact / Gold counts from PostgreSQL
- checks latest_loaded_at freshness
- evaluates HEALTHY / DEGRADED
- writes an append-only snapshot to control.pipeline_metrics
- exits with code 1 when DEGRADED
```

Key reconciliation logic:

```python
if bronze_rows != silver_rows + quarantine_rows:
    issues.append(...)

if silver_rows != raw_orders:
    issues.append(...)

if raw_orders != fact_orders:
    issues.append(...)

if fact_orders != gold_order_count:
    issues.append(...)
```

Freshness:

```python
MAX_LOAD_AGE_MINUTES = int(
    os.getenv("MAX_LOAD_AGE_MINUTES", "2880")
)
```

Failure propagation:

```python
if health["status"] != "HEALTHY":
    raise SystemExit(1)
```

## 6. Validate the Checker Locally

Run:

```cmd
ruff check warehouse\monitoring
```

Then:

```cmd
python warehouse\monitoring\check_pipeline_health.py
```

Expected current result:

```text
RetailPulse Pipeline Health
---------------------------
Bronze rows:       127
Silver rows:       127
Quarantine rows:   0
Raw orders:        127
Fact orders:       127
Gold order count:  127
Latest load:       ...
Load age:          ...

Status: HEALTHY
```

Check exit code:

```cmd
echo %ERRORLEVEL%
```

Expected:

```text
0
```

A degraded health result exits with code `1`.

## 7. Validate Spark Counts Manually

Launch PySpark:

```cmd
docker compose exec spark-master /opt/spark/bin/pyspark ^
  --master spark://spark-master:7077
```

Inside:

```python
bronze = spark.read.parquet(
    "/opt/retailpulse/data_lake/bronze/orders"
)

silver = spark.read.parquet(
    "/opt/retailpulse/data_lake/silver/orders"
)

quarantine = spark.read.parquet(
    "/opt/retailpulse/data_lake/quarantine/orders"
)

bronze.count()
silver.count()
quarantine.count()
```

Validated result:

```text
127
127
0
```

## 8. Add Health-Rule Unit Tests

Create:

```text
warehouse/tests/test_pipeline_health.py
```

Cover:

```text
1. healthy reconciliation
2. Bronze vs Silver + Quarantine mismatch
3. Silver vs Raw mismatch
4. Raw vs Fact mismatch
5. Fact vs Gold mismatch
6. stale warehouse data
7. missing latest-load timestamp
```

Run:

```cmd
pytest warehouse\tests\test_pipeline_health.py -v
```

Expected:

```text
7 passed
```

Then run full suite:

```cmd
pytest -v
```

Expected Session 09 state:

```text
10 passed
```

Run lint:

```cmd
ruff check .
```

Expected:

```text
All checks passed!
```

## 9. Integrate Health Check into Airflow

Update:

```text
airflow/dags/retailpulse_warehouse_pipeline.py
```

Add:

```python
check_pipeline_health = BashOperator(
    task_id="check_pipeline_health",
    bash_command=(
        "cd /opt/retailpulse && "
        "python warehouse/monitoring/check_pipeline_health.py"
    ),
)
```

Use the TaskFlow task instances correctly:

```python
validation = validate_raw_orders()

metrics = record_pipeline_metrics(
    validation
)

(
    run_incremental_loader
    >> validation
    >> run_dbt_build
    >> check_pipeline_health
    >> metrics
)
```

Final DAG order:

```text
run_incremental_loader
→ validate_raw_orders
→ run_dbt_build
→ check_pipeline_health
→ record_pipeline_metrics
```

## 10. Validate Airflow

Check DAG parsing:

```cmd
docker compose exec airflow-scheduler airflow dags list
```

Expected DAG:

```text
retailpulse_warehouse_pipeline
```

Run:

```cmd
ruff check .
pytest -v
```

Then trigger one manual DAG run in the Airflow UI:

```text
http://localhost:8083
```

Expected task order:

```text
run_incremental_loader
→ validate_raw_orders
→ run_dbt_build
→ check_pipeline_health
→ record_pipeline_metrics
```

Expected `check_pipeline_health` task log:

```text
Bronze rows:       127
Silver rows:       127
Quarantine rows:   0
Raw orders:        127
Fact orders:       127
Gold order count:  127

Status: HEALTHY
```

All tasks should be green.

## 11. Validate Audit Persistence

Run:

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "SELECT metric_id, recorded_at, bronze_rows, silver_rows, quarantine_rows, raw_orders, fact_orders, gold_order_count, latest_loaded_at, status, details FROM control.pipeline_metrics ORDER BY metric_id DESC LIMIT 5;"
```

Expected:

```text
new row per health-check execution
status = HEALTHY for the validated clean lineage
```

## 12. Session 09 Final Architecture

```text
Kafka
  ↓
Spark Structured Streaming
  ↓
Bronze
  ↓
Silver / Quarantine
  ↓
incremental warehouse loader
  ↓
raw.orders
  ↓
dbt
  ↓
analytics.fct_orders
  ↓
analytics.mart_daily_sales
  ↓
check_pipeline_health
  ↓
control.pipeline_metrics
  ↓
Airflow success / failure
```

The checker provides:

```text
logical layer reconciliation
freshness validation
persistent health history
non-zero failure exit code
Airflow visibility
```

## Session 09 Validation Gate

```text
[x] control.pipeline_metrics exists
[x] Bronze count uses Spark logical committed state
[x] Silver count uses Spark logical committed state
[x] Quarantine count uses Spark logical committed state
[x] Bronze = Silver + Quarantine validated
[x] Silver = Raw validated
[x] Raw = Fact validated
[x] Fact = Gold validated
[x] freshness check implemented
[x] HEALTHY / DEGRADED implemented
[x] DEGRADED returns non-zero exit code
[x] health snapshots persist to PostgreSQL
[x] 7 health-rule tests pass
[x] full repository suite = 10 passing tests
[x] Ruff passes
[x] health checker integrated into Airflow
[x] Airflow DAG parses successfully
[x] manual Airflow run is fully green
[x] current logical lineage = 127 / 127 / 0 / 127 / 127 / 127
```

**Session 09 status: Complete**
