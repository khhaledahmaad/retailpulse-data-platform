# Session 11 Runbook — Lag-Aware Monitoring / Operational SLOs

## Session goal

Replace strict live cross-layer equality with lag-aware operational monitoring while preserving exact settled-state validation.

Session 11 adds:

- `HEALTHY`, `WARNING`, and `DEGRADED` monitoring states
- configurable live row-lag tolerance
- strict reconciliation mode for settled-state validation
- Airflow success on expected `WARNING` conditions
- persisted warning/degraded audit history
- cleaned Airflow DAG wiring
- corrected fresh-install `control.pipeline_metrics` schema
- explicit monitoring configuration in Docker Compose

Core monitoring principles retained:

- Spark logical committed counts remain authoritative.
- Do not use physical Parquet row/file counts as health metrics.
- Gold reconciliation uses `SUM(order_count)`, not Gold table row count.
- Raw → Fact and Fact → Gold remain strict.
- Live lag tolerance applies only where asynchronous processing makes temporary lag legitimate.

---

# 1. Files/config changed

```text
warehouse/monitoring/check_pipeline_health.py
warehouse/tests/test_pipeline_health.py
airflow/dags/retailpulse_warehouse_pipeline.py
docker-compose.yml
warehouse/init/001_create_warehouse.sql
.env.example              # tracked configuration example, if updated
```

Local-only:

```text
.env                      # do not commit
```

If `python-dotenv` was not actually implemented during this session, do not add it to the dependency list or runbook.

---

# 2. Baseline validation

From:

```text
C:\Users\khhal\retailpulse-data-platform
```

Activate the environment:

```cmd
.venv\Scripts\activate
```

Start the stack:

```cmd
docker compose up -d
```

Check services:

```cmd
docker compose ps
```

Run baseline tests:

```cmd
pytest -v
```

Baseline before Session 11:

```text
10 passed
```

Run lint:

```cmd
ruff check .
```

Run health checker:

```cmd
python warehouse\monitoring\check_pipeline_health.py
```

At the captured baseline:

```text
Bronze        494
Silver        494
Quarantine      0
Raw           494
Fact          494
Gold          494
Status        HEALTHY
```

Check Git:

```cmd
git status
```

Expected:

```text
working tree clean
```

---

# 3. Define lag-aware behavior with tests first

Update:

```text
warehouse/tests/test_pipeline_health.py
```

The test contract should cover:

```text
exact reconciliation
→ HEALTHY

small Bronze vs Silver+Quarantine gap
→ WARNING

excessive Bronze gap
→ DEGRADED

small Silver lead over Raw
→ WARNING

excessive Silver lead over Raw
→ DEGRADED

Raw ahead of Silver
→ DEGRADED

strict mode with any cross-layer gap
→ DEGRADED

Raw != Fact
→ DEGRADED

Fact != Gold
→ DEGRADED

stale warehouse data
→ DEGRADED

missing latest load timestamp
→ DEGRADED
```

Tests inject a small tolerance such as:

```python
max_lag_rows=5
```

Run the health tests before implementing the behavior:

```cmd
pytest warehouse\tests\test_pipeline_health.py -v
```

Expected RED result before implementation:

```text
evaluate_health() does not yet accept max_lag_rows / strict
```

This proves the tests describe new behavior.

---

# 4. Implement lag-aware health logic

Update:

```text
warehouse/monitoring/check_pipeline_health.py
```

Add:

```python
import argparse
```

Monitoring defaults:

```python
MAX_LOAD_AGE_MINUTES = int(
    os.getenv("MAX_LOAD_AGE_MINUTES", "2880")
)

MAX_LAG_ROWS = int(
    os.getenv("MAX_LAG_ROWS", "60")
)
```

The operational contract is:

```text
HEALTHY
→ exact reconciliation
→ exit 0

WARNING
→ expected temporary live lag within tolerance
→ exit 0

DEGRADED
→ excessive/reverse lag, analytical mismatch, stale/missing load
→ exit 1
```

### Bronze rule

Evaluate:

```text
Bronze - (Silver + Quarantine)
```

Because Bronze, Silver, and Quarantine are separate Spark streaming queries, a small absolute temporary gap may occur while queries are moving.

Normal mode:

```text
abs(gap) <= MAX_LAG_ROWS
→ WARNING
```

Excessive gap:

```text
abs(gap) > MAX_LAG_ROWS
→ DEGRADED
```

Strict mode:

```text
any non-zero gap
→ DEGRADED
```

### Silver → Raw rule

A small positive gap is legitimate:

```text
Silver > Raw
```

This means Spark has committed data that the warehouse loader has not loaded yet.

Normal mode:

```text
0 < Silver - Raw <= MAX_LAG_ROWS
→ WARNING
```

Excessive positive gap:

```text
Silver - Raw > MAX_LAG_ROWS
→ DEGRADED
```

Reverse gap:

```text
Raw > Silver
→ DEGRADED
```

Raw should not contain more logical events than authoritative Silver.

### Raw → Fact rule

Remain strict:

```text
Raw = Fact
```

Any mismatch:

```text
DEGRADED
```

### Fact → Gold rule

Remain strict:

```text
Fact = SUM(mart_daily_sales.order_count)
```

Any mismatch:

```text
DEGRADED
```

### Freshness rule

Default:

```text
MAX_LOAD_AGE_MINUTES=2880
```

Equivalent to:

```text
48 hours
```

Missing or older data:

```text
DEGRADED
```

---

# 5. Add strict CLI mode

Add CLI support:

```cmd
python warehouse\monitoring\check_pipeline_health.py --strict
```

Optional lag override:

```cmd
python warehouse\monitoring\check_pipeline_health.py --max-lag-rows 100
```

Reject negative lag thresholds.

Normal mode is intended for operational/live monitoring.

Strict mode is intended for:

```text
settled-state validation
recovery validation
idempotency checks
post-backfill reconciliation
```

Change exit behavior from:

```python
if health["status"] != "HEALTHY":
    raise SystemExit(1)
```

to:

```python
if health["status"] == "DEGRADED":
    raise SystemExit(1)
```

Therefore:

```text
HEALTHY  → 0
WARNING  → 0
DEGRADED → 1
```

---

# 6. Validate RED → GREEN

Run:

```cmd
pytest warehouse\tests\test_pipeline_health.py -v
```

Expected:

```text
11 passed
```

Run full suite:

```cmd
pytest -v
```

Expected after Session 11 test additions:

```text
14 passed
```

Run lint:

```cmd
ruff check .
```

Expected:

```text
All checks passed!
```

---

# 7. Real runtime WARNING / strict DEGRADED test

Temporarily stop Airflow scheduling:

```cmd
docker compose stop airflow-scheduler
```

Start Spark lake streaming if not already active:

```cmd
docker compose exec spark-master /opt/spark/bin/spark-submit ^
  --master spark://spark-master:7077 ^
  --conf spark.jars.ivy=/tmp/.ivy2 ^
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3 ^
  /opt/retailpulse/spark/jobs/stream_orders_to_lake.py
```

Start producer:

```cmd
python producer\src\producer.py
```

Produce a small number of events, then stop with:

```text
Ctrl+C
```

Keep the gap below:

```text
60 rows
```

Allow Spark to drain the Kafka events.

Run normal health:

```cmd
python warehouse\monitoring\check_pipeline_health.py
```

Observed Session 11 validation:

```text
Bronze        509
Silver        509
Quarantine      0
Raw           494
Fact          494
Gold          494

Silver - Raw = 15

Status: WARNING
```

Check exit code:

```cmd
echo %ERRORLEVEL%
```

Expected:

```text
0
```

Run the same state in strict mode:

```cmd
python warehouse\monitoring\check_pipeline_health.py --strict
```

Expected:

```text
Status: DEGRADED
```

Check:

```cmd
echo %ERRORLEVEL%
```

Expected:

```text
1
```

This proves:

```text
same live data state
→ operational mode tolerates expected lag
→ strict mode still enforces exact reconciliation
```

---

# 8. Verify persisted health history

Inspect:

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "SELECT metric_id, recorded_at, bronze_rows, silver_rows, quarantine_rows, raw_orders, fact_orders, gold_order_count, status, details FROM control.pipeline_metrics ORDER BY metric_id DESC LIMIT 5;"
```

Expected health history includes:

```text
WARNING
DEGRADED
```

for the controlled lag test.

The strict invocation also records its actual health conclusion before exiting non-zero.

---

# 9. Recover to exact reconciliation

Keep producer stopped.

Run loader:

```cmd
python warehouse\loader\load_orders.py
```

Run dbt:

```cmd
cd warehouse\dbt\retailpulse
dbt build --no-partial-parse --target dev
cd ..\..\..
```

Run normal health:

```cmd
python warehouse\monitoring\check_pipeline_health.py
```

Expected:

```text
Status: HEALTHY
```

Run strict health:

```cmd
python warehouse\monitoring\check_pipeline_health.py --strict
```

Expected:

```text
Status: HEALTHY
```

Check:

```cmd
echo %ERRORLEVEL%
```

Expected:

```text
0
```

---

# 10. Clean duplicate Airflow task instantiation

Update:

```text
airflow/dags/retailpulse_warehouse_pipeline.py
```

Before cleanup, these TaskFlow tasks were instantiated twice:

```text
validate_raw_orders
record_pipeline_metrics
```

Keep only one instance of each:

```python
validation = validate_raw_orders()

metrics = record_pipeline_metrics(validation)

(
    run_incremental_loader
    >> validation
    >> run_dbt_build
    >> check_pipeline_health
    >> metrics
)
```

Final logical DAG:

```text
run_incremental_loader
→ validate_raw_orders
→ run_dbt_build
→ check_pipeline_health
→ record_pipeline_metrics
```

Validate:

```cmd
ruff check airflow\dags\retailpulse_warehouse_pipeline.py
pytest -v
```

Restart DAG processor:

```cmd
docker compose restart airflow-dag-processor
```

Check import errors:

```cmd
docker compose exec airflow-api-server airflow dags list-import-errors
```

Expected:

```text
no RetailPulse import errors
```

Confirm DAG exists:

```cmd
docker compose exec airflow-api-server airflow dags list
```

---

# 11. Real Airflow WARNING success test

Pause normal scheduling:

```cmd
docker compose exec airflow-api-server airflow dags pause retailpulse_warehouse_pipeline
```

Start scheduler:

```cmd
docker compose start airflow-scheduler
```

Confirm settled state first:

```cmd
python warehouse\monitoring\check_pipeline_health.py --strict
```

Expected:

```text
HEALTHY
```

Manually trigger:

```text
retailpulse_warehouse_pipeline
```

Watch the DAG.

Once:

```text
run_incremental_loader
```

has succeeded, start the producer:

```cmd
python producer\src\producer.py
```

This deliberately creates new Spark Silver events after the loader has already completed.

Expected logical state at health-check time:

```text
Silver > Raw
```

but with:

```text
Silver - Raw <= MAX_LAG_ROWS
```

Observed Airflow Session 11 proof:

```text
Bronze        510
Silver        510
Quarantine      0
Raw           509
Fact          509
Gold          509

gap = 1

Status: WARNING
Command exited with return code 0
```

Persisted audit row:

```text
metric_id: 86
bronze_rows: 510
silver_rows: 510
raw_orders: 509
fact_orders: 509
gold_order_count: 509
status: WARNING
details: Silver does not reconcile with raw.orders: 510 != 509 (gap=1)
```

This is the key Session 11 operational proof:

```text
Before Session 11
temporary live lag
→ DEGRADED
→ exit 1
→ Airflow retry/failure possible

After Session 11
small expected live lag
→ WARNING
→ exit 0
→ Airflow health task succeeds
→ DAG continues
```

Genuine degraded conditions still return non-zero.

Stop producer after the test.

---

# 12. Settle pipeline after Airflow validation

Run:

```cmd
python warehouse\loader\load_orders.py
```

Then:

```cmd
cd warehouse\dbt\retailpulse
dbt build --no-partial-parse --target dev
cd ..\..\..
```

Verify:

```cmd
python warehouse\monitoring\check_pipeline_health.py --strict
```

Expected settled state:

```text
Bronze        510
Silver        510
Quarantine      0
Raw           510
Fact          510
Gold          510

Status: HEALTHY
```

Exact counts may increase in later use; equality is the invariant.

---

# 13. Expose monitoring SLO configuration in Docker Compose

Update the shared Airflow environment in:

```text
docker-compose.yml
```

Add:

```yaml
# Pipeline monitoring SLOs.
MAX_LAG_ROWS: "${MAX_LAG_ROWS:-60}"
MAX_LOAD_AGE_MINUTES: "${MAX_LOAD_AGE_MINUTES:-2880}"
```

Meaning:

```text
MAX_LAG_ROWS
→ maximum tolerated live row lag
→ default 60

MAX_LOAD_AGE_MINUTES
→ maximum warehouse freshness age
→ default 2880 minutes / 48 hours
```

Docker Compose interpolation precedence is effectively:

```text
host shell environment
→ .env
→ ${VARIABLE:-fallback}
```

The resolved value is injected into the Airflow container environment.

Python then reads:

```python
os.getenv(...)
```

with its own default as the final fallback if the environment variable is absent.

Important:

```text
A host-side direct Python execution does not inherit container environment values.
```

If local `.env` loading is later required for direct host execution, `python-dotenv` can be added deliberately. It was discussed during Session 11 but is not required for the Docker/Airflow path.

---

# 14. `.env` / `.env.example`

For local runtime configuration, `.env` may contain:

```dotenv
MAX_LAG_ROWS=60
MAX_LOAD_AGE_MINUTES=2880
```

Do not commit `.env`.

The tracked example file should contain non-secret defaults:

```dotenv
# Pipeline monitoring SLOs
MAX_LAG_ROWS=60
MAX_LOAD_AGE_MINUTES=2880
```

Recommended tracked file:

```text
.env.example
```

---

# 15. Validate Compose SLO injection

Run:

```cmd
docker compose config
```

Check resolved values:

```cmd
docker compose config | findstr /I "MAX_LAG MAX_LOAD"
```

Expected:

```text
MAX_LAG_ROWS: "60"
MAX_LOAD_AGE_MINUTES: "2880"
```

Recreate relevant Airflow services so new environment values are applied:

```cmd
docker compose up -d ^
  airflow-api-server ^
  airflow-scheduler ^
  airflow-dag-processor
```

Check runtime environment:

```cmd
docker compose exec airflow-scheduler printenv MAX_LAG_ROWS
```

Expected:

```text
60
```

Then:

```cmd
docker compose exec airflow-scheduler printenv MAX_LOAD_AGE_MINUTES
```

Expected:

```text
2880
```

---

# 16. Correct fresh-install warehouse schema

Update:

```text
warehouse/init/001_create_warehouse.sql
```

The development database had already been migrated during earlier sessions using `ALTER TABLE`, but the bootstrap SQL still represented the old monitoring schema.

Final bootstrap file should create:

```sql
CREATE SCHEMA IF NOT EXISTS raw;

CREATE SCHEMA IF NOT EXISTS control;


CREATE TABLE IF NOT EXISTS raw.orders (
    event_id           TEXT PRIMARY KEY,
    event_type         TEXT NOT NULL,
    event_timestamp    TIMESTAMPTZ NOT NULL,
    event_date         DATE NOT NULL,

    order_id           TEXT NOT NULL,
    customer_id        TEXT,
    product_id         TEXT NOT NULL,
    category           TEXT NOT NULL,

    quantity           INTEGER NOT NULL,
    unit_price         NUMERIC(12, 2) NOT NULL,
    order_value        NUMERIC(14, 2) NOT NULL,
    currency           CHAR(3) NOT NULL,

    kafka_key          TEXT,
    kafka_topic        TEXT NOT NULL,
    kafka_partition    INTEGER NOT NULL,
    kafka_offset       BIGINT NOT NULL,
    kafka_timestamp    TIMESTAMPTZ,
    ingested_at        TIMESTAMPTZ,

    loaded_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS control.loaded_files (
    file_path          TEXT PRIMARY KEY,
    dataset_name       TEXT NOT NULL,
    row_count          BIGINT NOT NULL,
    loaded_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS control.loader_watermarks (
    dataset_name       TEXT PRIMARY KEY,
    watermark_date     DATE NOT NULL,
    watermark_hour     INTEGER NOT NULL
        CHECK (
            watermark_hour >= 0
            AND watermark_hour <= 23
        ),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


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

Important schema correction:

```text
added to bootstrap definition:
- bronze_rows
- quarantine_rows

removed from bootstrap definition:
- duplicate_rows
```

No current database recreation is required because the live database already has the correct migrated schema.

The purpose of this change is fresh-install reproducibility.

---

# 17. Final validation gate

Run:

```cmd
pytest -v
```

Expected:

```text
14 passed
```

Run:

```cmd
ruff check .
```

Expected:

```text
All checks passed!
```

Run:

```cmd
docker compose config
```

Expected:

```text
no errors
```

Run:

```cmd
docker compose exec airflow-api-server airflow dags list-import-errors
```

Expected:

```text
no RetailPulse DAG import errors
```

Run strict health:

```cmd
python warehouse\monitoring\check_pipeline_health.py --strict
```

Expected:

```text
Status: HEALTHY
```

---

# 18. Session 11 proven properties

```text
[x] HEALTHY / WARNING / DEGRADED states
[x] live row-lag tolerance
[x] configurable MAX_LAG_ROWS
[x] freshness SLO retained
[x] strict settled-state mode
[x] WARNING exits 0
[x] DEGRADED exits 1
[x] Bronze live lag handled
[x] Silver → Raw live lag handled
[x] reverse Raw > Silver remains degraded
[x] Raw → Fact remains strict
[x] Fact → Gold remains strict
[x] WARNING persisted to control.pipeline_metrics
[x] strict DEGRADED persisted
[x] recovery to HEALTHY proven
[x] Airflow accepts real WARNING state
[x] Airflow no longer retries/fails on expected small live lag
[x] duplicate DAG task instantiation removed
[x] Docker Compose exposes monitoring SLOs
[x] fresh-install pipeline_metrics schema corrected
[x] 14 tests passing
[x] Ruff passing
[x] Docker Compose validation passing
[x] Airflow DAG parsing clean
```

---

# 19. Important architectural interpretation

The platform is now:

```text
idempotent
+ recoverable
+ convergent
+ lag-aware
```

Strict reconciliation is still available when the platform is expected to be settled.

Normal live monitoring recognizes that:

```text
Kafka
Spark
warehouse loader
dbt
```

are asynchronous stages and may legitimately represent slightly different snapshots.

This avoids false operational failures without weakening analytical correctness.

---

# 20. Git update

First inspect the final diff:

```cmd
git status
git diff
```

Ensure `.env` is not staged or tracked.

If `.env.example` was updated, stage it. Recommended explicit staging:

```cmd
git add warehouse/monitoring/check_pipeline_health.py
git add warehouse/tests/test_pipeline_health.py
git add airflow/dags/retailpulse_warehouse_pipeline.py
git add docker-compose.yml
git add warehouse/init/001_create_warehouse.sql
git add .env.example
```

If `.env.example` was not changed, omit that command.

Add this runbook after copying it into the repository runbook/docs location used by previous sessions, for example:

```cmd
git add session_11_runbook.md
```

or use the actual existing runbook directory if Sessions 08–10 are stored elsewhere.

Check staged changes:

```cmd
git status
git diff --cached
```

Commit:

```cmd
git commit -m "Add lag-aware pipeline monitoring and operational SLOs"
```

Push:

```cmd
git push origin main
```

Final check:

```cmd
git status
```

Expected:

```text
On branch main
Your branch is up to date with 'origin/main'.

nothing to commit, working tree clean
```

GitHub Actions should run after the push and return green.

---

# 21. Validation gate before Session 12

Do not start Session 12 until:

```text
[x] 14 tests pass
[x] Ruff passes
[x] Docker Compose config passes
[x] Airflow DAG has no import errors
[x] strict health returns HEALTHY in settled state
[x] WARNING behavior proven with real Airflow run
[x] WARNING persisted in control.pipeline_metrics
[x] .env is not committed
[x] GitHub Actions is green after push
```

Next planned session:

```text
Session 12 — Backfill / Replay Workflow
```

Potential scope:

```text
controlled historical replay
safe reprocessing boundaries
watermark reset strategy
checkpoint considerations
warehouse idempotency during replay
before/after validation
```
