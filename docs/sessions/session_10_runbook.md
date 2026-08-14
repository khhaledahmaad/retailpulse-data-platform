# RetailPulse — Session 10 Reproduction Runbook

**Session:** 10  
**Focus:** Resilience, recovery, and idempotent reruns  
**Goal:** Prove the platform can be safely rerun, recover from Spark interruption, survive full Docker restarts, and converge back to a healthy reconciled state without duplicating business data.

## 1. Capture a Clean Baseline

```cmd
python warehouse\monitoring\check_pipeline_health.py
```

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "SELECT COUNT(*) AS raw_orders FROM raw.orders;"
```

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "SELECT COUNT(*) AS fact_orders FROM analytics.fct_orders;"
```

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "SELECT SUM(order_count) AS gold_order_count FROM analytics.mart_daily_sales;"
```

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "SELECT COUNT(*) AS loaded_files FROM control.loaded_files;"
```

Validated baseline:

```text
Bronze        253
Silver        253
Quarantine      0
Raw           253
Fact          253
Gold          253
Loaded files  150

Status: HEALTHY
```

## 2. Prove Loader Rerun Idempotency

```cmd
python warehouse\loader\load_orders.py
```

Validated:

```text
Files discovered: 50
Files skipped: 50
Files loaded: 0
Rows processed: 0
Rows inserted: 0
Duplicate rows ignored: 0
```

Recheck:

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "SELECT COUNT(*) AS raw_orders FROM raw.orders;"
```

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "SELECT COUNT(*) AS loaded_files FROM control.loaded_files;"
```

Expected and validated:

```text
raw_orders   = 253
loaded_files = 150
```

Then:

```cmd
python warehouse\monitoring\check_pipeline_health.py
```

Validated:

```text
Bronze       253
Silver       253
Quarantine     0
Raw          253
Fact         253
Gold         253

Status: HEALTHY
```

Result:

```text
same Silver input
→ loader rerun
→ all known files skipped
→ no duplicate rows inserted
→ no extra loaded_files records
→ health unchanged
```

## 3. Prove dbt Rerun Safety

```cmd
cd warehouse\dbt\retailpulse
dbt build --no-partial-parse --target dev
```

Key validated result:

```text
analytics.fct_orders
[INSERT 0 0]
```

Return to root:

```cmd
cd ..\..\..
```

Validate:

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "SELECT COUNT(*) AS fact_orders FROM analytics.fct_orders;"
```

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "SELECT SUM(order_count) AS gold_order_count FROM analytics.mart_daily_sales;"
```

Validated:

```text
Fact = 253
Gold = 253
```

Then:

```cmd
python warehouse\monitoring\check_pipeline_health.py
```

Validated:

```text
Status: HEALTHY
```

Result:

```text
same Raw input
→ dbt rerun
→ incremental fact inserts 0 rows
→ Gold remains correct
→ no duplicate analytical rows
```

## 4. Validate Airflow Behaviour During Live Ingestion

Current strict health rules:

```text
Bronze = Silver + Quarantine
Silver = Raw
Raw = Fact
Fact = Gold
```

Observed:

```text
Producer running
→ Spark streaming
→ Airflow triggered while data is moving
→ layers can be temporarily out of sync
→ check_pipeline_health fails
→ Airflow retries / can fail
```

Recovery:

```text
stop producer
→ keep Spark streamer running
→ allow Kafka/lake to settle
→ trigger Airflow again
→ loader catches up
→ dbt catches up
→ all layers reconcile
→ HEALTHY
```

This proves safe failure and eventual convergence.

Note:

```text
The current health model is intentionally strict.
It checks settled-state correctness.
A future live-streaming enhancement could add lag tolerance or time-window-based checks.
```

## 5. Prove Spark Checkpoint Recovery

Keep Kafka running and stop only the Spark streaming job.

Capture the current state:

```cmd
python warehouse\monitoring\check_pipeline_health.py
```

While Spark is stopped, produce a small batch of new Kafka events.

Expected:

```text
Kafka receives events
Bronze/Silver do not move while Spark is stopped
```

Restart the same Spark streaming job using the existing checkpoints.

Do not delete or replace checkpoint directories.

Allow Spark to catch up, then:

```cmd
python warehouse\monitoring\check_pipeline_health.py
```

Bronze/Silver may now be ahead of the warehouse.

Trigger the Airflow DAG.

Final check:

```cmd
python warehouse\monitoring\check_pipeline_health.py
```

Validated:

```text
Bronze = Silver
Silver = Raw
Raw = Fact
Fact = Gold
Status = HEALTHY
```

Result:

```text
Spark stopped
→ Kafka keeps events
→ Spark restarts from checkpoint
→ missed offsets are consumed
→ lake catches up
→ Airflow catches warehouse/dbt up
→ no duplicate logical orders
→ HEALTHY
```

## 6. Full Docker Stack Restart Persistence

```cmd
docker compose down
```

Do not use:

```cmd
docker compose down -v
```

Bring the stack back:

```cmd
docker compose up -d
```

Check services:

```cmd
docker compose ps
```

Verify persistence:

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "SELECT COUNT(*) AS raw_orders FROM raw.orders;"
```

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "SELECT COUNT(*) AS loaded_files FROM control.loaded_files;"
```

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "SELECT COUNT(*) AS fact_orders FROM analytics.fct_orders;"
```

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "SELECT SUM(order_count) AS gold_order_count FROM analytics.mart_daily_sales;"
```

Restart Spark with the same checkpoints, produce new events, allow processing, then trigger Airflow.

Final:

```cmd
python warehouse\monitoring\check_pipeline_health.py
```

Validated:

```text
containers removed
→ named volumes survive
→ lake/checkpoints survive
→ PostgreSQL state survives
→ Kafka state survives
→ Spark resumes
→ Airflow runs normally
→ new events continue flowing
→ HEALTHY
```

## 7. Aggressive Mid-Stream Spark Failure Test

This test was completed successfully **twice**.

Procedure:

```text
Producer running
→ Kafka receiving events
→ Spark processing
→ force-stop Spark mid-stream
→ Kafka retains backlog
→ restart Spark with existing checkpoint
→ Spark resumes from committed offsets
→ Bronze/Silver catch up
→ trigger Airflow
→ Raw/Fact/Gold catch up
→ health checker reconciles all layers
→ HEALTHY
```

Validated outcome:

```text
no duplicate downstream business rows
no lost logical events observed
successful recovery on both aggressive test runs
```

## 8. Final Regression Checks

```cmd
pytest -v
```

```cmd
ruff check .
```

```cmd
python warehouse\monitoring\check_pipeline_health.py
```

Expected:

```text
tests pass
Ruff passes
pipeline status = HEALTHY
```

## 9. Session 10 Proven Properties

```text
[x] loader rerun idempotency
[x] known Silver files skipped
[x] loader rerun inserts 0 rows
[x] loaded_files unchanged on no-op rerun

[x] dbt incremental rerun idempotency
[x] fct_orders inserts 0 rows with no new input
[x] Gold remains reconciled

[x] Airflow can fail safely during moving-state inconsistency
[x] Airflow rerun converges after upstream settlement

[x] Spark checkpoint recovery after downtime
[x] Kafka retains events during Spark outage
[x] missed Kafka offsets consumed after restart

[x] docker compose down/up preserves PostgreSQL state
[x] Kafka persistence survives restart
[x] Spark checkpoints survive restart
[x] pipeline continues processing new events after restart

[x] forced Spark termination during active ingestion recovers
[x] aggressive recovery repeated successfully twice
[x] no duplicate logical orders after recovery
[x] pipeline returns to HEALTHY
```

## 10. Session 10 Recovery Model

```text
New Kafka events
      ↓
Spark interrupted
      ↓
Kafka retains backlog
      ↓
Spark restarted
      ↓
checkpoint restores processing position
      ↓
Bronze / Silver catch up
      ↓
Airflow rerun
      ↓
incremental loader safely skips/loads files
      ↓
dbt safely updates Fact / Gold
      ↓
health reconciliation
      ↓
HEALTHY
```

## Session 10 Validation Gate

```text
[x] clean baseline captured
[x] loader no-op rerun validated
[x] dbt no-op rerun validated
[x] Airflow retry/recovery behaviour validated
[x] Spark downtime recovery validated
[x] Spark checkpoint recovery validated
[x] full Docker restart persistence validated
[x] PostgreSQL persistence validated
[x] Kafka persistence validated
[x] Spark checkpoint persistence validated
[x] forced mid-stream Spark failure validated
[x] aggressive failure recovery repeated twice
[x] no duplicate business rows after recovery
[x] pipeline converges back to HEALTHY
```

**Session 10 status: Complete**
