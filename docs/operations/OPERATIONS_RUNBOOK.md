# RetailPulse Operations Runbook

## 1. Operator map

```text
Infrastructure        docker-compose.yml
Spark lake job        spark/jobs/stream_orders_to_lake.py
Contract              spark/common/order_contract.py
Quality rules         spark/common/order_quality.py
Silver→Raw loader     warehouse/loader/load_orders.py
Warehouse bootstrap   warehouse/init/001_create_warehouse.sql
dbt project           warehouse/dbt/retailpulse/
Airflow DAG           airflow/dags/retailpulse_warehouse_pipeline.py
Health                warehouse/monitoring/check_pipeline_health.py
Monitoring config     warehouse/monitoring/config.py
Alerts                warehouse/monitoring/notifier.py
Terminal view         warehouse/monitoring/operations_view.py
Dashboard             warehouse/monitoring/operations_dashboard.py
Quarantine repair     warehouse/tools/reprocess_quarantine.py
Silver key repair     warehouse/tools/repair_order_business_key.py
Quality parity tool   spark/tools/check_order_quality_parity.py
```

Commands assume repository root and activated `.venv` unless stated otherwise.

## 2. Daily startup

```cmd
docker compose up -d
```

Then:

```cmd
docker compose ps
```

Check whether the Spark application is running:

```cmd
docker compose exec spark-master sh -lc "ps -ef | grep stream_orders_to_lake.py | grep -v grep"
```

If absent, start it:

```cmd
docker compose exec -d -e PYTHONPATH=/opt/retailpulse spark-master /opt/spark/bin/spark-submit ^
  --master spark://spark-master:7077 ^
  --conf spark.jars.ivy=/tmp/.ivy2 ^
  --conf spark.executorEnv.PYTHONPATH=/opt/retailpulse ^
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3 ^
  /opt/retailpulse/spark/jobs/stream_orders_to_lake.py
```

## 3. Health checks

### Normal live health

```cmd
python -m warehouse.monitoring.check_pipeline_health
```

Default thresholds come from `.env` / `warehouse/monitoring/config.py`:

```text
MAX_LAG_ROWS=60
MAX_LOAD_AGE_MINUTES=2880
```

Interpretation:

```text
HEALTHY   no issue
WARNING   tolerated live reconciliation gap; command exits successfully
DEGRADED  tolerance exceeded / strict mismatch / freshness failure; non-zero exit
```

### Strict reconciliation

```cmd
python -m warehouse.monitoring.check_pipeline_health --strict
```

Use strict mode after catch-up, for release validation and after recovery work.

Every health execution records a row in `control.pipeline_metrics` and reconciles incidents.

## 4. Operations views

### Terminal

```cmd
python -m warehouse.monitoring.operations_view
```

### Dashboard

```cmd
python -m warehouse.monitoring.operations_dashboard
```

Open:

```text
http://127.0.0.1:8084
```

The dashboard reads the control tables; it does not scan the lake itself on each browser request.

## 5. Airflow run inspection

Recent runs:

```cmd
docker compose exec postgres sh -lc "psql -U $POSTGRES_USER -d $POSTGRES_DB -c \"SELECT airflow_run_id, started_at, finished_at, status, dbt_status, health_status, loader_rows_inserted, error_message FROM control.pipeline_runs ORDER BY started_at DESC LIMIT 10;\""
```

Airflow UI:

```text
http://localhost:8083
```

DAG:

```text
retailpulse_warehouse_pipeline
```

Schedule:

```text
every 10 minutes
catchup=False
max_active_runs=1
```

## 6. Metrics and incidents

Latest metrics:

```cmd
docker compose exec postgres sh -lc "psql -U $POSTGRES_USER -d $POSTGRES_DB -c \"SELECT recorded_at, bronze_rows, silver_rows, silver_unique_events, quarantine_rows, raw_orders, fact_orders, gold_order_count, status, details FROM control.pipeline_metrics ORDER BY recorded_at DESC LIMIT 10;\""
```

Active incidents:

```cmd
docker compose exec postgres sh -lc "psql -U $POSTGRES_USER -d $POSTGRES_DB -c \"SELECT incident_id, incident_type, severity, details, opened_at, alert_sent_at FROM control.pipeline_incidents WHERE resolved_at IS NULL ORDER BY opened_at DESC;\""
```

Recent incident lifecycle:

```cmd
docker compose exec postgres sh -lc "psql -U $POSTGRES_USER -d $POSTGRES_DB -c \"SELECT incident_id, incident_type, severity, opened_at, resolved_at, opened_by_airflow_run_id, resolved_by_airflow_run_id FROM control.pipeline_incidents ORDER BY opened_at DESC LIMIT 20;\""
```

## 7. Understanding a burst catch-up failure

A failed DAG does not necessarily mean the loader rolled back successful earlier work.

The v1 scale test demonstrated:

```text
1,000,000-event burst
  ↓
run 1 loader inserts 730,822
  ↓
strict health sees Silver ahead of Raw
  ↓
metrics record DEGRADED
  ↓
run marked FAILED
  ↓
next scheduled run inserts remaining 269,178
  ↓
Raw = Fact = Gold = Silver unique
  ↓
HEALTHY
```

Do not manually replay merely because strict health failed during a large live catch-up. First inspect whether upstream is still committing data and allow the next scheduled run to converge.

## 8. Normal loader operation

Manual normal mode:

```cmd
python -m warehouse.loader.load_orders
```

Normal mode uses the live watermark and skips registered files.

## 9. Historical backfill

```cmd
python -m warehouse.loader.load_orders ^
  --from 2026-08-11T00 ^
  --to 2026-08-12T00
```

Backfill:

- requires both `--from` and `--to`;
- processes committed Silver only;
- skips already-loaded files;
- does not advance the live watermark.

## 10. Historical replay

```cmd
python -m warehouse.loader.load_orders ^
  --from 2026-08-11T00 ^
  --to 2026-08-12T00 ^
  --replay
```

Replay rereads already-registered committed files in the range. `raw.orders.event_id` preserves logical idempotency.

## 11. Quarantine remediation

Implementation:

```text
warehouse/tools/reprocess_quarantine.py
```

Dry run:

```cmd
python -m warehouse.tools.reprocess_quarantine ^
  --event-id <EVENT_ID> ^
  --set quantity=1
```

The tool:

1. locates the quarantined event;
2. applies requested field corrections;
3. validates the repaired contract;
4. validates data quality;
5. prints the repaired payload;
6. writes an audit row with `DRY_RUN`.

Publish only after reviewing the dry run:

```cmd
python -m warehouse.tools.reprocess_quarantine ^
  --event-id <EVENT_ID> ^
  --set quantity=1 ^
  --publish
```

Publishing writes `PUBLISHED` / `PUBLISH_FAILED` audit state and broker metadata.

## 12. Targeted durable Silver business-key repair

Implementation:

```text
warehouse/tools/repair_order_business_key.py
```

Dry run:

```cmd
python -m warehouse.tools.repair_order_business_key ^
  --event-id <EVENT_ID> ^
  --old-order-id <OLD_ORDER_ID> ^
  --new-order-id <NEW_ORDER_ID>
```

Apply only after exact-match validation:

```cmd
python -m warehouse.tools.repair_order_business_key ^
  --event-id <EVENT_ID> ^
  --old-order-id <OLD_ORDER_ID> ^
  --new-order-id <NEW_ORDER_ID> ^
  --apply
```

This is a targeted historical correction tool, not a normal transformation path.

## 13. Quality-rule parity

Implementation:

```text
spark/tools/check_order_quality_parity.py
```

Run it through Spark/PySpark in an environment where the project and Spark Python libraries are available. It verifies representative canonical Python quality results equal Spark-expression results.

## 14. dbt operation

Local build:

```cmd
python -m dotenv run -- dbt build ^
  --project-dir warehouse\dbt\retailpulse ^
  --profiles-dir warehouse\dbt\retailpulse ^
  --target dev
```

Fact full refresh when intentionally required:

```cmd
python -m dotenv run -- dbt build ^
  --project-dir warehouse\dbt\retailpulse ^
  --profiles-dir warehouse\dbt\retailpulse ^
  --target dev ^
  --full-refresh
```

Do not manually recreate the `fct_orders(event_id)` index. It is declared in `fct_orders.sql` and dbt recreates it with the table.

## 15. Safe Compose/config operation

Validate:

```cmd
docker compose config --quiet
```

After environment/config changes while the stack is running, recreate affected services or use:

```cmd
docker compose up -d --force-recreate
```

After a full `docker compose down`, a normal:

```cmd
docker compose up -d
```

is sufficient.

Never casually use:

```text
docker compose down -v
```

because it destroys named persisted volumes.

## 16. Shutdown

```cmd
docker compose down
```

The next startup must also restart the Spark application separately.

## 17. Escalation checklist for `DEGRADED`

1. Run the health command again and read the exact issue.
2. Check Spark application is running.
3. Inspect Kafka/Spark UI for ongoing upstream catch-up.
4. Inspect latest `control.pipeline_runs` loader counts.
5. Inspect latest `control.pipeline_metrics` trend.
6. Inspect active incidents.
7. If Silver is ahead of Raw during a large live burst, allow normal scheduling to resume before replaying.
8. If Raw/Fact/Gold mismatch persists after upstream is stable, inspect dbt/Airflow task logs.
9. Use replay/recovery only when the cause is understood.
