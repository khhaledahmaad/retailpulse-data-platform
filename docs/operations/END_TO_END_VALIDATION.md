# RetailPulse End-to-End Validation

Use this checklist after a fresh start, significant change, recovery exercise or before release/tagging.

## 1. Infrastructure

```cmd
docker compose config --quiet
docker compose up -d
docker compose ps
```

Critical services should be running; Postgres, Airflow DB, Kafka, Spark master and Airflow API should be healthy.

## 2. Spark application

```cmd
docker compose exec spark-master sh -lc "ps -ef | grep stream_orders_to_lake.py | grep -v grep"
```

If absent:

```cmd
docker compose exec -d -e PYTHONPATH=/opt/retailpulse spark-master /opt/spark/bin/spark-submit ^
  --master spark://spark-master:7077 ^
  --conf spark.jars.ivy=/tmp/.ivy2 ^
  --conf spark.executorEnv.PYTHONPATH=/opt/retailpulse ^
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3 ^
  /opt/retailpulse/spark/jobs/stream_orders_to_lake.py
```

## 3. Kafka topic

```cmd
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh ^
  --bootstrap-server localhost:9092 ^
  --list
```

Expected topic includes:

```text
orders
```

## 4. Produce a finite smoke batch

Record the baseline strict-health counts, then:

```cmd
python -m producer.src.producer --count 20 --interval 0 --quiet
```

Expected: producer reports exactly 20 events.

## 5. Allow Spark and Airflow to process

Wait for Spark to commit the events and for the next Airflow run, or trigger the DAG manually in:

```text
http://localhost:8083
```

## 6. Strict health

```cmd
python -m warehouse.monitoring.check_pipeline_health --strict
```

Expected stable invariants:

```text
Bronze = Silver + Quarantine
Silver unique = Raw
Raw = Fact
Fact = Gold order count
Status = HEALTHY
```

The exact absolute counts depend on retained test history.

## 7. Recent Airflow runs

```cmd
docker compose exec postgres sh -lc "psql -U $POSTGRES_USER -d $POSTGRES_DB -c \"SELECT airflow_run_id, started_at, finished_at, status, dbt_status, health_status, loader_rows_inserted FROM control.pipeline_runs ORDER BY started_at DESC LIMIT 5;\""
```

## 8. Analytical objects

```cmd
docker compose exec postgres sh -lc "psql -U $POSTGRES_USER -d $POSTGRES_DB -c \"SELECT table_schema, table_name, table_type FROM information_schema.tables WHERE table_schema='analytics' ORDER BY table_name;\""
```

Expected:

```text
fct_orders       BASE TABLE
mart_daily_sales BASE TABLE
stg_orders       VIEW
```

## 9. Business counts

```cmd
docker compose exec postgres sh -lc "psql -U $POSTGRES_USER -d $POSTGRES_DB -c \"SELECT 'raw.orders' AS relation, COUNT(*) AS rows FROM raw.orders UNION ALL SELECT 'analytics.fct_orders', COUNT(*) FROM analytics.fct_orders UNION ALL SELECT 'analytics.mart_daily_sales', COALESCE(SUM(order_count),0) FROM analytics.mart_daily_sales;\""
```

All three logical counts should match once caught up.

## 10. Fact unique index

```cmd
docker compose exec postgres sh -lc "psql -U $POSTGRES_USER -d $POSTGRES_DB -c \"\\d+ analytics.fct_orders\""
```

Expected index includes:

```text
UNIQUE, btree (event_id)
```

The index is dbt-managed; do not create it manually.

## 11. Dashboard

```cmd
python -m warehouse.monitoring.operations_dashboard
```

Open:

```text
http://127.0.0.1:8084
```

Verify current health, recent runs, incident state and trends render quickly.

## 12. Code/data-model gate

```cmd
ruff check .
pytest -v
python -m dotenv run -- dbt build ^
  --project-dir warehouse\dbt\retailpulse ^
  --profiles-dir warehouse\dbt\retailpulse ^
  --target dev
docker compose config --quiet
```

## 13. v1 scale evidence

The production-readiness exercise already validated:

```text
20-event E2E smoke batch: PASS
1,000,000-event burst: PASS
producer throughput: 1,457.6 events/s
catch-up run 1: 730,822 inserted; temporary DEGRADED
catch-up run 2: 269,178 inserted; HEALTHY
final logical count: 1,029,150
strict health runtime at 1M+: 7.88 s
operations dashboard responsiveness at 1M+: PASS
```
