# RetailPulse — Session 6 Reproduction Runbook

**Goal:** Build the dbt analytics/Gold layer on PostgreSQL.

## 1. Install dbt

Ensure:

```text
dbt-postgres
```

is installed.

```cmd
pip install -r requirements-dev.txt
dbt --version
```

## 2. Project path

```cmd
cd warehouse\dbt\retailpulse
```

Core structure:

```text
models/
├── staging/
│   └── stg_orders.sql
├── facts/
│   └── fct_orders.sql
└── marts/
    └── mart_daily_sales.sql

tests/
└── assert_positive_order_values.sql
```

## 3. Configure two dbt targets

Local Windows:

```text
target: dev
host: localhost
```

Airflow/Docker:

```text
target: airflow
host: postgres
```

Both use:

```text
port: 5432
dbname: retailpulse
schema: analytics
user: retailpulse
```

## 4. Validate local dbt connection

```cmd
dbt debug --target dev
```

## 5. Build the dbt layer

For the current local environment, avoid the stale partial-parser cache:

```cmd
dbt build --no-partial-parse --target dev
```

First clean rebuild when required:

```cmd
dbt build --full-refresh --no-partial-parse --target dev
```

## 6. Run tests

```cmd
dbt test --no-partial-parse --target dev
```

Expected model strategy:

```text
stg_orders        → view
fct_orders        → incremental
mart_daily_sales  → table
```

## 7. Count Raw / Staging / Fact / Gold Mart

From the repository root or any CMD:

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "SELECT COUNT(*) AS raw_orders FROM raw.orders;"
```

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "SELECT COUNT(*) AS stg_orders FROM analytics.stg_orders;"
```

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "SELECT COUNT(*) AS fct_orders FROM analytics.fct_orders;"
```

Gold mart row count:

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "SELECT COUNT(*) AS mart_daily_sales_rows FROM analytics.mart_daily_sales;"
```

Important:

```text
mart_daily_sales row count is number of aggregated day rows,
not number of order events.
```

Validate Gold metrics instead:

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "SELECT * FROM analytics.mart_daily_sales ORDER BY event_date;"
```

Validate total orders represented by the Gold mart:

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "SELECT SUM(order_count) AS gold_order_count FROM analytics.mart_daily_sales;"
```

For a clean all-valid lineage:

```text
raw.orders count
≈ stg_orders count
≈ fct_orders count
≈ SUM(mart_daily_sales.order_count)
```

## 8. Validate incremental fact behaviour

Record fact count:

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "SELECT COUNT(*) FROM analytics.fct_orders;"
```

Generate/load new events through Sessions 2–5, then run:

```cmd
dbt build --no-partial-parse --target dev
```

Count again:

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "SELECT COUNT(*) FROM analytics.fct_orders;"
```

Only new warehouse rows should extend the incremental fact.

## 9. dbt docs

```cmd
dbt docs generate --no-partial-parse --target dev
dbt docs serve --port 8082
```

Open:

```text
http://localhost:8082
```

## 10. Git hygiene

Ignore:

```text
warehouse/dbt/retailpulse/target/
warehouse/dbt/retailpulse/logs/
warehouse/dbt/retailpulse/dbt_packages/
warehouse/dbt/retailpulse/profiles.yml
warehouse/dbt/retailpulse/.user.yml
```

## Session 6 validation gate

```text
[ ] dbt debug passes
[ ] stg_orders builds
[ ] fct_orders builds incrementally
[ ] mart_daily_sales builds
[ ] all dbt tests pass
[ ] raw count query works
[ ] staging count query works
[ ] fact count query works
[ ] Gold SUM(order_count) reconciles to event-level fact
[ ] docs generate and serve on 8082
```
