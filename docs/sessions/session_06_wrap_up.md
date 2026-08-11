# RetailPulse — Session 6 Wrap-Up

**Date:** 10 August 2026  
**Session:** 6 of 30  
**Focus:** dbt analytics engineering, testing, documentation, and incremental modelling

## Session Goal

Add a dbt analytics layer on top of PostgreSQL so RetailPulse moves from raw warehouse ingestion to tested, query-ready analytical models.

Target flow:

```text
raw.orders
   ↓
stg_orders
   ↓
fct_orders
   ↓
mart_daily_sales
   ↓
dbt tests + docs
```

## What Was Completed

### 1. Added dbt for PostgreSQL

Installed:

```text
dbt-postgres
```

This provides dbt Core plus the PostgreSQL adapter required to build models against the local RetailPulse warehouse.

Verified using:

```cmd
dbt --version
```

### 2. Initialised the dbt Project

Created the dbt project under:

```text
warehouse/dbt/retailpulse/
```

The project connects to the local PostgreSQL database:

```text
host: localhost
port: 5432
database: retailpulse
schema: analytics
```

Connection was verified using:

```cmd
dbt debug
```

### 3. Declared `raw.orders` as a dbt Source

Created a dbt source definition for:

```text
raw.orders
```

This tells dbt that the table already exists outside dbt and should be treated as an upstream source.

Source-level tests were added for important fields such as:

```text
event_id
order_id
product_id
quantity
order_value
```

### 4. Built `stg_orders`

Created:

```text
models/staging/stg_orders.sql
```

The staging model provides a clean SQL boundary over the raw warehouse table.

Current staging materialisation:

```text
VIEW
```

A view stores query logic rather than duplicating all source rows into another physical table.

### 5. Built `fct_orders`

Created:

```text
models/facts/fct_orders.sql
```

The fact model represents order-level analytical data and depends on `stg_orders` using dbt `ref()` lineage.

### 6. Converted `fct_orders` to Incremental

The first version of `fct_orders` was a normal table and would be rebuilt in full.

It was upgraded to:

```text
incremental
```

with:

```text
unique_key = event_id
```

and incremental filtering based on:

```text
loaded_at
```

Conceptually:

```text
First run:
all staging rows
→ fct_orders

Later runs:
only newer rows
→ incremental insert/update
```

An intentional full rebuild remains available with:

```cmd
dbt build --full-refresh
```

### 7. Final Materialisation Strategy

The current dbt strategy is:

```text
staging
→ view

facts
→ incremental

marts
→ table
```

This balances simplicity and scalability.

### 8. Built `mart_daily_sales`

Created:

```text
models/marts/mart_daily_sales.sql
```

The mart provides daily business metrics such as:

```text
order_count
units_sold
gross_revenue
average_order_value
```

### 9. Added dbt Tests

Added schema tests including:

```text
not_null
unique
accepted_values
```

Examples:

```text
event_id must be unique
order_id must not be null
event_type must be order_created
currency must be GBP
```

### 10. Added a Custom Business-Rule Test

Created:

```text
tests/assert_positive_order_values.sql
```

This test fails if any row has:

```text
order_value <= 0
```

A custom dbt data test passes when its SQL query returns zero rows.

### 11. Ran dbt Builds and Tests

Used:

```cmd
dbt build
```

and:

```cmd
dbt test
```

The dependency flow is now:

```text
raw.orders
    ↓
stg_orders
    ↓
fct_orders
    ↓
mart_daily_sales
```

### 12. Generated dbt Documentation

Generated documentation with:

```cmd
dbt docs generate
```

The default docs port conflicted with Kafka UI on port `8080`, so dbt Docs is served on:

```cmd
dbt docs serve --port 8082
```

Current local UI ports:

```text
Kafka UI  → http://localhost:8080
Spark UI  → http://localhost:8081
dbt Docs  → http://localhost:8082
```

### 13. dbt Git Hygiene

Generated dbt artefacts were excluded from version control.

Ignored:

```text
warehouse/dbt/retailpulse/target/
warehouse/dbt/retailpulse/logs/
warehouse/dbt/retailpulse/dbt_packages/
warehouse/dbt/retailpulse/profiles.yml
```

Project files that remain tracked include:

```text
dbt_project.yml
models/
tests/
macros/
seeds/
snapshots/
packages.yml
```

`package-lock.yml` can remain tracked for reproducibility when dbt packages are used.

## Current Analytics Architecture

```text
Kafka
  ↓
Spark Structured Streaming
  ↓
Silver Parquet
  ↓
Incremental Python Loader
  ↓
PostgreSQL raw.orders
  ↓
dbt staging view
  ↓
dbt incremental fact
  ↓
dbt analytics mart
  ↓
tests + documentation
```

## Key Design Decisions

### Staging as a View

Reason:

```text
avoid unnecessary physical duplication
keep the source boundary simple
allow downstream filtering
```

### Fact as Incremental

Reason:

```text
event-grain table grows continuously
full rebuild becomes expensive
loaded_at provides a natural incremental boundary
```

### Mart as Table

Reason:

```text
current mart is small
aggregation is cheap
simple rebuild is acceptable
```

This can be revisited later as scale grows.

## Useful Commands

Test dbt connection:

```cmd
dbt debug
```

Build models and tests:

```cmd
dbt build
```

Run tests only:

```cmd
dbt test
```

Force a full rebuild:

```cmd
dbt build --full-refresh
```

Generate docs:

```cmd
dbt docs generate
```

Serve docs:

```cmd
dbt docs serve --port 8082
```

## Git Update

Session 6 changes were committed with:

```text
Add dbt analytics models and incremental fact loading
```

Generated dbt artefacts were also added to `.gitignore`.

## Session 6 Completion Checklist

- [x] `dbt-postgres` installed
- [x] dbt project initialised
- [x] PostgreSQL connection verified
- [x] `raw.orders` declared as a source
- [x] `stg_orders` created
- [x] staging materialised as a view
- [x] `fct_orders` created
- [x] fact model converted to incremental
- [x] `event_id` configured as unique key
- [x] `loaded_at` used as incremental boundary
- [x] `mart_daily_sales` created
- [x] schema tests added
- [x] custom business-rule test added
- [x] dbt build/test workflow working
- [x] dbt docs generated
- [x] dbt docs port conflict resolved
- [x] generated dbt artefacts added to `.gitignore`
- [x] changes committed and pushed to GitHub

## Session 7 Preview

The next major layer is Apache Airflow orchestration.

Target:

```text
discover Silver partitions
        ↓
run incremental warehouse loader
        ↓
validate warehouse load
        ↓
run dbt build
        ↓
record pipeline metrics
```

Planned topics:

```text
Airflow Docker services
Airflow metadata database
DAG creation
task dependencies
retries
timeouts
loader execution
dbt execution
failure handling
operational logging
```

Airflow will connect the separate components already built into one observable downstream workflow.

**Session 6 status: Complete**
