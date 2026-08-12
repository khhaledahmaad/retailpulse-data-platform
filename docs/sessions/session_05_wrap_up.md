# RetailPulse — Session 5 Wrap-Up

**Date:** 9 August 2026  
**Session:** 5 of 30  
**Focus:** Incremental PostgreSQL loading and watermark-based scalability

## Session Goal

Connect the Silver data-lake layer to PostgreSQL using an incremental, idempotent warehouse loader, then remove the full-history scanning bottleneck.

Final architecture:

```text
Silver
  ↓
ingestion_date / ingestion_hour partitions
  ↓
loader watermark
  ↓
active/new partitions only
  ↓
file-level idempotency
  ↓
event-level duplicate protection
  ↓
PostgreSQL
```

## What Was Completed

### 1. Added PostgreSQL Warehouse Schemas

Created:

```text
raw
control
```

The `raw` schema stores warehouse data.

The `control` schema stores operational pipeline state.

### 2. Created `raw.orders`

The main warehouse table contains fields including:

```text
event_id
event_type
event_timestamp
event_date
order_id
customer_id
product_id
category
quantity
unit_price
order_value
currency
kafka_key
kafka_topic
kafka_partition
kafka_offset
kafka_timestamp
ingested_at
loaded_at
```

`event_id` is the primary key, providing event-level duplicate protection.

### 3. Created `control.loaded_files`

Created:

```text
control.loaded_files
```

This table records which Silver files have already been processed:

```text
file_path
dataset_name
row_count
loaded_at
```

This provides file-level idempotency.

### 4. Built the First Incremental Loader

Created:

```text
warehouse/loader/load_orders.py
```

The loader initially performed:

```text
discover Silver Parquet files
→ check whether file was already loaded
→ read Parquet
→ insert rows
→ register file
```

### 5. Fixed Psycopg `executemany()`

The first execution failed because `executemany()` was called on the connection.

The fix was:

```python
with conn.cursor() as cur:
    cur.executemany(
        INSERT_ORDER_SQL,
        values,
    )
```

After the change, the loader worked successfully.

### 6. Transactional Loading

Each file load and its control-table registration run inside one transaction:

```text
load order rows
      +
register file
      ↓
single transaction
```

If the load fails, the file is not falsely marked as completed.

### 7. Event-Level Duplicate Protection

Warehouse inserts use:

```sql
ON CONFLICT (event_id) DO NOTHING
```

Combined with the primary key:

```text
Layer 1
control.loaded_files
→ avoid processing the same file twice

Layer 2
raw.orders.event_id
→ avoid inserting the same event twice
```

## Scalability Problem Identified

The first loader used:

```python
SILVER_ROOT.rglob("*.parquet")
```

which recursively scans the complete Silver history on every run.

This works for a few hundred files, but it does not scale to tens of millions.

Example:

```text
60,000,000 historical files
         ↓
scan all 60,000,000
         ↓
discover only a small number are new
```

The runtime would grow with total historical volume instead of new data volume.

## Improved Silver Partition Strategy

Silver was changed from:

```text
event_date=YYYY-MM-DD
```

to:

```text
ingestion_date=YYYY-MM-DD/
    ingestion_hour=HH/
```

Example:

```text
silver/orders/
└── ingestion_date=2026-08-09/
    ├── ingestion_hour=22/
    └── ingestion_hour=23/
```

### Why Ingestion Time?

Warehouse ingestion should advance according to when data entered the pipeline.

A late-arriving event may have an old business `event_date` but a current `ingestion_date`, so it still lands in an active partition and is discovered correctly.

## Added `control.loader_watermarks`

Created:

```text
control.loader_watermarks
```

The table stores:

```text
dataset_name
watermark_date
watermark_hour
updated_at
```

Example:

```text
dataset_name   = silver_orders
watermark_date = 2026-08-09
watermark_hour = 23
```

This is the persistent warehouse-ingestion position.

## Watermark Read Flow

Each loader run now starts by reading:

```text
control.loader_watermarks
```

Conceptually:

```text
loader starts
   ↓
read previous watermark
   ↓
identify eligible Silver partitions
```

If no watermark exists, the loader processes all currently available partitions.

## Watermark-Based Discovery

The loader no longer recursively scans every historical Parquet file.

Instead:

```text
read watermark
      ↓
inspect ingestion partitions
      ↓
ignore partitions older than watermark
      ↓
scan only watermark hour + newer hours
```

## Why the Watermark Hour Is Re-Scanned

The current watermark hour is intentionally scanned again.

Example:

```text
23:10 loader runs
→ files A, B, C loaded
→ watermark = 23

23:25 Spark writes D and E
into the same hour

23:30 loader runs
```

The loader rescans hour 23:

```text
A, B, C → already loaded → skip
D, E    → new → load
```

This prevents new files arriving in the same hour from being missed.

## Watermark vs Loaded-Files Tracking

They solve different problems:

```text
Watermark
→ eliminate old partitions from discovery

control.loaded_files
→ eliminate duplicate files inside active/new partitions

raw.orders primary key
→ eliminate duplicate events
```

## Watermark Update Flow

After a partition is successfully processed:

```text
update control.loader_watermarks
```

The watermark advances to the latest successfully processed ingestion date and hour.

The update is transactional, so it should not move past failed work.

## Final Incremental Loader Behaviour

Each loader run now performs:

```text
1. Read previous watermark
2. Discover watermark-hour and newer partitions
3. Check control.loaded_files inside eligible partitions
4. Skip already-loaded files
5. Load new files
6. Protect against duplicate event_id values
7. Register successfully loaded files
8. Update watermark
```

Simple mental model:

```text
previous watermark
      ↓
current/new ingestion partitions
      ↓
file-level deduplication
      ↓
event-level deduplication
      ↓
warehouse load
      ↓
advance watermark
```

## Before vs After

### Original Design

```text
60 million historical files
          ↓
scan all history
          ↓
find new files
```

Complexity grows with total history.

### Improved Design

```text
60 million historical files
          ↓
watermark says where to resume
          ↓
ignore old partitions
          ↓
scan active/new partitions only
```

Complexity is now much closer to the amount of new data.

## Important Production Caveat

At truly industrial scale, systems may use:

```text
batch manifests
object-store event notifications
catalog metadata
message-driven ingestion
```

instead of filesystem discovery.

However, RetailPulse now demonstrates the important production-style concepts:

```text
incremental discovery
persistent state
idempotency
watermarking
partition pruning
transactional loading
duplicate protection
```

## Current Architecture

```text
Python Producer
      ↓
Kafka
      ↓
Spark Structured Streaming
      ↓
Silver Parquet
      ↓
ingestion_date
      ↓
ingestion_hour
      ↓
PostgreSQL watermark
      ↓
active/new partitions
      ↓
loaded_files check
      ↓
Python incremental loader
      ↓
raw.orders
```

## Control State

Two PostgreSQL control tables are now used:

```text
control.loaded_files
control.loader_watermarks
```

Their roles:

```text
loaded_files
→ which individual files were processed?

loader_watermarks
→ how far through the partition timeline has the loader progressed?
```

## Useful Commands

Create/update warehouse structures:

```cmd
docker compose exec -T postgres psql -U retailpulse -d retailpulse < warehouse\init\001_create_warehouse.sql
```

Run the loader:

```cmd
python warehouse\loader\load_orders.py
```

Check warehouse rows:

```sql
SELECT COUNT(*)
FROM raw.orders;
```

Check processed files:

```sql
SELECT COUNT(*)
FROM control.loaded_files;
```

Check watermark:

```sql
SELECT *
FROM control.loader_watermarks;
```

Run linting:

```cmd
ruff check spark warehouse
```

## Git Update

The scalability changes were committed with:

```text
Add watermark-based incremental warehouse loading
```

and pushed to GitHub.

## Session 5 Completion Checklist

- [x] PostgreSQL `raw` schema created
- [x] PostgreSQL `control` schema created
- [x] `raw.orders` created
- [x] `control.loaded_files` created
- [x] Silver Parquet loader created
- [x] Psycopg cursor issue fixed
- [x] Transactional file loading implemented
- [x] File-level idempotency implemented
- [x] Event-level duplicate protection implemented
- [x] Full-history scanning scalability problem identified
- [x] Silver changed to ingestion date/hour partitioning
- [x] `control.loader_watermarks` created
- [x] Watermark reads implemented
- [x] Partition-based discovery implemented
- [x] Historical partition pruning implemented
- [x] Current watermark hour safely re-scanned
- [x] Watermark updates implemented
- [x] Incremental design tested
- [x] Changes committed and pushed to GitHub

## Session 6 Preview

The next major layer is dbt.

Target:

```text
PostgreSQL raw.orders
        ↓
dbt staging
        ↓
dimensions / facts
        ↓
analytics marts
        ↓
data-quality tests
```

Planned topics:

```text
dbt project initialisation
PostgreSQL profile
dbt sources
stg_orders
dimensional modelling
fct_orders
analytics marts
not_null / unique tests
accepted-values tests
custom business rules
dbt documentation and lineage
```

**Session 5 status: Complete**

---

## Final Loader Regression and Counting Clarification — 12 August 2026

The incremental loader was tested with multiple Spark streaming runs occurring before a warehouse load.

Example validated behaviour:

```text
Producer / Spark batch 1
→ new Silver files

Producer / Spark batch 2
→ more new Silver files

No loader between batches
        ↓
single loader execution
        ↓
discovers every eligible unregistered Silver file
        ↓
loads the accumulated backlog
```

This confirms that loader execution is independent of Spark-run boundaries. It works from persisted Silver files plus control state, not from an assumption of one Spark run per warehouse load.

### Watermark-Hour Behaviour

The loader deliberately includes the current watermark hour again:

```text
partition < watermark
→ skip

partition == watermark
→ scan again
```

Then:

```text
control.loaded_files
→ skips files already processed
→ allows later files written into the same ingestion hour to be loaded
```

This is important for micro-batch streaming because multiple new Parquet files can arrive in the same hour after an earlier loader run.

### `Rows processed` vs `Rows inserted`

Current `load_orders.py` returns the number of rows read from each new Parquet file and reports the accumulated value as:

```text
Rows processed
```

The PostgreSQL insert also uses:

```sql
ON CONFLICT (event_id) DO NOTHING
```

Therefore, in the general case:

```text
rows processed
may be greater than
rows actually inserted
```

if duplicate `event_id` values are present.

For the current RetailPulse producer/regression data, `event_id` values are unique, so processed rows and inserted rows matched during the clean tests.

`control.loaded_files.row_count` currently records the file row count / rows processed, not a separately measured PostgreSQL insert count.

A future observability enhancement can expose both:

```text
rows_processed
rows_inserted
```

without changing the loader's current idempotency model.
