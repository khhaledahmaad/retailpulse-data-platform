# RetailPulse — Session 5 Reproduction Runbook

**Goal:** Load Silver incrementally into PostgreSQL with file-level and event-level idempotency plus watermarks.

## 1. Add loader dependency

Ensure `requirements-dev.txt` includes:

```text
pyarrow
psycopg[binary]
```

Install:

```cmd
pip install -r requirements-dev.txt
```

## 2. Create warehouse structures

File:

```text
warehouse/init/001_create_warehouse.sql
```

Required objects:

```text
raw.orders
control.loaded_files
control.loader_watermarks
```

Apply:

```cmd
docker compose exec -T postgres psql -U retailpulse -d retailpulse < warehouse\init\001_create_warehouse.sql
```

## 3. Validate schemas/tables

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "\dn"
```

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "\dt raw.*"
```

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "\dt control.*"
```

## 4. Create the loader

File:

```text
warehouse/loader/load_orders.py
```

Final behaviour:

```text
read watermark
→ discover watermark hour + newer ingestion partitions
→ skip control.loaded_files
→ read Parquet with PyArrow
→ insert raw.orders
→ ON CONFLICT(event_id) DO NOTHING
→ register loaded file
→ update watermark transactionally
```

## 5. Run the loader

```cmd
python warehouse\loader\load_orders.py
```

## 6. Validate warehouse counts

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "SELECT COUNT(*) AS raw_orders FROM raw.orders;"
```

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "SELECT COUNT(*) AS loaded_files FROM control.loaded_files;"
```

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "SELECT * FROM control.loader_watermarks;"
```

Inspect latest rows:

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "SELECT event_id, order_id, event_timestamp, order_value, kafka_partition, kafka_offset, loaded_at FROM raw.orders ORDER BY loaded_at DESC LIMIT 10;"
```

## 7. Reconcile Silver count vs warehouse

Use Session 4 PySpark count:

```python
silver.count()
```

Then:

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "SELECT COUNT(*) FROM raw.orders;"
```

For a clean all-valid lineage with no intentionally filtered duplicates, these should align.

## 8. Test idempotency

Run loader again without creating new Silver files:

```cmd
python warehouse\loader\load_orders.py
```

Then count again:

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "SELECT COUNT(*) FROM raw.orders;"
```

Expected:

```text
row count unchanged
already loaded files skipped
```

## 9. Test same-hour incremental behaviour

1. Run producer/Spark to create new Silver files.
2. Run loader.
3. Produce more events in the same ingestion hour.
4. Run loader again.

Expected:

```text
watermark hour is scanned again
old files skipped by control.loaded_files
new same-hour files loaded
```

## 10. Lint

```cmd
ruff check spark warehouse
```

## Session 5 validation gate

```text
[ ] raw.orders exists
[ ] control.loaded_files exists
[ ] control.loader_watermarks exists
[ ] loader inserts Silver rows
[ ] warehouse count can be queried
[ ] second loader run is idempotent
[ ] same-hour later files are not missed
[ ] watermark advances
[ ] ruff passes
```
