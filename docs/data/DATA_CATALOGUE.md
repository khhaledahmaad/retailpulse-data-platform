# RetailPulse Data Catalogue

## 1. Purpose

This catalogue describes the durable lake datasets, warehouse business models and operational control tables in v1. The primary business-facing analytical outputs are `analytics.fct_orders` and `analytics.mart_daily_sales`.

## 2. Lake datasets

### Bronze — `data_lake/bronze/orders`

**Grain:** one physical Kafka delivery per committed Bronze row.

**Partition:** `ingestion_date`.

| Field | Description |
|---|---|
| `kafka_key` | Kafka message key as string |
| `raw_payload` | Original Kafka value as string |
| `topic` | Kafka topic |
| `partition` | Kafka partition |
| `offset` | Kafka offset |
| `kafka_timestamp` | Broker record timestamp |
| `ingested_at` | Spark ingestion timestamp |
| `ingestion_date` | Derived date used for partitioning |

**Authority:** Spark `_spark_metadata` defines committed files.

### Silver — `data_lake/silver/orders`

**Grain:** one accepted physical event delivery per committed Silver row. The same `event_id` can appear more than once because physical delivery is at-least-once.

**Partitions:** `ingestion_date`, `ingestion_hour`.

| Field | Description |
|---|---|
| `event_id` | Logical event identifier |
| `event_type` | Event action |
| `event_timestamp` | Parsed event/business timestamp |
| `event_date` | Event date |
| `order_id` | Business order identifier |
| `customer_id` | Optional customer identifier |
| `product_id` | Product identifier |
| `category` | Product category |
| `quantity` | Units ordered |
| `unit_price` | Unit price |
| `order_value` | Quantity × unit price, rounded to 2 decimals |
| `currency` | Currency code |
| `kafka_key` | Kafka key |
| `topic` | Kafka topic |
| `partition` | Kafka partition |
| `offset` | Kafka offset |
| `kafka_timestamp` | Broker record timestamp |
| `ingested_at` | Spark ingestion timestamp |
| `ingestion_date` | Ingestion date partition |
| `ingestion_hour` | Ingestion hour partition |

### Quarantine — `data_lake/quarantine/orders`

**Grain:** one rejected physical delivery per committed quarantine row.

| Field | Description |
|---|---|
| `raw_payload` | Original event payload |
| `schema_version` | Parsed version where available |
| `contract_error` | Structural/version rejection reason |
| `validation_error` | Domain quality rejection reason |
| `topic` | Kafka topic |
| `partition` | Kafka partition |
| `offset` | Kafka offset |
| `kafka_timestamp` | Broker timestamp |
| `ingested_at` | Spark ingestion timestamp |

## 3. Raw warehouse

### `raw.orders`

**Implementation:** `warehouse/init/001_create_warehouse.sql`

**Grain:** one logical event per unique `event_id`.

**Primary key:** `event_id`.

**Load source:** committed Silver Parquet via `warehouse/loader/load_orders.py`.

| Column | PostgreSQL type | Nullable | Description |
|---|---|---:|---|
| `event_id` | `TEXT` | No | Logical event id / primary key |
| `event_type` | `TEXT` | No | Event action |
| `event_timestamp` | `TIMESTAMPTZ` | No | Event time |
| `event_date` | `DATE` | No | Event date |
| `order_id` | `TEXT` | No | Business order id |
| `customer_id` | `TEXT` | Yes | Customer id |
| `product_id` | `TEXT` | No | Product id |
| `category` | `TEXT` | No | Product category |
| `quantity` | `INTEGER` | No | Units ordered |
| `unit_price` | `NUMERIC(12,2)` | No | Unit price |
| `order_value` | `NUMERIC(14,2)` | No | Extended order value |
| `currency` | `CHAR(3)` | No | Currency |
| `kafka_key` | `TEXT` | Yes | Kafka key |
| `kafka_topic` | `TEXT` | No | Source topic |
| `kafka_partition` | `INTEGER` | No | Source partition |
| `kafka_offset` | `BIGINT` | No | Source offset |
| `kafka_timestamp` | `TIMESTAMPTZ` | Yes | Broker timestamp |
| `ingested_at` | `TIMESTAMPTZ` | Yes | Spark ingestion timestamp |
| `loaded_at` | `TIMESTAMPTZ` | No | Warehouse load timestamp; defaults to `NOW()` |

Duplicate `event_id` inserts are ignored by `ON CONFLICT (event_id) DO NOTHING`.

## 4. dbt analytical models

### `analytics.stg_orders`

**Materialization:** view.

**Source:** `raw.orders`.

**Grain:** one logical event per `event_id`.

The staging view selects the validated Raw columns and is the testable dbt boundary. Tests include `event_id` not-null/unique, `order_id` not-null, accepted `event_type`, accepted `currency`, and required quantity/order value.

### `analytics.fct_orders`

**Implementation:** `warehouse/dbt/retailpulse/models/facts/fct_orders.sql`

**Materialization:** incremental table.

**Grain:** one logical accepted order event per `event_id`.

**dbt unique key:** `event_id`.

**Index:** dbt-managed unique B-tree on `event_id`.

**Incremental predicate:** `loaded_at > max(loaded_at)` already present in the fact.

| Column | Meaning |
|---|---|
| `event_id` | Unique logical event id |
| `order_id` | Business order id |
| `customer_id` | Customer id |
| `product_id` | Product id |
| `ordered_at` | Event timestamp renamed for analytics |
| `event_date` | Event date |
| `category` | Product category |
| `quantity` | Units |
| `unit_price` | Price per unit |
| `order_value` | Extended order value |
| `currency` | Currency |
| `loaded_at` | Raw warehouse load timestamp |

Tests include not-null/unique `event_id`, not-null `order_id`, quantity and order value, positive order values, and the cross-event business-key collision test on `order_id`.

### `analytics.mart_daily_sales`

**Implementation:** `warehouse/dbt/retailpulse/models/marts/mart_daily_sales.sql`

**Materialization:** table.

**Grain:** one row per `event_date`.

**Source:** `analytics.fct_orders`.

| Column | Definition |
|---|---|
| `event_date` | Calendar date of the event |
| `order_count` | `count(*)` of fact rows |
| `units_sold` | `sum(quantity)` |
| `gross_revenue` | `round(sum(order_value), 2)` |
| `average_order_value` | `round(avg(order_value), 2)` |

`SUM(order_count)` is used by pipeline health as the Gold logical-event reconciliation count.

## 5. Operational control catalogue

### `control.loaded_files`

**Grain:** one registered Silver file path.

| Column | Purpose |
|---|---|
| `file_path` | Primary key; processed file identity |
| `dataset_name` | Logical dataset name |
| `row_count` | Rows observed in file |
| `loaded_at` | Registration timestamp |

### `control.loader_watermarks`

**Grain:** one row per loader dataset.

Tracks the latest normal-mode partition date/hour. Historical backfill/replay does not advance the live watermark.

### `control.pipeline_metrics`

**Grain:** one health-evaluation snapshot.

Important fields:

```text
recorded_at
bronze_rows
silver_rows
silver_unique_events
quarantine_rows
raw_orders
fact_orders
gold_order_count
latest_loaded_at
status
details
```

`status` reflects the evaluated health state at that snapshot, including temporary `DEGRADED` catch-up periods.

### `control.pipeline_runs`

**Grain:** one Airflow DAG run (`airflow_run_id` unique).

Captures start/end, loader files/rows/duplicates, dbt status, health status, Raw count, latest load, final status and failure message.

### `control.pipeline_incidents`

**Grain:** incident lifecycle record.

A partial unique index prevents more than one simultaneously open incident of the same `incident_type`.

Tracks severity, details, opened/resolved times, run ids and alert/recovery notification timestamps.

### `control.event_reprocessing_log`

**Grain:** one quarantine remediation attempt.

Tracks original errors/timestamp, corrections JSON, action (`DRY_RUN` or `PUBLISH`), status, republish metadata and error message.
