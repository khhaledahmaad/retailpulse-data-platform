# RetailPulse — Session 4 Wrap-Up

**Date:** 9 August 2026  
**Session:** 4 of 30  
**Focus:** Bronze, Silver, Quarantine, and Spark checkpoint recovery

## Session Goal

Move RetailPulse from a console-only streaming demo to a persisted streaming data pipeline:

```text
Python Producer
      ↓
Apache Kafka
      ↓
orders topic
      ↓
Spark Structured Streaming
      │
      ├── Bronze
      ├── Silver
      └── Quarantine
```

The session also introduced Spark checkpointing so the pipeline can recover after a restart without losing its processing position.

## What Was Completed

### 1. Mounted the Data Lake into Spark

The local `data_lake/` directory was mounted into both Spark containers so Spark can write files inside Docker while the outputs remain visible in the Windows project folder.

```text
Windows:
C:\Users\khhal\retailpulse-data-platform\data_lake

Docker:
/opt/retailpulse/data_lake
```

### 2. Created the Persistent Streaming Job

Created:

```text
spark/jobs/stream_orders_to_lake.py
```

The job now performs:

```text
Kafka read
→ Bronze build
→ JSON parsing
→ validation
→ Silver build
→ Quarantine routing
→ Parquet writes
→ checkpoint updates
```

## Bronze Layer

Bronze preserves the source event with Kafka metadata and minimal transformation.

Important fields:

```text
kafka_key
raw_payload
topic
partition
offset
kafka_timestamp
ingested_at
ingestion_date
```

Path:

```text
data_lake/bronze/orders/
```

Purpose:

> Preserve what actually arrived from the source for traceability, auditing, and replay.

## Silver Layer

Silver contains validated, parsed, and analytics-ready orders.

Important fields:

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
topic
partition
offset
kafka_timestamp
ingested_at
```

Derived field:

```text
order_value = quantity × unit_price
```

Path:

```text
data_lake/silver/orders/
```

## Quarantine Layer

Quarantine captures records received successfully but rejected by validation.

Examples:

```text
missing event_id
missing order_id
missing product_id
invalid event timestamp
quantity <= 0
negative unit price
unsupported currency
```

Important fields:

```text
raw_payload
validation_error
topic
partition
offset
kafka_timestamp
ingested_at
```

Path:

```text
data_lake/quarantine/orders/
```

This keeps bad data out of downstream analytics while preserving it for investigation.

## Nullable Parsing Schema

The Spark schema was deliberately made nullable so malformed or incomplete data can be parsed as far as possible and then handled by validation logic instead of crashing the stream.

## Parquet Output

Spark successfully wrote Snappy-compressed Parquet files:

```text
*.snappy.parquet
```

These are the actual data files.

### CRC Files

Files such as:

```text
.part-00000-....snappy.parquet.crc
```

are checksum files created by the Hadoop local filesystem layer.

Simple distinction:

```text
*.snappy.parquet = actual data
*.crc            = checksum/integrity metadata
```

Other normal artefacts can include:

```text
_SUCCESS
._SUCCESS.crc
_spark_metadata/
```

## Small Files

Structured Streaming writes data continuously in micro-batches, so many small Parquet files can accumulate.

This is expected for the current development setup.

Later improvements may include:

```text
file compaction
larger target file sizes
batch consolidation
partition optimisation
```

## Spark Resource Warning

While the streaming job was running, opening a separate PySpark shell produced:

```text
Initial job has not accepted any resources
```

### Cause

The local cluster currently has:

```text
Spark Master
    ↓
1 Spark Worker
```

The long-running stream was already using the available worker resources, so the PySpark shell had to wait.

### Current Inspection Workflow

```text
1. Run producer
2. Run streaming job
3. Generate data
4. Stop streaming job
5. Open PySpark
6. Inspect Bronze/Silver/Quarantine
7. Exit PySpark
8. Restart streaming job
```

## Spark Checkpointing

Three checkpoint locations were introduced:

```text
data_lake/checkpoints/bronze_orders/
data_lake/checkpoints/silver_orders/
data_lake/checkpoints/quarantine_orders/
```

Checkpoint state is updated continuously while the stream is running.

It is not created only when the application stops.

```text
micro-batch processed
→ checkpoint updated

next micro-batch processed
→ checkpoint updated
```

## What Happens While Spark Is Down

Kafka retains incoming events independently of Spark.

```text
Producer
   ↓
Kafka stores events
   ↓
Spark offline
```

When Spark restarts with the same checkpoint:

```text
checkpoint
   ↓
resume previous Kafka offsets
   ↓
process backlog
   ↓
catch up
   ↓
continue live
```

Temporary Spark downtime therefore does not normally mean data loss, provided Kafka still retains the events and the checkpoint state is preserved.

## Checkpoints and Kafka Offsets

Example:

```text
Checkpoint:
partition 0 → offset 220
partition 1 → offset 198
partition 2 → offset 205
```

If Kafka advances while Spark is down:

```text
partition 0 → 240
partition 1 → 215
partition 2 → 230
```

Spark can restart from its saved progress and catch up.

## First-Run Backfill Strategy

The original configuration used:

```python
.option("startingOffsets", "latest")
```

Meaning:

```text
No checkpoint
→ start from newest Kafka data
→ older retained events are skipped
```

For RetailPulse development, the decision was made to use:

```python
.option("startingOffsets", "earliest")
```

This gives the desired behaviour:

```text
First run:
historical Kafka backlog
→ catch up
→ continue live

Later restart:
checkpoint
→ catch up missed events
→ continue live
```

## Key Reliability Concept

The combination of:

```text
Kafka retention
+
Spark checkpoints
```

decouples event storage from event processing.

Kafka can continue collecting events while Spark is unavailable, and Spark can later recover its position and process the backlog.

## Current Architecture

```text
                           +----------------+
                           | Python Producer|
                           +-------+--------+
                                   |
                                   v
                              +---------+
                              |  Kafka  |
                              +----+----+
                                   |
                                   v
                       +-------------------------+
                       | Spark Structured Stream |
                       +------------+------------+
                                    |
                  +-----------------+-----------------+
                  |                 |                 |
                  v                 v                 v
             +---------+       +---------+       +------------+
             | Bronze  |       | Silver  |       | Quarantine |
             | raw     |       | trusted |       | rejected   |
             +---------+       +---------+       +------------+
                  |                 |                 |
                  +-----------------+-----------------+
                                    |
                                    v
                              Checkpoints
```

## Useful Commands

Start the platform:

```cmd
docker compose up -d
```

Check containers:

```cmd
docker compose ps
```

Start the producer:

```cmd
python producer\src\producer.py
```

Run the persistent Spark stream:

```cmd
docker compose exec spark-master /opt/spark/bin/spark-submit ^
  --master spark://spark-master:7077 ^
  --conf spark.jars.ivy=/tmp/.ivy2 ^
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3 ^
  /opt/retailpulse/spark/jobs/stream_orders_to_lake.py
```

Inspect outputs:

```cmd
dir data_lake\bronze\orders
dir data_lake\silver\orders
dir data_lake\quarantine\orders
```

Launch PySpark:

```cmd
docker compose exec spark-master /opt/spark/bin/pyspark ^
  --master spark://spark-master:7077
```

## Session 4 Completion Checklist

- [x] Data lake mounted into Spark containers
- [x] Persistent streaming job created
- [x] Bronze output implemented
- [x] Silver output implemented
- [x] Quarantine output implemented
- [x] Validation rules added
- [x] `order_value` transformation added
- [x] Parquet output confirmed
- [x] CRC/checksum files understood
- [x] Small-file behaviour identified
- [x] Spark resource contention understood
- [x] Separate checkpoint directories implemented
- [x] Checkpoint recovery behaviour understood
- [x] Kafka backlog behaviour understood
- [x] `earliest` first-run backfill strategy selected

## Session 5 Preview

Next:

```text
Silver Parquet
      ↓
Incremental Loader
      ↓
PostgreSQL
```

Planned topics:

```text
PostgreSQL schemas
raw order table
control/audit tables
incremental partition discovery
idempotent loading
duplicate protection
load reconciliation
```

**Session 4 status: Complete**
