# RetailPulse — Session 3 Wrap-Up

**Date:** 8 August 2026  
**Session:** 3 of 30  
**Focus:** Spark Structured Streaming integration with Kafka

## Session Goal

Extend RetailPulse so that Apache Spark can continuously consume order events from Kafka and parse the JSON payload into typed Spark columns.

Target flow:

```text
Python Producer
      ↓
Apache Kafka
      ↓
orders topic
      ↓
Spark Structured Streaming
      ↓
Parsed order rows
```

## What Was Completed

### 1. Added Spark to Docker Compose

The local platform was extended with:

```text
spark-master
spark-worker
```

The current infrastructure now includes PostgreSQL, Kafka, Kafka UI, Spark Master, and Spark Worker.

The Spark Master UI is exposed at:

```text
http://localhost:8081
```

### 2. Spark Master and Worker Roles

The Spark Master coordinates the Spark cluster:

```text
Spark Master
    ↓
allocates work/resources
    ↓
Spark Worker
```

The Spark Worker provides CPU and memory for application execution.

This is different from Kafka. Kafka currently runs as a broker that stores and serves event streams.

### 3. Created the Spark Streaming Job

Created:

```text
spark/jobs/stream_orders.py
```

The application uses:

```python
spark.readStream
```

to consume Kafka continuously.

Kafka is configured as the source using:

```text
kafka:29092
```

because Spark and Kafka are both running inside Docker.

### 4. Defined an Explicit Event Schema

The Kafka JSON payload is parsed into typed columns using a Spark `StructType`.

The event schema contains fields including:

```text
event_id
event_type
event_timestamp
order_id
customer_id
product_id
category
quantity
unit_price
currency
```

### 5. Retained Kafka Metadata

The Spark stream also retains:

```text
kafka_key
topic
partition
offset
kafka_timestamp
```

This metadata will later help with traceability, replay, diagnostics, and pipeline auditing.

### 6. Added the Spark Kafka Connector

The Spark job was submitted with:

```text
org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3
```

This connector enables Structured Streaming to communicate with Kafka.

## Ivy Cache Issue

The first `spark-submit` attempt failed with:

```text
FileNotFoundException:
/nonexistent/.ivy2.5.2/...
```

### Cause

The Spark container attempted to use a default Ivy/Maven cache location under:

```text
/nonexistent/.ivy2.5.2/
```

That path did not exist.

### Fix

The submission command was updated with:

```text
--conf spark.jars.ivy=/tmp/.ivy2
```

The corrected command was:

```cmd
docker compose exec spark-master /opt/spark/bin/spark-submit ^
  --master spark://spark-master:7077 ^
  --conf spark.jars.ivy=/tmp/.ivy2 ^
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3 ^
  /opt/retailpulse/spark/jobs/stream_orders.py
```

This redirected Spark's dependency cache to a writable location.

## Successful Streaming Result

Spark successfully consumed live order events from Kafka and displayed them as parsed rows.

Observed fields included:

```text
kafka_key
topic
partition
offset
kafka_timestamp
event_id
event_type
event_timestamp
order_id
customer_id
product_id
category
quantity
unit_price
currency
```

Spark processed continuous micro-batches such as:

```text
Batch: 147
Batch: 148
Batch: 149
Batch: 150
Batch: 151
```

This confirmed that the application was running as a live Structured Streaming query rather than a one-time batch read.

## Kafka Partitions

The `orders` topic currently has:

```text
3 partitions
```

Conceptually:

```text
orders
├── partition 0
├── partition 1
└── partition 2
```

Kafka uses partitions to divide a topic into independent ordered logs.

Because the producer sends `order_id` as the Kafka message key, Kafka hashes the key to determine which partition receives the event.

## Kafka Offsets

An offset is the position of a message inside a specific Kafka partition.

Example:

```text
partition 1
offset 147
offset 148
offset 149
```

and separately:

```text
partition 2
offset 127
offset 128
offset 129
offset 130
```

Offsets are not global across the whole topic. Each partition has its own independent offset sequence.

Simple definition:

```text
Partition = where the message is stored
Offset    = position of the message inside that partition
```

## Spark UI Clarification

The Spark Master UI at:

```text
http://localhost:8081
```

is primarily a cluster and application monitoring interface.

It shows information such as:

```text
registered workers
CPU cores
memory
active applications
completed applications
executor/resource information
```

It is not designed to display the actual incoming order records.

The current Spark application writes streaming output using:

```python
.writeStream
.format("console")
```

so the parsed data appears in the terminal running `spark-submit`.

Some links inside the Spark UI may point to internal Docker hostnames that are not directly resolvable from the Windows browser. This is a local Docker networking detail and is not currently a blocker.

## Current Networking Model

### Windows Host

The Python producer runs directly on Windows:

```text
Python Producer
      ↓
localhost:9092
      ↓
Kafka EXTERNAL listener
```

### Docker Network

Spark and Kafka both run inside Docker:

```text
Spark
  ↓
kafka:29092
  ↓
Kafka INTERNAL listener
```

## Current Platform Architecture

```text
                  RetailPulse
                       |
        +--------------+--------------+
        |                             |
   PostgreSQL                       Kafka
                                      |
                           +----------+----------+
                           |                     |
                    Python Producer         Kafka UI
                           |
                      orders topic
                           |
                           v
                    Spark Master
                           |
                           v
                    Spark Worker
                           |
                           v
               Structured Streaming
                           |
                           v
                 Parsed typed rows
```

## Useful Commands

Start the platform:

```cmd
docker compose up -d
```

Check services:

```cmd
docker compose ps
```

Start the order producer:

```cmd
python producer\src\producer.py
```

Submit the Spark streaming job:

```cmd
docker compose exec spark-master /opt/spark/bin/spark-submit ^
  --master spark://spark-master:7077 ^
  --conf spark.jars.ivy=/tmp/.ivy2 ^
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3 ^
  /opt/retailpulse/spark/jobs/stream_orders.py
```

Run Spark linting:

```cmd
ruff check spark
```

Open the Spark Master UI:

```text
http://localhost:8081
```

## Session 3 Completion Checklist

- [x] Spark Master added
- [x] Spark Worker added
- [x] Spark UI accessible
- [x] Spark Worker registered
- [x] Spark Kafka connector configured
- [x] Ivy cache issue diagnosed and fixed
- [x] Streaming job created
- [x] Explicit order-event schema defined
- [x] Spark connected to Kafka
- [x] Kafka JSON parsed into typed columns
- [x] Kafka metadata retained
- [x] Multiple Kafka partitions observed
- [x] Kafka offsets understood
- [x] Continuous Spark micro-batches confirmed
- [x] Spark UI purpose clarified

## Session 4 Preview

The next session will introduce persistent stream outputs:

```text
Kafka
  ↓
Spark Structured Streaming
  ├── Bronze
  ├── Silver
  └── Quarantine
```

Planned focus:

- Write raw event data to Bronze storage
- Write validated and transformed data to Silver storage
- Route invalid records to Quarantine
- Add Spark checkpoint directories
- Test stream recovery after restart
- Introduce the first production-style data quality logic

**Session 3 status: Complete**

---

## Later Streaming Reconciliation Note — 12 August 2026

The Kafka metadata retained by the Session 3 Spark design proved important during final regression testing.

For the current clean lineage, Spark Bronze and Silver contained:

```text
partition 0 → offsets 0–42 → 43 rows
partition 1 → offsets 0–30 → 31 rows
partition 2 → offsets 0–33 → 34 rows
```

Total:

```text
43 + 31 + 34 = 108 rows
```

This exactly matched Kafka's broker end offsets:

```text
orders:0:43
orders:1:31
orders:2:34
```

Therefore the final regression demonstrated:

```text
Kafka
→ no offset gaps

Bronze
→ all 108 current-lineage messages present

Silver
→ all 108 valid messages present
```

The retained `partition` and `offset` fields were therefore useful not only for observability, but for end-to-end reconciliation.
