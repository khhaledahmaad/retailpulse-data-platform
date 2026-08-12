# RetailPulse — Session 4 Wrap-Up (Updated)

**Session:** 4  
**Focus:** Spark lakehouse persistence, Bronze/Silver/Quarantine layers, checkpoint recovery, and restart-safe Kafka/Spark state

## Session Goal

Move the Spark consumer from console-only output to persistent lakehouse-style storage with:

```text
Kafka
  ↓
Spark Structured Streaming
  ↓
Bronze
Silver
Quarantine
  ↓
Checkpoints
```

The session also established the recovery model for restarting streaming jobs safely.

---

## 1. Data Lake Layout

RetailPulse persists streaming output under:

```text
data_lake/
├── bronze/
├── silver/
├── quarantine/
└── checkpoints/
```

The Spark containers mount this host directory:

```yaml
- ./data_lake:/opt/retailpulse/data_lake
```

Because it is host-mounted, the data lake and Spark checkpoints survive container recreation.

---

## 2. Bronze Layer

Bronze stores the raw Kafka event payload plus ingestion metadata.

Typical fields include:

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

Bronze is append-only and preserves the source event faithfully.

---

## 3. Silver Layer

Silver parses and validates Bronze-compatible Kafka data into typed order records.

Important fields include:

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
order_value
kafka metadata
ingested_at
ingestion_date
ingestion_hour
```

Silver is partitioned by ingestion time:

```text
ingestion_date
ingestion_hour
```

This was chosen so late-arriving events are still discoverable in the current ingestion partition.

---

## 4. Quarantine Layer

Invalid records are written separately rather than crashing the stream.

Examples of validation failures:

```text
missing_or_invalid_event_id
missing_order_id
missing_product_id
invalid_event_timestamp
invalid_quantity
invalid_unit_price
unsupported_currency
```

This preserves bad records for later inspection.

---

## 5. Parquet Output

Spark writes:

```text
.snappy.parquet
```

files.

Associated `.crc` files are Hadoop checksum metadata and are normal.

Micro-batch streaming can create many small Parquet files. This is acceptable for the current portfolio scale; compaction can be introduced later if needed.

---

## 6. Checkpoints

Each streaming query has its own checkpoint directory:

```text
data_lake/checkpoints/bronze_orders
data_lake/checkpoints/silver_orders
data_lake/checkpoints/quarantine_orders
```

Spark checkpoints store streaming progress, including Kafka partition offsets already processed.

Checkpoint state is continuously updated while the query runs.

It is not only written when the stream stops.

---

## 7. `startingOffsets`

RetailPulse uses:

```python
.option("startingOffsets", "earliest")
```

This means:

```text
No existing checkpoint
→ read the earliest Kafka records still retained

Existing checkpoint
→ resume from checkpointed offsets
```

An existing checkpoint takes precedence over `startingOffsets`.

---

## 8. Recovery Model

When Spark stops but Kafka remains intact:

```text
Kafka keeps retained events
        ↓
Spark checkpoint remembers processed offsets
        ↓
Spark restarts
        ↓
continues from checkpoint
        ↓
catches up backlog
```

This is the normal recovery path.

---

# 9. Final Kafka Persistence Model

The original development setup persisted Spark checkpoints but did not persist Kafka broker storage.

That created a dangerous asymmetric state:

```text
Spark checkpoints
→ survived Docker recreation

Kafka broker data
→ could disappear with Kafka container recreation
```

This could produce an offset mismatch after:

```cmd
docker compose down
docker compose up -d
```

because Spark might remember offsets from an older Kafka broker state.

The final architecture now persists Kafka as well.

Kafka uses:

```yaml
volumes:
  - kafka_data:/tmp/kraft-combined-logs
```

with:

```yaml
KAFKA_LOG_DIRS: /tmp/kraft-combined-logs
```

and a fixed KRaft cluster ID.

Final state:

```text
Kafka topic/partition state
→ kafka_data named volume

Spark processed-offset state
→ data_lake/checkpoints/
```

Both sides now survive ordinary Compose recreation.

---

## 10. Kafka Volume Initialisation

Kafka runs as UID/GID 1000.

A small one-time `kafka-init` service ensures the persistent Kafka volume has the correct ownership before Kafka starts.

Conceptually:

```text
kafka_data volume
      ↓
kafka-init
      ↓
chown 1000:1000
      ↓
Kafka starts
```

This prevents write-permission errors in:

```text
/tmp/kraft-combined-logs
```

---

## 11. Restart-Safe Behaviour

With the final persistence setup:

```cmd
docker compose down
docker compose up -d
```

preserves:

```text
Kafka topic data
Kafka partition logs
Kafka offsets
Spark checkpoints
Bronze/Silver/Quarantine files
PostgreSQL data
Airflow metadata
```

Therefore Spark should normally resume from its existing checkpoints without manual checkpoint deletion.

---

## 12. When Checkpoints Should Be Deleted

Checkpoint deletion is not part of normal operation.

Only reset checkpoints when intentionally creating a new streaming lineage, for example:

```text
Kafka topic recreated from scratch
Kafka broker state intentionally reset
stream schema/query identity fundamentally changed
development data intentionally reset
```

In that case, delete the relevant checkpoint directories once and allow Spark to establish a new checkpoint history.

---

# 13. Clean Reset After Kafka Persistence Migration

During development, Kafka was migrated from a disposable broker to a new persistent `kafka_data` volume.

That produced a new `orders` topic with a new offset history.

For a clean portfolio dataset, the recommended one-time reset is:

```text
old Kafka topic lineage
→ discard old synthetic lake data

new persistent orders topic
→ rebuild Bronze/Silver/Quarantine from new events
```

Recommended directories to reset once:

```text
data_lake/bronze/orders
data_lake/silver/orders
data_lake/quarantine/orders

data_lake/checkpoints/bronze_orders
data_lake/checkpoints/silver_orders
data_lake/checkpoints/quarantine_orders
```

After the reset, keep these directories persistent going forward.

---

## 14. Why Reset Old Lake Data

Technically, old and new synthetic events could coexist because `event_id` values are UUIDs.

However, keeping them would mix two different Kafka histories:

```text
old disposable Kafka topic
+
new persistent Kafka topic
```

That makes lineage less clean.

For a portfolio project, a single coherent lineage is preferable:

```text
persistent Kafka orders topic
        ↓
Spark
        ↓
Bronze
        ↓
Silver / Quarantine
        ↓
warehouse
```

---

# 15. Final Session 4 Architecture

```text
Persistent Kafka `orders`
        ↓
Spark Structured Streaming
        ↓
┌─────────────┬─────────────┬─────────────┐
│   Bronze    │   Silver    │ Quarantine  │
└─────────────┴─────────────┴─────────────┘
        ↓
persistent Spark checkpoints
```

Persistence:

```text
Kafka
→ kafka_data

Spark lake
→ host-mounted data_lake/

Spark checkpoints
→ host-mounted data_lake/checkpoints/
```

This provides restart-safe streaming recovery.

---

## Session 4 Completion Checklist

- [x] Bronze Parquet persistence
- [x] Silver typed/validated layer
- [x] Quarantine invalid-record layer
- [x] independent streaming checkpoints
- [x] `startingOffsets="earliest"`
- [x] checkpoint recovery understood
- [x] Parquet/CRC behaviour understood
- [x] ingestion-time Silver partitioning
- [x] Kafka persistence added later to complete recovery model
- [x] fixed KRaft cluster identity
- [x] Kafka volume ownership initialisation
- [x] restart-safe Kafka + Spark checkpoint alignment
- [x] one-time clean reset guidance after Kafka lineage migration

**Session 4 status: Complete**

---

## Final Recovery and Data Reconciliation — 12 August 2026

The restart-safe Kafka/Spark model was regression-tested after the persistence changes.

### Current Persistence Contract

```text
Kafka broker state
→ named volume `kafka_data`

Spark Bronze/Silver/Quarantine
→ host-mounted `data_lake/`

Spark streaming progress
→ host-mounted `data_lake/checkpoints/`

PostgreSQL
→ named volume `postgres_data`

Airflow metadata
→ named volume `airflow_db_data`
```

After the one-time Kafka-lineage migration/reset, the normal rule is now:

```text
DO NOT delete Spark lake data or checkpoints
for ordinary docker compose down/up cycles.
```

Checkpoint deletion is reserved for an intentional source-lineage reset or incompatible streaming-query/schema change.

### Final Kafka ↔ Spark Reconciliation

Broker end offsets:

```text
orders:0:43
orders:1:31
orders:2:34
```

Spark Bronze:

```text
partition 0 → min 0, max 42, rows 43
partition 1 → min 0, max 30, rows 31
partition 2 → min 0, max 33, rows 34
```

Spark Silver produced the exact same result.

Final counts:

```text
Kafka broker records = 108
Bronze               = 108
Silver               = 108
Quarantine           =   0
```

This proves that the current lineage has no missing offsets between Kafka and Bronze and that all current producer events passed Silver validation.

### Recommended Reconciliation Commands

Kafka:

```cmd
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh ^
  --bootstrap-server localhost:9092 ^
  --topic orders
```

Spark:

```python
from pyspark.sql import functions as F

bronze.groupBy("partition").agg(
    F.min("offset").alias("min_offset"),
    F.max("offset").alias("max_offset"),
    F.count("*").alias("rows"),
).orderBy("partition").show()
```

The broker's end offset is the next offset, so for a partition starting at zero it also represents the number of records currently retained in that clean lineage.
