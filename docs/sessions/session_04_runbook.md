# RetailPulse — Session 4 Reproduction Runbook

**Goal:** Persist Kafka data into Bronze, Silver and Quarantine Parquet with independent Spark checkpoints.

## 1. Mount the data lake into Spark

Add to both Spark Master and Worker:

```yaml
- ./data_lake:/opt/retailpulse/data_lake
```

Restart/recreate services:

```cmd
docker compose up -d
docker compose ps
```

## 2. Create the persistent streaming job

File:

```text
spark/jobs/stream_orders_to_lake.py
```

Paths:

```text
/opt/retailpulse/data_lake/bronze/orders
/opt/retailpulse/data_lake/silver/orders
/opt/retailpulse/data_lake/quarantine/orders

/opt/retailpulse/data_lake/checkpoints/bronze_orders
/opt/retailpulse/data_lake/checkpoints/silver_orders
/opt/retailpulse/data_lake/checkpoints/quarantine_orders
```

Kafka first-run strategy:

```python
.option("startingOffsets", "earliest")
```

Silver validation includes invalid/missing IDs, timestamp, quantity, unit price and unsupported currency.

Silver derives:

```text
order_value = quantity × unit_price
```

Silver is partitioned by:

```text
ingestion_date
ingestion_hour
```

## 3. Run the persistent Spark stream

```cmd
docker compose exec spark-master /opt/spark/bin/spark-submit ^
  --master spark://spark-master:7077 ^
  --conf spark.jars.ivy=/tmp/.ivy2 ^
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3 ^
  /opt/retailpulse/spark/jobs/stream_orders_to_lake.py
```

## 4. Produce events

In another CMD:

```cmd
python producer\src\producer.py
```

Let several events flow, then stop the producer.

## 5. Validate files from Windows

```cmd
dir data_lake\bronze\orders /s
dir data_lake\silver\orders /s
dir data_lake\quarantine\orders /s
dir data_lake\checkpoints /s
```

Expected actual data files:

```text
*.snappy.parquet
```

CRC files are normal Hadoop checksum metadata.

## 6. Count Bronze / Silver / Quarantine with Spark

Stop the long-running Spark submit first if the single worker is fully occupied.

Launch PySpark:

```cmd
docker compose exec spark-master /opt/spark/bin/pyspark ^
  --master spark://spark-master:7077
```

Inside PySpark:

```python
bronze = spark.read.parquet(
    "/opt/retailpulse/data_lake/bronze/orders"
)

silver = spark.read.parquet(
    "/opt/retailpulse/data_lake/silver/orders"
)

quarantine = spark.read.parquet(
    "/opt/retailpulse/data_lake/quarantine/orders"
)

bronze.count()
silver.count()
quarantine.count()
```

Inspect schemas:

```python
bronze.printSchema()
silver.printSchema()
quarantine.printSchema()
```

Inspect sample rows:

```python
bronze.show(5, truncate=False)
silver.show(5, truncate=False)
quarantine.show(5, truncate=False)
```

## 7. Validate Kafka ↔ Bronze offsets

Kafka broker end offsets:

```cmd
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh ^
  --bootstrap-server localhost:9092 ^
  --topic orders
```

In PySpark:

```python
from pyspark.sql import functions as F

bronze.groupBy("partition").agg(
    F.min("offset").alias("min_offset"),
    F.max("offset").alias("max_offset"),
    F.count("*").alias("rows"),
).orderBy("partition").show()

silver.groupBy("partition").agg(
    F.min("offset").alias("min_offset"),
    F.max("offset").alias("max_offset"),
    F.count("*").alias("rows"),
).orderBy("partition").show()
```

For a clean lineage starting at offset 0:

```text
broker end offset N
should correspond to
Spark max offset N-1 and row count N
```

Exit PySpark:

```python
exit()
```

## 8. Validate restart/checkpoint behaviour

Restart the streaming job with the same checkpoint paths.

Produce more events.

Expected:

```text
old data is not reprocessed
new Kafka backlog is consumed
new Parquet files are appended
```

## Session 4 validation gate

```text
[ ] Bronze Parquet exists
[ ] Silver Parquet exists
[ ] Quarantine path/checkpoint exists
[ ] Bronze count can be queried
[ ] Silver count can be queried
[ ] invalid rows route to Quarantine when tested
[ ] Bronze/Silver partition offsets are inspectable
[ ] Spark restart resumes from checkpoints
[ ] new events append without replaying old events
```
