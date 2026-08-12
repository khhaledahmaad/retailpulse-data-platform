# RetailPulse — Session 3 Reproduction Runbook

**Goal:** Add Spark Master/Worker and consume Kafka as a typed Structured Streaming DataFrame.

## 1. Add Spark services

Image:

```text
apache/spark:4.1.3-python3
```

Services:

```text
spark-master
spark-worker
```

Important ports:

```text
Spark Master RPC → 7077
Spark UI         → host 8081 → container 8080
```

Worker connects to:

```text
spark://spark-master:7077
```

Mount project Spark code:

```yaml
- ./spark:/opt/retailpulse/spark
```

## 2. Start and validate Spark

```cmd
docker compose up -d
docker compose ps
```

Open:

```text
http://localhost:8081
```

Confirm at least one registered worker.

## 3. Create the console streaming job

File:

```text
spark/jobs/stream_orders.py
```

Kafka source:

```text
kafka:29092
```

Use an explicit order schema and retain:

```text
kafka_key
topic
partition
offset
kafka_timestamp
```

## 4. Submit the job

```cmd
docker compose exec spark-master /opt/spark/bin/spark-submit ^
  --master spark://spark-master:7077 ^
  --conf spark.jars.ivy=/tmp/.ivy2 ^
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3 ^
  /opt/retailpulse/spark/jobs/stream_orders.py
```

The Ivy override is required because the image's default cache path is not usable.

## 5. Produce events in another CMD

```cmd
python producer\src\producer.py
```

The Spark terminal should display parsed streaming rows.

## 6. Validate partition and offset flow

Kafka:

```cmd
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh ^
  --bootstrap-server localhost:9092 ^
  --topic orders
```

Spark output should show:

```text
partition
offset
```

Remember:

```text
offset is per partition, not global
```

## 7. Lint

```cmd
ruff check spark
```

## Session 3 validation gate

```text
[ ] Spark Master UI opens
[ ] worker registered
[ ] Spark downloads Kafka connector
[ ] stream connects to kafka:29092
[ ] producer events appear in Spark terminal
[ ] JSON parsed into typed fields
[ ] Kafka partition/offset metadata retained
[ ] ruff passes
```
