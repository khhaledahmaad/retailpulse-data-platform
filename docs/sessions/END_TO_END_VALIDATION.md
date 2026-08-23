# RetailPulse — End-to-End Validation Cheat Sheet

Use this after Sessions 1–7 or before starting a new session.

## 1. Services

```cmd
docker compose up -d
docker compose ps -a
```

## 2. Kafka topic and offsets

```cmd
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh ^
  --bootstrap-server localhost:9092 ^
  --list
```

```cmd
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh ^
  --bootstrap-server localhost:9092 ^
  --topic orders
```

## 3. Produce test events

```cmd
python producer\src\producer.py
```

## 4. Run lake stream

```cmd
docker compose exec spark-master /opt/spark/bin/spark-submit ^
  --master spark://spark-master:7077 ^
  --conf spark.jars.ivy=/tmp/.ivy2 ^
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3 ^
  /opt/retailpulse/spark/jobs/stream_orders_to_lake.py
```

## 5. Count lake layers

Stop the stream first if the only Spark worker is occupied.

```cmd
docker compose exec spark-master /opt/spark/bin/pyspark ^
  --master spark://spark-master:7077
```

```python
bronze = spark.read.parquet("/opt/retailpulse/data_lake/bronze/orders")
silver = spark.read.parquet("/opt/retailpulse/data_lake/silver/orders")
quarantine = spark.read.parquet("/opt/retailpulse/data_lake/quarantine/orders")

bronze.count()
silver.count()
quarantine.count()
```

## 6. Load Silver manually when validating outside Airflow

```cmd
python -m warehouse.loader.load_orders
```

## 7. Warehouse count

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "SELECT COUNT(*) FROM raw.orders;"
```

## 8. dbt build locally

```cmd
cd warehouse\dbt\retailpulse
dbt build --no-partial-parse --target dev
cd ..\..\..
```

## 9. Fact count

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "SELECT COUNT(*) FROM analytics.fct_orders;"
```

## 10. Gold reconciliation

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "SELECT SUM(order_count) FROM analytics.mart_daily_sales;"
```

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "SELECT * FROM analytics.mart_daily_sales ORDER BY order_date;"
```

## 11. Airflow checks

```cmd
docker compose exec airflow-scheduler airflow dags list
```

```cmd
docker compose exec airflow-scheduler airflow config get-value core executor
```

Then trigger:

```text
retailpulse_warehouse_pipeline
```

from:

```text
http://localhost:8083
```

## 12. Expected reconciliation for an all-valid clean lineage

```text
Kafka retained records
        ≈
Bronze rows
        =
Silver rows
        =
raw.orders rows
        =
fct_orders rows
        =
SUM(mart_daily_sales.order_count)

Quarantine rows = 0
```

If invalid test events are intentionally injected:

```text
Bronze = Silver + Quarantine
```

subject to the exact validation/write design used by the streaming job.
