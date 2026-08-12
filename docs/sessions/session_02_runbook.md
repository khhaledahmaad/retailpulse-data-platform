# RetailPulse — Session 2 Reproduction Runbook

**Goal:** Add Kafka, Kafka UI, the `orders` topic and a Windows Python producer.

## 1. Add dependencies

Add to `requirements-dev.txt`:

```text
kafka-python
```

Install:

```cmd
pip install -r requirements-dev.txt
```

## 2. Add Kafka and Kafka UI

Kafka image:

```text
apache/kafka:4.1.0
```

Kafka UI:

```text
provectuslabs/kafka-ui:latest
```

Kafka requires two client paths:

```text
Windows host → localhost:9092
Docker       → kafka:29092
```

Essential listener configuration:

```yaml
KAFKA_LISTENERS: EXTERNAL://:9092,INTERNAL://:29092,CONTROLLER://:9093
KAFKA_ADVERTISED_LISTENERS: EXTERNAL://localhost:9092,INTERNAL://kafka:29092
KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT,INTERNAL:PLAINTEXT
KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
```

Kafka UI connects to:

```text
kafka:29092
```

## 3. Start and validate services

```cmd
docker compose config
docker compose up -d
docker compose ps
```

Kafka UI:

```text
http://localhost:8080
```

## 4. Create the topic

List topics first:

```cmd
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh ^
  --bootstrap-server localhost:9092 ^
  --list
```

Create:

```cmd
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh ^
  --bootstrap-server localhost:9092 ^
  --create ^
  --topic orders ^
  --partitions 3 ^
  --replication-factor 1
```

Verify:

```cmd
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh ^
  --bootstrap-server localhost:9092 ^
  --describe ^
  --topic orders
```

Expected:

```text
PartitionCount: 3
```

## 5. Create the producer

File:

```text
producer/src/producer.py
```

Producer connection:

```python
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
```

Event fields:

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

Use `order_id` as the Kafka key.

## 6. Run and validate producer

```cmd
python producer\src\producer.py
```

Stop with:

```text
Ctrl+C
```

Lint:

```cmd
ruff check producer
```

## 7. Validate Kafka data

Check topic end offsets:

```cmd
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh ^
  --bootstrap-server localhost:9092 ^
  --topic orders
```

You should see three partitions with non-zero end offsets after producing events.

Optional console consumer:

```cmd
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh ^
  --bootstrap-server localhost:9092 ^
  --topic orders ^
  --from-beginning ^
  --max-messages 5
```

## Session 2 validation gate

```text
[ ] Kafka running
[ ] Kafka UI opens
[ ] `orders` topic exists
[ ] `orders` has 3 partitions
[ ] producer publishes events
[ ] Kafka offsets increase
[ ] events visible in Kafka UI/console
[ ] ruff passes
```
