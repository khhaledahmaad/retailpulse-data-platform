# RetailPulse — Session 2 Wrap-Up

**Date:** 8 August 2026  
**Session:** 2 of 30  
**Focus:** Kafka infrastructure and Python event production

## Session Goal

Extend the RetailPulse platform from a static project foundation into its first working event-driven pipeline:

```text
Python Producer
      ↓
Apache Kafka
      ↓
orders topic
      ↓
Kafka UI
```

## What Was Completed

### 1. Added Kafka to Docker Compose

The Docker Compose stack was expanded from only PostgreSQL to include:

```text
PostgreSQL
Apache Kafka
Kafka UI
```

Kafka is running in a Docker container and acts as the event broker for the platform.

### 2. Created the `orders` Topic

Created the first Kafka topic:

```text
orders
```

The topic was configured with three partitions:

```text
orders
├── partition 0
├── partition 1
└── partition 2
```

This introduces the idea that Kafka topics can distribute events across partitions for scalability and parallel processing.

### 3. Added the Python Kafka Client

Added:

```text
kafka-python
```

to the project development requirements.

The Python producer uses `KafkaProducer` to connect to Kafka from the Windows host.

### 4. Built the First Event Producer

Created:

```text
producer/src/producer.py
```

The producer generates synthetic retail order events containing fields such as:

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

Example event structure:

```json
{
  "event_id": "example-uuid",
  "event_type": "order_created",
  "event_timestamp": "2026-08-08T12:30:00+00:00",
  "order_id": "ORD-123456",
  "customer_id": "CUS-1234",
  "product_id": "PRD-123",
  "category": "electronics",
  "quantity": 2,
  "unit_price": 49.99,
  "currency": "GBP"
}
```

### 5. Kafka Message Keys

Each event is sent using:

```text
order_id
```

as the Kafka message key.

Conceptually:

```text
key   = ORD-123456
value = complete JSON order event
```

Using a stable key is important because related events can later be routed consistently to the same Kafka partition.

### 6. Verified Event Production

Ran:

```cmd
python producer\src\producer.py
```

The producer successfully connected to Kafka at:

```text
localhost:9092
```

and generated order events continuously.

### 7. Ruff Code Quality Check

Validated the producer using:

```cmd
ruff check producer
```

The final result was:

```text
All checks passed!
```

This confirms that the first producer implementation meets the current Python linting rules.

## Kafka UI Networking Issue

The first Kafka UI configuration did not work correctly.

The web application opened, but Kafka cluster information remained stuck on a loading indicator.

### Original Problem

Kafka originally advertised:

```text
localhost:9092
```

to every Kafka client.

This worked for the Python producer because the producer runs directly on Windows.

```text
Windows Python
      ↓
localhost:9092
      ↓
Kafka container
```

However, Kafka UI runs inside its own Docker container.

Inside that container:

```text
localhost
```

means:

```text
the Kafka UI container itself
```

and not the Kafka container.

The failure path was therefore:

```text
Kafka UI
   ↓
initial connection to Kafka
   ↓
Kafka advertises localhost:9092
   ↓
Kafka UI tries localhost:9092
   ↓
looks inside its own container
   ↓
connection fails
```

## Final Kafka Listener Design

Kafka was updated to use two different listeners.

### External Listener

Used by applications running on the Windows host:

```text
EXTERNAL://localhost:9092
```

The Python producer uses:

```python
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
```

### Internal Listener

Used by services running inside Docker:

```text
INTERNAL://kafka:29092
```

Kafka UI now connects to:

```text
kafka:29092
```

The working architecture is:

```text
                        Windows Host

                       Python Producer
                             |
                      localhost:9092
                             |
                             v
                    +----------------+
                    |     Kafka      |
                    |                |
                    | External :9092 |
                    | Internal :29092|
                    +--------+-------+
                             |
                       kafka:29092
                             |
                             v
                       +----------+
                       | Kafka UI |
                       +----------+

                    Docker Network
```

## Key Concept: Docker Service Names

Docker Compose creates an internal network for its services.

Services can reach each other using their service names:

```text
kafka
postgres
kafka-ui
```

For example:

```text
kafka:29092
```

means:

```text
connect to the service named `kafka`
on port 29092
```

This is different from accessing a container from the Windows host, where mapped ports such as:

```text
localhost:9092
```

are used.

## Key Concept: Kafka Advertised Listeners

Kafka clients initially connect to a bootstrap server.

Kafka then returns broker metadata containing the broker address the client should use afterwards.

That address is controlled by:

```text
KAFKA_ADVERTISED_LISTENERS
```

This is why simply giving Kafka UI the correct bootstrap address was not enough when Kafka later advertised an address that was only valid from the Windows host.

## Current Platform Architecture

At the end of Session 2:

```text
                  RetailPulse
                       |
             +---------+---------+
             |                   |
        PostgreSQL            Kafka
                                 |
                     +-----------+-----------+
                     |                       |
              Python Producer            Kafka UI
                     |
                order events
                     |
                orders topic
```

The project now has its first genuine moving-data pipeline.

## Useful Commands

Start the platform:

```cmd
docker compose up -d
```

Check service status:

```cmd
docker compose ps
```

List Kafka topics:

```cmd
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

Describe the `orders` topic:

```cmd
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --describe --topic orders --bootstrap-server localhost:9092
```

Run the Python producer:

```cmd
python producer\src\producer.py
```

Run Ruff:

```cmd
ruff check producer
```

Open Kafka UI:

```text
http://localhost:8080
```

## Session 2 Completion Checklist

- [x] Kafka added to Docker Compose
- [x] Kafka container started successfully
- [x] Kafka UI added
- [x] `orders` topic created
- [x] Topic configured with three partitions
- [x] `kafka-python` installed
- [x] Python order-event producer created
- [x] Producer successfully published events
- [x] Events visible in Kafka UI
- [x] Ruff checks passed
- [x] Kafka Docker networking issue diagnosed
- [x] External and internal Kafka listeners configured correctly

## Session 3 Preview

The next session will begin the stream-processing layer:

```text
Kafka
  ↓
Spark Structured Streaming
  ↓
parsed order events
```

Planned focus:

- Add Spark to Docker Compose
- Connect Spark to Kafka
- Read from the `orders` topic
- Define an explicit order-event schema
- Parse Kafka JSON messages
- Inspect the first streaming DataFrame

**Session 2 status: Complete**

---

## Later Kafka Persistence and Reconciliation Note — 12 August 2026

The original Session 2 Kafka networking model remained correct:

```text
Windows producer
→ localhost:9092

Docker services
→ kafka:29092
```

A later recovery test identified that Kafka broker data itself also needed persistence. The final platform therefore adds a named Kafka volume and a fixed KRaft cluster identity.

Final Kafka persistence model:

```text
kafka_data
→ /tmp/kraft-combined-logs

CLUSTER_ID
→ fixed value

kafka-init
→ initialises/chowns the Kafka log volume
```

This allows ordinary:

```cmd
docker compose down
docker compose up -d
```

to preserve the `orders` topic and its offsets.

### Final Topic Reconciliation

For the current clean Kafka lineage, the authoritative broker end offsets were:

```text
orders:0:43
orders:1:31
orders:2:34
```

Total records:

```text
43 + 31 + 34 = 108
```

The Kafka UI displayed a separate `117 messages consumed` value during inspection. That UI value was not used as the authoritative topic-row count; broker partition end offsets are the reconciliation source of truth.

Useful command:

```cmd
docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh ^
  --bootstrap-server localhost:9092 ^
  --topic orders
```
