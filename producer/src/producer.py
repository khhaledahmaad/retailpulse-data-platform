import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "orders"
SCHEMA_VERSION = 1


def create_order_event() -> dict:
    quantity = random.randint(1, 5)
    unit_price = round(random.uniform(5.0, 150.0), 2)

    return {
        "schema_version": SCHEMA_VERSION,
        "event_id": str(uuid.uuid4()),
        "event_type": "order_created",
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "order_id": f"ORD-{random.randint(100000, 999999)}",
        "customer_id": f"CUS-{random.randint(1000, 9999)}",
        "product_id": f"PRD-{random.randint(100, 999)}",
        "category": random.choice(
            [
                "electronics",
                "home",
                "fashion",
                "sports",
                "books",
            ]
        ),
        "quantity": quantity,
        "unit_price": unit_price,
        "currency": "GBP",
    }


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    print(f"Producing events to topic: {TOPIC_NAME}")

    try:
        while True:
            event = create_order_event()

            producer.send(
                TOPIC_NAME,
                key=event["order_id"],
                value=event,
            )

            producer.flush()

            print(json.dumps(event, indent=2))

            time.sleep(2)

    except KeyboardInterrupt:
        print("\nProducer stopped.")

    finally:
        producer.close()


if __name__ == "__main__":
    main()
