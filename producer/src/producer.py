import argparse
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
        "order_id": str(uuid.uuid4()),
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


def produce_events(
    producer: KafkaProducer,
    *,
    count: int | None,
    interval: float,
    quiet: bool,
) -> int:
    produced = 0

    while count is None or produced < count:
        event = create_order_event()

        producer.send(
            TOPIC_NAME,
            key=event["order_id"],
            value=event,
        )

        produced += 1

        if not quiet:
            print(json.dumps(event, indent=2))

        if interval > 0 and (count is None or produced < count):
            time.sleep(interval)

    return produced


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Produce RetailPulse order events to Kafka."
    )

    parser.add_argument(
        "--count",
        type=int,
        default=None,
        help="Number of events to produce. Default: continuous.",
    )

    parser.add_argument(
        "--interval",
        type=float,
        default=2.0,
        help="Seconds between events. Default: 2.",
    )

    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Do not print every generated event.",
    )

    args = parser.parse_args()

    if args.count is not None and args.count <= 0:
        parser.error("--count must be greater than 0")

    if args.interval < 0:
        parser.error("--interval must be 0 or greater")

    return args


def main() -> None:
    args = parse_args()

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    print(f"Producing events to topic: {TOPIC_NAME}")

    produced = 0
    started_at = time.perf_counter()

    try:
        produced = produce_events(
            producer,
            count=args.count,
            interval=args.interval,
            quiet=args.quiet,
        )

    except KeyboardInterrupt:
        print("\nProducer stopped.")

    finally:
        producer.flush()
        producer.close()

    elapsed = time.perf_counter() - started_at

    if args.count is not None:
        rate = produced / elapsed if elapsed > 0 else 0

        print(
            f"Produced {produced} events in "
            f"{elapsed:.2f}s "
            f"({rate:.1f} events/s)"
        )


if __name__ == "__main__":
    main()
