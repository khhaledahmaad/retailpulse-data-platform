import argparse
import copy
import json
import os
from pathlib import Path

import pyarrow.parquet as pq
from kafka import KafkaProducer
from kafka.serializer import (
    DefaultSerializer,
    JsonSerializer,
)

from spark.common.order_contract import validate_event_contract
from spark.common.order_quality import validate_event_quality

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

TOPIC_NAME = "orders"

QUARANTINE_ROOT = Path("data_lake/quarantine/orders")

PROTECTED_FIELDS = {
    "event_id",
    "event_timestamp",
}

def validate_repaired_contract(payload):
    error = validate_event_contract(payload)

    if error is not None:
        raise ValueError("Repaired payload failed contract " f"validation: {error}")


def validate_repaired_quality(payload):
    error = validate_event_quality(payload)

    if error is not None:
        raise ValueError("Repaired payload failed data-quality " f"validation: {error}")


def discover_quarantine_files():
    if not QUARANTINE_ROOT.exists():
        return []

    return sorted(
        path
        for path in QUARANTINE_ROOT.rglob("*.parquet")
        if "_spark_metadata" not in path.parts
    )


def find_quarantined_event(event_id):
    matches = []

    for path in discover_quarantine_files():
        table = pq.ParquetFile(path).read(
            columns=[
                "raw_payload",
                "contract_error",
                "validation_error",
                "kafka_timestamp",
            ]
        )

        for row in table.to_pylist():
            raw_payload = row["raw_payload"]

            try:
                payload = json.loads(raw_payload)
            except json.JSONDecodeError:
                continue

            if payload.get("event_id") == event_id:
                matches.append(
                    {
                        "payload": payload,
                        "contract_error": row["contract_error"],
                        "validation_error": row["validation_error"],
                        "kafka_timestamp": row["kafka_timestamp"],
                    }
                )

    if not matches:
        raise ValueError(f"No quarantined event found " f"for event_id={event_id}")

    if len(matches) > 1:
        raise ValueError(
            f"Multiple quarantined records found " f"for event_id={event_id}"
        )

    return matches[0]


def parse_set_values(values):
    corrections = {}

    for item in values:
        if "=" not in item:
            raise ValueError(f"Invalid correction '{item}'. " "Expected FIELD=VALUE.")

        field, raw_value = item.split("=", 1)

        field = field.strip()

        if not field:
            raise ValueError("Correction field cannot be empty")

        raw_value = raw_value.strip()

        try:
            value = json.loads(raw_value)
        except json.JSONDecodeError:
            value = raw_value

        corrections[field] = value

    return corrections


def apply_corrections(
    payload,
    corrections,
):
    repaired = copy.deepcopy(payload)

    for field, value in corrections.items():
        if field in PROTECTED_FIELDS and value != payload.get(field):
            raise ValueError(
                f"{field} cannot be changed " "during quarantine remediation"
            )

        repaired[field] = value

    return repaired


def publish_repaired_event(
    payload,
    *,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    topic=TOPIC_NAME,
    producer_factory=KafkaProducer,
):
    producer = producer_factory(
        bootstrap_servers=bootstrap_servers,
        key_serializer=DefaultSerializer(),
        value_serializer=JsonSerializer(),
    )

    try:
        future = producer.send(
            topic,
            key=payload["order_id"],
            value=payload,
        )

        metadata = future.get(timeout=10)

        producer.flush()

        return {
            "topic": metadata.topic,
            "partition": metadata.partition,
            "offset": metadata.offset,
        }

    finally:
        producer.close()


def parse_args():
    parser = argparse.ArgumentParser(
        description=("Inspect and repair a quarantined " "RetailPulse event")
    )

    parser.add_argument(
        "--event-id",
        required=True,
    )

    parser.add_argument(
        "--set",
        action="append",
        default=[],
        dest="set_values",
        help="Correction in FIELD=VALUE form",
    )

    parser.add_argument(
        "--publish",
        action="store_true",
        help=(
            "Publish the repaired event to Kafka. "
            "Without this flag the command is a dry run."
        ),
    )

    return parser.parse_args()


def main():
    args = parse_args()

    record = find_quarantined_event(args.event_id)

    corrections = parse_set_values(args.set_values)

    original = record["payload"]

    repaired = apply_corrections(
        original,
        corrections,
    )

    validate_repaired_contract(repaired)
    validate_repaired_quality(repaired)

    print()
    print("Contract validation: PASS")
    print("Data-quality validation: PASS")

    print()
    print("RetailPulse Quarantine Remediation")
    print("---------------------------------")
    print(f"Event ID:          {original.get('event_id')}")
    print(f"Order ID:          {original.get('order_id')}")
    print("Contract error:    " f"{record['contract_error']}")
    print("Validation error:  " f"{record['validation_error']}")
    print("Kafka timestamp:   " f"{record['kafka_timestamp']}")

    print()
    print("Corrections:")

    if not corrections:
        print("- none")
    else:
        for field, new_value in corrections.items():
            old_value = original.get(field)

            print(f"- {field}: " f"{old_value!r} -> " f"{new_value!r}")

    print()
    print("Repaired payload:")
    print(
        json.dumps(
            repaired,
            indent=2,
            sort_keys=True,
        )
    )

    if not args.publish:
        print()
        print("DRY RUN")
        print("Nothing published.")
        return

    metadata = publish_repaired_event(repaired)

    print()
    print("PUBLISHED")
    print(f"Topic:     {metadata['topic']}")
    print(f"Partition: {metadata['partition']}")
    print(f"Offset:    {metadata['offset']}")


if __name__ == "__main__":
    main()
