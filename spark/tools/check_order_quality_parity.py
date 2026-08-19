from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    lit,
    try_to_timestamp,
)

from spark.common.order_quality import (
    validate_event_quality,
)
from spark.jobs.stream_orders_to_lake import (
    add_validation,
)

BASE_EVENT = {
    "schema_version": 1,
    "event_id": "evt-001",
    "event_type": "order_created",
    "event_timestamp": "2026-08-19T12:00:00",
    "order_id": "ORD-100001",
    "customer_id": "CUS-1001",
    "product_id": "PRD-101",
    "category": "electronics",
    "quantity": 2,
    "unit_price": 25.50,
    "currency": "GBP",
}


def changed(**updates):
    event = BASE_EVENT.copy()
    event.update(updates)
    return event


CASES = {
    "valid": BASE_EVENT.copy(),
    "blank_event_id": changed(
        event_id=" ",
    ),
    "blank_order_id": changed(
        order_id="",
    ),
    "blank_product_id": changed(
        product_id=" ",
    ),
    "unsupported_event_type": changed(
        event_type="order_deleted",
    ),
    "invalid_timestamp": changed(
        event_timestamp="not-a-timestamp",
    ),
    "zero_quantity": changed(
        quantity=0,
    ),
    "zero_unit_price": changed(
        unit_price=0.0,
    ),
    "unsupported_currency": changed(
        currency="USD",
    ),
    "unsupported_category": changed(
        category="automotive",
    ),
}


def main():
    spark = (
        SparkSession.builder.appName("RetailPulseOrderQualityParity")
        .master("local[1]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    rows = [
        {
            "case_name": case_name,
            **event,
        }
        for case_name, event in CASES.items()
    ]

    df = (
        spark.createDataFrame(rows)
        .withColumn(
            "event_timestamp",
            try_to_timestamp("event_timestamp"),
        )
        .withColumn(
            "contract_error",
            lit(None).cast("string"),
        )
    )

    actual_rows = (
        add_validation(df)
        .select(
            "case_name",
            "validation_error",
        )
        .collect()
    )

    actual = {row["case_name"]: row["validation_error"] for row in actual_rows}

    expected = {
        case_name: validate_event_quality(event) for case_name, event in CASES.items()
    }

    failures = []

    for case_name in CASES:
        expected_error = expected[case_name]

        actual_error = actual[case_name]

        print(
            f"{case_name}: " f"canonical={expected_error!r}, " f"spark={actual_error!r}"
        )

        if actual_error != expected_error:
            failures.append(case_name)

    spark.stop()

    if failures:
        raise AssertionError("Quality parity failed for: " + ", ".join(failures))

    print()
    print(f"Quality parity PASS: " f"{len(CASES)}/{len(CASES)} cases")


if __name__ == "__main__":
    main()
