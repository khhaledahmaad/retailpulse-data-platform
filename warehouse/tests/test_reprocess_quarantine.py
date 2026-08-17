import pytest

from warehouse.tools.reprocess_quarantine import (
    apply_corrections,
    parse_set_values,
    publish_repaired_event,
    record_reprocessing_attempt,
    validate_repaired_contract,
    validate_repaired_quality,
)

BASE_PAYLOAD = {
    "schema_version": 1,
    "event_id": "7fbec326-c180-41bd-8a7d-a24b0f35b68f",
    "event_type": "order_created",
    "event_timestamp": "2026-08-16T12:43:22.858263+00:00",
    "order_id": "ORD-DQ-BAD-QUANTITY",
    "customer_id": "CUS-4310",
    "product_id": "PRD-685",
    "category": "home",
    "quantity": 0,
    "unit_price": 149.35,
    "currency": "GBP",
}


def test_apply_corrections_preserves_event_identity():
    repaired = apply_corrections(
        BASE_PAYLOAD,
        {"quantity": 2},
    )

    assert repaired["quantity"] == 2
    assert repaired["event_id"] == BASE_PAYLOAD["event_id"]
    assert repaired["event_timestamp"] == BASE_PAYLOAD["event_timestamp"]

    # Original quarantine payload must remain unchanged.
    assert BASE_PAYLOAD["quantity"] == 0


def test_apply_corrections_rejects_event_id_change():
    with pytest.raises(
        ValueError,
        match="event_id",
    ):
        apply_corrections(
            BASE_PAYLOAD,
            {"event_id": "different-event-id"},
        )


def test_apply_corrections_rejects_event_timestamp_change():
    with pytest.raises(
        ValueError,
        match="event_timestamp",
    ):
        apply_corrections(
            BASE_PAYLOAD,
            {"event_timestamp": "2026-08-17T10:00:00+00:00"},
        )


def test_parse_set_values_preserves_scalar_types():
    corrections = parse_set_values(
        [
            "quantity=2",
            "unit_price=149.35",
            "category=home",
        ]
    )

    assert corrections == {
        "quantity": 2,
        "unit_price": 149.35,
        "category": "home",
    }


def test_validate_repaired_contract_accepts_valid_v1():
    repaired = apply_corrections(
        BASE_PAYLOAD,
        {"quantity": 2},
    )

    assert validate_repaired_contract(repaired) is None


def test_validate_repaired_contract_rejects_unsupported_version():
    payload = {
        **BASE_PAYLOAD,
        "schema_version": 99,
    }

    with pytest.raises(
        ValueError,
        match="contract_unsupported_schema_version",
    ):
        validate_repaired_contract(payload)


def test_validate_repaired_quality_accepts_valid_event():
    repaired = apply_corrections(
        BASE_PAYLOAD,
        {"quantity": 2},
    )

    assert validate_repaired_quality(repaired) is None


def test_validate_repaired_quality_rejects_zero_quantity():
    with pytest.raises(
        ValueError,
        match="invalid_quantity",
    ):
        validate_repaired_quality(BASE_PAYLOAD)


def test_validate_repaired_quality_rejects_negative_unit_price():
    payload = {
        **BASE_PAYLOAD,
        "quantity": 2,
        "unit_price": -1.0,
    }

    with pytest.raises(
        ValueError,
        match="invalid_unit_price",
    ):
        validate_repaired_quality(payload)


def test_validate_repaired_quality_rejects_unsupported_currency():
    payload = {
        **BASE_PAYLOAD,
        "quantity": 2,
        "currency": "USD",
    }

    with pytest.raises(
        ValueError,
        match="unsupported_currency",
    ):
        validate_repaired_quality(payload)


def test_validate_repaired_quality_rejects_bad_timestamp():
    payload = {
        **BASE_PAYLOAD,
        "quantity": 2,
        "event_timestamp": "not-a-timestamp",
    }

    with pytest.raises(
        ValueError,
        match="invalid_event_timestamp",
    ):
        validate_repaired_quality(payload)


def test_publish_repaired_event_preserves_payload_and_identity():
    captured = {}

    class FakeMetadata:
        topic = "orders"
        partition = 1
        offset = 123

    class FakeFuture:
        def get(self, timeout):
            assert timeout == 10
            return FakeMetadata()

    class FakeProducer:
        def __init__(self, **kwargs):
            captured["producer_kwargs"] = kwargs
            captured["flushed"] = False
            captured["closed"] = False

        def send(
            self,
            topic,
            key,
            value,
        ):
            captured["topic"] = topic
            captured["key"] = key
            captured["value"] = value

            return FakeFuture()

        def flush(self):
            captured["flushed"] = True

        def close(self):
            captured["closed"] = True

    repaired = apply_corrections(
        BASE_PAYLOAD,
        {"quantity": 2},
    )

    metadata = publish_repaired_event(
        repaired,
        producer_factory=FakeProducer,
    )

    assert captured["topic"] == "orders"
    assert captured["key"] == "ORD-DQ-BAD-QUANTITY"
    assert captured["value"] == repaired

    assert captured["value"]["event_id"] == BASE_PAYLOAD["event_id"]

    assert captured["flushed"] is True
    assert captured["closed"] is True

    assert metadata == {
        "topic": "orders",
        "partition": 1,
        "offset": 123,
    }


def test_record_reprocessing_attempt_writes_audit_row():
    captured = {}

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(
            self,
            exc_type,
            exc_value,
            traceback,
        ):
            return False

        def execute(
            self,
            sql,
            params,
        ):
            captured["sql"] = sql
            captured["params"] = params

        def fetchone(self):
            return (42,)

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            captured["committed"] = True

    record = {
        "payload": BASE_PAYLOAD,
        "contract_error": None,
        "validation_error": "invalid_quantity",
        "kafka_timestamp": "2026-08-16T12:43:22+00:00",
    }

    reprocessing_id = record_reprocessing_attempt(
        FakeConnection(),
        record=record,
        corrections={"quantity": 2},
        action="PUBLISH",
        status="PUBLISHED",
        publish_metadata={
            "topic": "orders",
            "partition": 0,
            "offset": 214,
        },
    )

    assert reprocessing_id == 42
    assert captured["committed"] is True

    params = captured["params"]

    assert params[0] == BASE_PAYLOAD["event_id"]
    assert params[1] == BASE_PAYLOAD["order_id"]
    assert params[3] == "invalid_quantity"
    assert params[6] == "PUBLISH"
    assert params[7] == "PUBLISHED"
    assert params[8] == "orders"
    assert params[9] == 0
    assert params[10] == 214


def test_record_reprocessing_attempt_supports_dry_run():
    captured = {}

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(
            self,
            exc_type,
            exc_value,
            traceback,
        ):
            return False

        def execute(
            self,
            sql,
            params,
        ):
            captured["params"] = params

        def fetchone(self):
            return (7,)

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            pass

    record = {
        "payload": BASE_PAYLOAD,
        "contract_error": None,
        "validation_error": "invalid_quantity",
        "kafka_timestamp": "2026-08-16T12:43:22+00:00",
    }

    reprocessing_id = record_reprocessing_attempt(
        FakeConnection(),
        record=record,
        corrections={"quantity": 2},
        action="DRY_RUN",
        status="DRY_RUN",
    )

    assert reprocessing_id == 7

    params = captured["params"]

    assert params[6] == "DRY_RUN"
    assert params[7] == "DRY_RUN"

    assert params[8] is None
    assert params[9] is None
    assert params[10] is None


def test_record_reprocessing_attempt_supports_publish_failure():
    captured = {}

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(
            self,
            exc_type,
            exc_value,
            traceback,
        ):
            return False

        def execute(
            self,
            sql,
            params,
        ):
            captured["params"] = params

        def fetchone(self):
            return (99,)

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            captured["committed"] = True

    record = {
        "payload": BASE_PAYLOAD,
        "contract_error": None,
        "validation_error": "invalid_quantity",
        "kafka_timestamp": "2026-08-16T12:43:22+00:00",
    }

    result = record_reprocessing_attempt(
        FakeConnection(),
        record=record,
        corrections={"quantity": 2},
        action="PUBLISH",
        status="PUBLISH_FAILED",
        error_message="Kafka unavailable",
    )

    assert result == 99
    assert captured["committed"] is True

    params = captured["params"]

    assert params[6] == "PUBLISH"
    assert params[7] == "PUBLISH_FAILED"

    assert params[8] is None
    assert params[9] is None
    assert params[10] is None

    assert params[11] == "Kafka unavailable"
