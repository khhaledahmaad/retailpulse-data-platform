from producer.src.producer import (
    create_order_event,
)
from spark.common.order_quality import (
    QUALITY_RULES,
    validate_event_quality,
)


def test_current_order_event_passes_quality():
    event = create_order_event()

    assert validate_event_quality(event) is None


def test_quality_rules_have_unique_names():
    names = [rule.name for rule in QUALITY_RULES]

    assert len(names) == len(set(names))


def test_quality_rules_have_unique_error_codes():
    error_codes = [rule.error_code for rule in QUALITY_RULES]

    assert len(error_codes) == len(set(error_codes))


def test_rejects_blank_event_id():
    event = create_order_event()
    event["event_id"] = " "

    assert validate_event_quality(event) == "missing_or_invalid_event_id"


def test_rejects_blank_order_id():
    event = create_order_event()
    event["order_id"] = ""

    assert validate_event_quality(event) == "missing_order_id"


def test_rejects_unsupported_event_type():
    event = create_order_event()
    event["event_type"] = "order_deleted"

    assert validate_event_quality(event) == "unsupported_event_type"


def test_rejects_zero_quantity():
    event = create_order_event()
    event["quantity"] = 0

    assert validate_event_quality(event) == "invalid_quantity"


def test_rejects_zero_unit_price():
    event = create_order_event()
    event["unit_price"] = 0

    assert validate_event_quality(event) == "invalid_unit_price"


def test_rejects_unsupported_currency():
    event = create_order_event()
    event["currency"] = "USD"

    assert validate_event_quality(event) == "unsupported_currency"


def test_rejects_unsupported_category():
    event = create_order_event()
    event["category"] = "automotive"

    assert validate_event_quality(event) == "unsupported_category"


def test_rejects_invalid_timestamp():
    event = create_order_event()
    event["event_timestamp"] = "not-a-timestamp"

    assert validate_event_quality(event) == "invalid_event_timestamp"
