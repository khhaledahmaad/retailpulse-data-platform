from collections.abc import Callable
from datetime import datetime
from typing import NamedTuple

SUPPORTED_EVENT_TYPES = {
    "order_created",
}

SUPPORTED_CURRENCIES = {
    "GBP",
}

SUPPORTED_CATEGORIES = {
    "electronics",
    "home",
    "fashion",
    "sports",
    "books",
}


class QualityRule(NamedTuple):
    name: str
    error_code: str
    validator: Callable[[dict], bool]


def is_non_empty_string(value) -> bool:
    return isinstance(value, str) and bool(value.strip())


def is_valid_event_timestamp(value) -> bool:
    if not isinstance(value, str):
        return False

    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False

    return True


def has_valid_event_id(event: dict) -> bool:
    return is_non_empty_string(event.get("event_id"))


def has_valid_order_id(event: dict) -> bool:
    return is_non_empty_string(event.get("order_id"))


def has_valid_product_id(event: dict) -> bool:
    return is_non_empty_string(event.get("product_id"))


def has_supported_event_type(
    event: dict,
) -> bool:
    return event.get("event_type") in SUPPORTED_EVENT_TYPES


def has_valid_timestamp(event: dict) -> bool:
    return is_valid_event_timestamp(event.get("event_timestamp"))


def has_valid_quantity(event: dict) -> bool:
    quantity = event.get("quantity")

    return isinstance(quantity, int) and not isinstance(quantity, bool) and quantity > 0


def has_valid_unit_price(event: dict) -> bool:
    unit_price = event.get("unit_price")

    return (
        isinstance(
            unit_price,
            (int, float),
        )
        and not isinstance(unit_price, bool)
        and unit_price > 0
    )


def has_supported_currency(
    event: dict,
) -> bool:
    return event.get("currency") in SUPPORTED_CURRENCIES


def has_supported_category(
    event: dict,
) -> bool:
    return event.get("category") in SUPPORTED_CATEGORIES


QUALITY_RULES = (
    QualityRule(
        name="event_id_required",
        error_code="missing_or_invalid_event_id",
        validator=has_valid_event_id,
    ),
    QualityRule(
        name="order_id_required",
        error_code="missing_order_id",
        validator=has_valid_order_id,
    ),
    QualityRule(
        name="product_id_required",
        error_code="missing_product_id",
        validator=has_valid_product_id,
    ),
    QualityRule(
        name="event_type_supported",
        error_code="unsupported_event_type",
        validator=has_supported_event_type,
    ),
    QualityRule(
        name="event_timestamp_valid",
        error_code="invalid_event_timestamp",
        validator=has_valid_timestamp,
    ),
    QualityRule(
        name="quantity_positive",
        error_code="invalid_quantity",
        validator=has_valid_quantity,
    ),
    QualityRule(
        name="unit_price_positive",
        error_code="invalid_unit_price",
        validator=has_valid_unit_price,
    ),
    QualityRule(
        name="currency_supported",
        error_code="unsupported_currency",
        validator=has_supported_currency,
    ),
    QualityRule(
        name="category_supported",
        error_code="unsupported_category",
        validator=has_supported_category,
    ),
)


def validate_event_quality(
    event: dict,
) -> str | None:
    for rule in QUALITY_RULES:
        if not rule.validator(event):
            return rule.error_code

    return None
