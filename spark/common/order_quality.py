from datetime import datetime


def is_valid_event_timestamp(value) -> bool:
    if not isinstance(value, str):
        return False

    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False

    return True


def validate_event_quality(
    event: dict,
) -> str | None:
    if event.get("event_id") is None:
        return "missing_or_invalid_event_id"

    if event.get("order_id") is None:
        return "missing_order_id"

    if event.get("product_id") is None:
        return "missing_product_id"

    if not is_valid_event_timestamp(event.get("event_timestamp")):
        return "invalid_event_timestamp"

    quantity = event.get("quantity")

    if not isinstance(quantity, int) or isinstance(quantity, bool) or quantity <= 0:
        return "invalid_quantity"

    unit_price = event.get("unit_price")

    if (
        not isinstance(unit_price, (int, float))
        or isinstance(unit_price, bool)
        or unit_price < 0
    ):
        return "invalid_unit_price"

    if event.get("currency") != "GBP":
        return "unsupported_currency"

    return None
