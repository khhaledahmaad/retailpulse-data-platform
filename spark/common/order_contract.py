CURRENT_SCHEMA_VERSION = 1

SUPPORTED_SCHEMA_VERSIONS = {
    CURRENT_SCHEMA_VERSION,
}

V1_REQUIRED_FIELDS = (
    "event_id",
    "event_type",
    "event_timestamp",
    "order_id",
    "product_id",
    "category",
    "quantity",
    "unit_price",
    "currency",
)


def validate_event_contract(
    event: dict,
) -> str | None:
    if not isinstance(event, dict):
        return "contract_invalid_payload"

    schema_version = event.get("schema_version")

    if schema_version is None:
        return "contract_missing_schema_version"

    if schema_version not in SUPPORTED_SCHEMA_VERSIONS:
        return "contract_unsupported_schema_version"

    for field in V1_REQUIRED_FIELDS:
        if field not in event or event[field] is None:
            return f"contract_missing_{field}"

    return None
