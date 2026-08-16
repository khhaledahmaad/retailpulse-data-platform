from importlib import import_module

from producer.src.producer import create_order_event


def get_contract_module():
    return import_module("spark.common.order_contract")


def test_producer_emits_schema_version_v1():
    event = create_order_event()

    assert event["schema_version"] == 1


def test_v1_contract_accepts_current_order_event():
    contract = get_contract_module()

    event = create_order_event()

    assert contract.validate_event_contract(event) is None


def test_contract_accepts_unknown_extra_field():
    contract = get_contract_module()

    event = create_order_event()
    event["promotion_code"] = "SUMMER26"

    assert contract.validate_event_contract(event) is None


def test_contract_rejects_missing_required_field():
    contract = get_contract_module()

    event = create_order_event()
    del event["order_id"]

    assert contract.validate_event_contract(event) == "contract_missing_order_id"


def test_contract_rejects_unsupported_schema_version():
    contract = get_contract_module()

    event = create_order_event()
    event["schema_version"] = 99

    assert (
        contract.validate_event_contract(event) == "contract_unsupported_schema_version"
    )
