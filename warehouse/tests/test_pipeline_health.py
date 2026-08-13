from datetime import datetime, timedelta, timezone

from warehouse.monitoring.check_pipeline_health import evaluate_health


def test_health_is_healthy_when_all_layers_reconcile():
    latest_load = datetime.now(timezone.utc)

    result = evaluate_health(
        bronze_rows=127,
        silver_rows=127,
        quarantine_rows=0,
        raw_orders=127,
        fact_orders=127,
        gold_order_count=127,
        latest_loaded_at=latest_load,
    )

    assert result["status"] == "HEALTHY"
    assert result["issues"] == []


def test_health_detects_bronze_reconciliation_failure():
    latest_load = datetime.now(timezone.utc)

    result = evaluate_health(
        bronze_rows=130,
        silver_rows=127,
        quarantine_rows=0,
        raw_orders=127,
        fact_orders=127,
        gold_order_count=127,
        latest_loaded_at=latest_load,
    )

    assert result["status"] == "DEGRADED"
    assert any("Bronze does not reconcile" in issue for issue in result["issues"])


def test_health_detects_silver_raw_mismatch():
    latest_load = datetime.now(timezone.utc)

    result = evaluate_health(
        bronze_rows=128,
        silver_rows=128,
        quarantine_rows=0,
        raw_orders=127,
        fact_orders=127,
        gold_order_count=127,
        latest_loaded_at=latest_load,
    )

    assert result["status"] == "DEGRADED"
    assert any("Silver does not reconcile" in issue for issue in result["issues"])


def test_health_detects_raw_fact_mismatch():
    latest_load = datetime.now(timezone.utc)

    result = evaluate_health(
        bronze_rows=127,
        silver_rows=127,
        quarantine_rows=0,
        raw_orders=127,
        fact_orders=126,
        gold_order_count=126,
        latest_loaded_at=latest_load,
    )

    assert result["status"] == "DEGRADED"
    assert any("raw.orders does not reconcile" in issue for issue in result["issues"])


def test_health_detects_fact_gold_mismatch():
    latest_load = datetime.now(timezone.utc)

    result = evaluate_health(
        bronze_rows=127,
        silver_rows=127,
        quarantine_rows=0,
        raw_orders=127,
        fact_orders=127,
        gold_order_count=126,
        latest_loaded_at=latest_load,
    )

    assert result["status"] == "DEGRADED"
    assert any("fct_orders does not reconcile" in issue for issue in result["issues"])


def test_health_detects_stale_warehouse_data():
    stale_load = datetime.now(timezone.utc) - timedelta(minutes=3000)

    result = evaluate_health(
        bronze_rows=127,
        silver_rows=127,
        quarantine_rows=0,
        raw_orders=127,
        fact_orders=127,
        gold_order_count=127,
        latest_loaded_at=stale_load,
    )

    assert result["status"] == "DEGRADED"
    assert any("Warehouse data is stale" in issue for issue in result["issues"])


def test_health_detects_missing_load_timestamp():
    result = evaluate_health(
        bronze_rows=127,
        silver_rows=127,
        quarantine_rows=0,
        raw_orders=127,
        fact_orders=127,
        gold_order_count=127,
        latest_loaded_at=None,
    )

    assert result["status"] == "DEGRADED"
    assert any("No warehouse load timestamp" in issue for issue in result["issues"])
