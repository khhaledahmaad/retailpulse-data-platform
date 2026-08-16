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
        max_lag_rows=5,
    )

    assert result["status"] == "HEALTHY"
    assert result["issues"] == []


def test_health_warns_when_bronze_gap_is_within_tolerance():
    latest_load = datetime.now(timezone.utc)

    result = evaluate_health(
        bronze_rows=130,
        silver_rows=127,
        quarantine_rows=0,
        raw_orders=127,
        fact_orders=127,
        gold_order_count=127,
        latest_loaded_at=latest_load,
        max_lag_rows=5,
    )

    assert result["status"] == "WARNING"
    assert any("Bronze" in issue for issue in result["issues"])


def test_health_degrades_when_bronze_gap_exceeds_tolerance():
    latest_load = datetime.now(timezone.utc)

    result = evaluate_health(
        bronze_rows=133,
        silver_rows=127,
        quarantine_rows=0,
        raw_orders=127,
        fact_orders=127,
        gold_order_count=127,
        latest_loaded_at=latest_load,
        max_lag_rows=5,
    )

    assert result["status"] == "DEGRADED"
    assert any("Bronze" in issue for issue in result["issues"])


def test_health_warns_when_silver_leads_raw_within_tolerance():
    latest_load = datetime.now(timezone.utc)

    result = evaluate_health(
        bronze_rows=130,
        silver_rows=130,
        quarantine_rows=0,
        raw_orders=127,
        fact_orders=127,
        gold_order_count=127,
        latest_loaded_at=latest_load,
        max_lag_rows=5,
    )

    assert result["status"] == "WARNING"
    assert any("Silver" in issue for issue in result["issues"])


def test_health_degrades_when_silver_raw_gap_exceeds_tolerance():
    latest_load = datetime.now(timezone.utc)

    result = evaluate_health(
        bronze_rows=133,
        silver_rows=133,
        quarantine_rows=0,
        raw_orders=127,
        fact_orders=127,
        gold_order_count=127,
        latest_loaded_at=latest_load,
        max_lag_rows=5,
    )

    assert result["status"] == "DEGRADED"
    assert any("Silver" in issue for issue in result["issues"])


def test_health_degrades_when_raw_is_ahead_of_silver():
    latest_load = datetime.now(timezone.utc)

    result = evaluate_health(
        bronze_rows=127,
        silver_rows=127,
        quarantine_rows=0,
        raw_orders=128,
        fact_orders=128,
        gold_order_count=128,
        latest_loaded_at=latest_load,
        max_lag_rows=5,
    )

    assert result["status"] == "DEGRADED"
    assert any("Silver" in issue for issue in result["issues"])


def test_strict_health_requires_exact_reconciliation():
    latest_load = datetime.now(timezone.utc)

    result = evaluate_health(
        bronze_rows=130,
        silver_rows=127,
        quarantine_rows=0,
        raw_orders=127,
        fact_orders=127,
        gold_order_count=127,
        latest_loaded_at=latest_load,
        max_lag_rows=5,
        strict=True,
    )

    assert result["status"] == "DEGRADED"


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
        max_lag_rows=5,
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
        max_lag_rows=5,
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
        max_lag_rows=5,
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
        max_lag_rows=5,
    )

    assert result["status"] == "DEGRADED"
    assert any("No warehouse load timestamp" in issue for issue in result["issues"])


def test_health_allows_duplicate_silver_delivery_when_business_state_reconciles():
    latest_load = datetime.now(timezone.utc)
    health = evaluate_health(
        bronze_rows=653,
        silver_rows=649,
        silver_unique_events=648,
        quarantine_rows=4,
        raw_orders=648,
        fact_orders=648,
        gold_order_count=648,
        latest_loaded_at=latest_load,
        strict=True,
    )

    assert health["status"] == "HEALTHY"


def test_health_detects_missing_logical_silver_event_in_raw():
    latest_load = datetime.now(timezone.utc)
    health = evaluate_health(
        bronze_rows=653,
        silver_rows=649,
        silver_unique_events=649,
        quarantine_rows=4,
        raw_orders=648,
        fact_orders=648,
        gold_order_count=648,
        latest_loaded_at=latest_load,
        strict=True,
    )

    assert health["status"] == "DEGRADED"
