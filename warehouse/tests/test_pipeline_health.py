from datetime import datetime, timedelta, timezone

import warehouse.monitoring.check_pipeline_health as health_module
from warehouse.monitoring.check_pipeline_health import (
    evaluate_health,
    reconcile_incidents,
)


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
    assert result["incident_types"] == []


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
    assert result["incident_types"] == ["BRONZE_RECONCILIATION"]
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
    assert result["incident_types"] == ["BRONZE_RECONCILIATION"]
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
    assert result["incident_types"] == ["SILVER_RAW_RECONCILIATION"]
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
    assert result["incident_types"] == ["SILVER_RAW_RECONCILIATION"]
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
    assert result["incident_types"] == ["SILVER_RAW_RECONCILIATION"]
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
    assert result["incident_types"] == ["BRONZE_RECONCILIATION"]


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
    assert result["incident_types"] == ["RAW_FACT_RECONCILIATION"]
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
    assert result["incident_types"] == ["FACT_GOLD_RECONCILIATION"]
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
    assert result["incident_types"] == ["WAREHOUSE_FRESHNESS"]
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
    assert result["incident_types"] == ["WAREHOUSE_FRESHNESS"]
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
    assert health["incident_types"] == []


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
    assert health["incident_types"] == ["SILVER_RAW_RECONCILIATION"]


def test_reconcile_incidents_opens_new_incident(
    monkeypatch,
):
    executed = []

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
            params=None,
        ):
            executed.append((sql, params))

        def fetchall(self):
            return []

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            executed.append(("COMMIT", None))

    health = {
        "status": "DEGRADED",
        "issues": ["raw.orders does not reconcile with fct_orders: 127 != 126"],
        "incident_types": ["RAW_FACT_RECONCILIATION"],
    }

    monkeypatch.setattr(
        health_module,
        "send_incident_alert",
        lambda **kwargs: None,
    )

    reconcile_incidents(
        FakeConnection(),
        health,
        airflow_run_id="test_run_001",
    )

    params = [
        params
        for sql, params in executed
        if "INSERT INTO control.pipeline_incidents" in sql
    ]

    assert params == [
        (
            "RAW_FACT_RECONCILIATION",
            "DEGRADED",
            "raw.orders does not reconcile with fct_orders: 127 != 126",
            "test_run_001",
        )
    ]


def test_reconcile_incidents_updates_existing_incident():
    executed = []

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
            params=None,
        ):
            executed.append((sql, params))

        def fetchall(self):
            return [
                (1, "BRONZE_RECONCILIATION")
            ]

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            pass

    health = {
        "status": "WARNING",
        "issues": ["Bronze does not reconcile"],
        "incident_types": ["BRONZE_RECONCILIATION"],
    }

    reconcile_incidents(
        FakeConnection(),
        health,
    )

    inserts = [
        sql for sql, _ in executed if "INSERT INTO control.pipeline_incidents" in sql
    ]

    updates = [
        params
        for sql, params in executed
        if ("UPDATE control.pipeline_incidents" in sql and "severity = %s" in sql)
    ]

    assert inserts == []

    assert updates == [
        (
            "WARNING",
            "Bronze does not reconcile",
            "BRONZE_RECONCILIATION",
        )
    ]


def test_reconcile_incidents_resolves_recovered_incident(
    monkeypatch,
):
    executed = []

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
            params=None,
        ):
            executed.append((sql, params))

        def fetchall(self):
            return [
                (2, "SILVER_RAW_RECONCILIATION")
            ]

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            pass

    health = {
        "status": "HEALTHY",
        "issues": [],
        "incident_types": [],
    }

    monkeypatch.setattr(
        health_module,
        "send_recovery_alert",
        lambda **kwargs: None,
    )

    reconcile_incidents(
        FakeConnection(),
        health,
        airflow_run_id="recovery_run_001",
    )

    resolutions = [params for sql, params in executed if "resolved_at = NOW()" in sql]

    assert resolutions == [
        (
            "recovery_run_001",
            2,
        )
    ]


def test_new_incident_sends_alert(monkeypatch):
    executed = []
    alerts = []

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            return False

        def execute(self, sql, params=None):
            executed.append((sql, params))

        def fetchall(self):
            return []

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            pass

    def fake_send_incident_alert(**kwargs):
        alerts.append(kwargs)

    monkeypatch.setattr(
        health_module,
        "send_incident_alert",
        fake_send_incident_alert,
    )

    health = {
        "status": "DEGRADED",
        "issues": ["Silver is ahead of Raw"],
        "incident_types": ["SILVER_RAW_RECONCILIATION"],
    }

    reconcile_incidents(
        FakeConnection(),
        health,
    )

    assert alerts == [
        {
            "incident_type": "SILVER_RAW_RECONCILIATION",
            "severity": "DEGRADED",
            "details": "Silver is ahead of Raw",
        }
    ]


def test_existing_incident_does_not_resend_alert(monkeypatch):
    alerts = []

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            return False

        def execute(self, sql, params=None):
            pass

        def fetchall(self):
            return [(1, "BRONZE_RECONCILIATION")]

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            pass

    def fake_send_incident_alert(**kwargs):
        alerts.append(kwargs)

    monkeypatch.setattr(
        health_module,
        "send_incident_alert",
        fake_send_incident_alert,
    )

    health = {
        "status": "WARNING",
        "issues": ["Bronze mismatch"],
        "incident_types": ["BRONZE_RECONCILIATION"],
    }

    reconcile_incidents(
        FakeConnection(),
        health,
    )

    assert alerts == []


def test_resolved_incident_sends_recovery_alert(monkeypatch):
    recoveries = []

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            return False

        def execute(self, sql, params=None):
            pass

        def fetchall(self):
            return [(2, "SILVER_RAW_RECONCILIATION")]

    class FakeConnection:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            pass

    def fake_send_recovery_alert(**kwargs):
        recoveries.append(kwargs)

    monkeypatch.setattr(
        health_module,
        "send_recovery_alert",
        fake_send_recovery_alert,
    )

    health = {
        "status": "HEALTHY",
        "issues": [],
        "incident_types": [],
    }

    reconcile_incidents(
        FakeConnection(),
        health,
    )

    assert recoveries == [
        {
            "incident_type": "SILVER_RAW_RECONCILIATION",
        }
    ]
