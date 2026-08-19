from datetime import datetime, timedelta, timezone

from warehouse.monitoring.operations_dashboard import (
    render_dashboard,
    render_trend_chart,
)

NOW = datetime(
    2026,
    8,
    19,
    14,
    30,
    tzinfo=timezone.utc,
)


def sample_metrics():
    return {
        "metric_id": 325,
        "recorded_at": NOW,
        "bronze_rows": 1147,
        "silver_rows": 1142,
        "silver_unique_events": 1140,
        "silver_duplicate_deliveries": 2,
        "quarantine_rows": 5,
        "raw_orders": 1140,
        "fact_orders": 1140,
        "gold_order_count": 1140,
        "silver_raw_lag": 0,
        "latest_loaded_at": (NOW - timedelta(minutes=863)),
        "freshness_minutes": 863,
        "status": "HEALTHY",
        "details": ("All reconciliation and " "freshness checks passed"),
    }


def sample_history():
    return [
        {
            "recorded_at": NOW,
            "status": "HEALTHY",
            "silver_rows": 1142,
            "silver_unique_events": 1140,
            "silver_duplicate_deliveries": 2,
            "raw_orders": 1140,
            "silver_raw_lag": 0,
            "freshness_minutes": 863,
        },
        {
            "recorded_at": (NOW - timedelta(minutes=10)),
            "status": "HEALTHY",
            "silver_rows": 1142,
            "silver_unique_events": 1140,
            "silver_duplicate_deliveries": 2,
            "raw_orders": 1140,
            "silver_raw_lag": 0,
            "freshness_minutes": 853,
        },
    ]


def test_dashboard_renders_core_operator_metrics():
    html = render_dashboard(
        sample_metrics(),
        [],
        [],
        sample_history(),
    )

    assert "RetailPulse Operations" in html
    assert "HEALTHY" in html
    assert "Silver→Raw lag" in html
    assert "Silver duplicates" in html
    assert "Layer reconciliation" in html
    assert "1142" in html
    assert "1140" in html
    assert "863" in html


def test_dashboard_renders_trend_charts():
    html = render_dashboard(
        sample_metrics(),
        [],
        [],
        sample_history(),
    )

    assert "Trends" in html
    assert "Silver→Raw logical lag" in html
    assert "Warehouse freshness" in html
    assert "<svg" in html
    assert "trend-line" in html
    assert "trend-point" in html
    assert "<title>" in html
    assert "snapshots" in html
    assert "Hover points for details" in html


def test_dashboard_escapes_incident_details():
    incidents = [
        {
            "incident_id": 1,
            "incident_type": ("WAREHOUSE_FRESHNESS"),
            "severity": "WARNING",
            "details": ("<script>alert('x')</script>"),
            "opened_at": NOW,
            "alert_sent_at": NOW,
        }
    ]

    html = render_dashboard(
        sample_metrics(),
        incidents,
        [],
        sample_history(),
    )

    assert "<script>alert('x')</script>" not in html

    assert "&lt;script&gt;" in html


def test_trend_chart_handles_no_values():
    html = render_trend_chart(
        [
            {
                "recorded_at": NOW,
                "silver_raw_lag": None,
            }
        ],
        "Test metric",
        "silver_raw_lag",
    )

    assert "Test metric" in html
    assert "No historical values available." in html
