import pytest

from warehouse.monitoring.config import (
    load_monitoring_config,
)


def test_monitoring_config_uses_defaults(
    monkeypatch,
):
    monkeypatch.delenv(
        "MAX_LAG_ROWS",
        raising=False,
    )
    monkeypatch.delenv(
        "MAX_LOAD_AGE_MINUTES",
        raising=False,
    )

    config = load_monitoring_config()

    assert config.max_lag_rows == 60
    assert config.max_load_age_minutes == 2880


def test_monitoring_config_reads_environment(
    monkeypatch,
):
    monkeypatch.setenv(
        "MAX_LAG_ROWS",
        "25",
    )
    monkeypatch.setenv(
        "MAX_LOAD_AGE_MINUTES",
        "120",
    )

    config = load_monitoring_config()

    assert config.max_lag_rows == 25
    assert config.max_load_age_minutes == 120


def test_monitoring_config_rejects_negative_lag(
    monkeypatch,
):
    monkeypatch.setenv(
        "MAX_LAG_ROWS",
        "-1",
    )

    with pytest.raises(
        ValueError,
        match="MAX_LAG_ROWS",
    ):
        load_monitoring_config()


def test_monitoring_config_rejects_negative_load_age(
    monkeypatch,
):
    monkeypatch.setenv(
        "MAX_LOAD_AGE_MINUTES",
        "-1",
    )

    with pytest.raises(
        ValueError,
        match="MAX_LOAD_AGE_MINUTES",
    ):
        load_monitoring_config()
