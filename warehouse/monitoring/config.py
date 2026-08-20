import os
from dataclasses import dataclass


@dataclass(frozen=True)
class MonitoringConfig:
    max_lag_rows: int = 60
    max_load_age_minutes: int = 2880


def load_monitoring_config() -> MonitoringConfig:
    config = MonitoringConfig(
        max_lag_rows=int(
            os.getenv(
                "MAX_LAG_ROWS",
                "60",
            )
        ),
        max_load_age_minutes=int(
            os.getenv(
                "MAX_LOAD_AGE_MINUTES",
                "2880",
            )
        ),
    )

    if config.max_lag_rows < 0:
        raise ValueError("MAX_LAG_ROWS must be zero or greater")

    if config.max_load_age_minutes < 0:
        raise ValueError("MAX_LOAD_AGE_MINUTES must be zero or greater")

    return config
