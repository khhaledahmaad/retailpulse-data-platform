# RetailPulse — Validated Platform Checkpoint

**Date:** 12 August 2026  
**Completed sessions:** 1–7  
**Next session:** 8 — CI/CD and automated quality checks

## Current Architecture

```text
Python synthetic order producer
        ↓
Kafka
        ↓
Spark Structured Streaming
        ↓
Bronze / Silver / Quarantine Parquet
        ↓
incremental Python warehouse loader
        ↓
PostgreSQL raw.orders
        ↓
dbt staging / incremental fact / mart
        ↓
Airflow downstream orchestration
```

## Final Validation State

```text
Kafka broker records = 108
Bronze               = 108
Silver               = 108
Quarantine           =   0
Warehouse / dbt      = 108
```

Kafka broker offsets:

```text
orders:0:43
orders:1:31
orders:2:34
```

Spark current-lineage ranges:

```text
partition 0 → 0–42 → 43 rows
partition 1 → 0–30 → 31 rows
partition 2 → 0–33 → 34 rows
```

## Regression Status

- Two repeated Airflow regression runs completed successfully.
- Incremental loader processed only new Silver files.
- dbt incremental execution completed successfully.
- Kafka persistence and Spark checkpoint alignment are in place.
- Normal `docker compose down` / `docker compose up -d` is the restart-safe path.
- `docker compose down -v` is destructive to named-volume state.
- Kafka UI `messages consumed` is not used as the authoritative current-topic row count; broker end offsets are.

## Session 8 Starting Point

Planned focus:

```text
GitHub Actions
Ruff
pytest
dbt parse / compile
docker compose config
intentional CI failure / fix
```

The full local Kafka/Spark/Airflow stack does not need to be started inside CI at this stage.
