# RetailPulse Disaster Recovery

## 1. Recovery philosophy

RetailPulse treats downstream analytical state as reproducible/derived state. The most directly proven recovery boundary is **committed Silver**.

```text
committed Silver
  ↓
raw.orders
  ↓
dbt staging/fact/mart
  ↓
strict HEALTHY
```

Kafka is the deeper event source if retained history is still available, but exact historical lake replay across schema-version changes may require version-aware transformation.

## 2. Proven analytical-layer disaster scenario

Session 29 deliberately destroyed:

```text
raw.orders
analytics schema
  ├─ stg_orders
  ├─ fct_orders
  └─ mart_daily_sales
```

while preserving `control.*` operational/audit history.

Before recovery, the committed Silver state was:

```text
Committed Silver files       1,433
Committed Silver rows       29,132
Silver unique events        29,130
Duplicate deliveries             2
Quarantine rows                  5
```

A physical Silver orphan file was also found and correctly excluded because it was not present in Spark `_spark_metadata`.

## 3. Freeze orchestration

Before destructive warehouse recovery:

```cmd
docker compose stop airflow-scheduler airflow-dag-processor
```

This prevents a scheduled DAG from operating against a deliberately incomplete warehouse.

## 4. Destructive analytical reset used in the drill

```cmd
docker compose exec postgres sh -lc "psql -v ON_ERROR_STOP=1 -U $POSTGRES_USER -d $POSTGRES_DB -c \"DROP SCHEMA IF EXISTS analytics CASCADE; DROP TABLE IF EXISTS raw.orders;\""
```

This intentionally preserves `control.*`.

## 5. Recreate Raw/control DDL

```cmd
docker compose exec -T postgres sh -lc "psql -v ON_ERROR_STOP=1 -U $POSTGRES_USER -d $POSTGRES_DB" < warehouse\init\001_create_warehouse.sql
```

The control tables already present are retained by the idempotent bootstrap.

## 6. Replay committed Silver

Example drill range:

```cmd
python -m warehouse.loader.load_orders ^
  --from 2026-08-11T00 ^
  --to 2026-08-23T00 ^
  --replay
```

Observed recovery result:

```text
Files discovered:        1433
Files skipped:              0
Files loaded:            1433
Rows processed:         29132
Rows inserted:          29130
Duplicate rows ignored:     2
```

This proved:

- the loader respected committed Silver metadata;
- the uncommitted physical orphan was ignored;
- every authoritative committed delivery was reread;
- duplicate deliveries had one logical business effect.

## 7. Resume Airflow and rebuild analytics

```cmd
docker compose start airflow-scheduler airflow-dag-processor
```

The next normal DAG/dbt build recreated:

```text
analytics.stg_orders
analytics.fct_orders
analytics.mart_daily_sales
```

The `fct_orders(event_id)` unique index also reappeared automatically because it is dbt-owned.

## 8. Validate recovery

```cmd
python -m warehouse.monitoring.check_pipeline_health --strict
```

Observed final recovery state:

```text
Silver unique     29130
Raw               29130
Fact              29130
Gold              29130
Status            HEALTHY
```

## 9. Complete lake loss

If the entire `data_lake/` directory is removed, the Spark checkpoints inside it are also removed.

Provided Kafka still retains the required event history and the Spark stream starts with a fresh checkpoint, the canonical job is configured with:

```text
startingOffsets = earliest
```

so it can reconstruct new Bronze/Silver/Quarantine derived state from retained Kafka history.

Conceptually:

```text
retained Kafka history
   ↓
fresh Spark checkpoints
   ↓
Bronze / Silver / Quarantine
   ↓
loader
   ↓
Raw
   ↓
dbt
   ↓
Fact / Mart
```

### Important schema-evolution caveat

The project contains a historical pre-V1 period before `schema_version` became mandatory. Re-running today's V1 rules over all historical Bronze does **not** reproduce historical Silver exactly: legacy events valid at ingestion time would now be quarantined for missing `schema_version`.

Therefore exact full-history lake reconstruction across contract changes requires a version-aware replay strategy. RetailPulse documents this limitation rather than introducing an artificial historical exception.

## 10. Total local platform loss

Repository + `.env` + runtime source history are different assets:

```text
Git repository    code/infrastructure/config templates
.env              local runtime configuration/secrets
Kafka volume      retained event history
Postgres volume   current warehouse/control state
data_lake          derived lake/checkpoints
```

A clean clone with a valid `.env` recreates the **platform structure**, but not historical business data unless an upstream retained source (for example Kafka history) is also available.

## 11. Recovery boundaries summary

| Loss | Recovery source | Proven? |
|---|---|---|
| dbt analytics objects | Raw / dbt source | Yes |
| Raw + analytics | committed Silver | Yes — destructive drill |
| Physical uncommitted Silver residue | `_spark_metadata` authority excludes it | Yes |
| Full lake/checkpoints | retained Kafka | Architecturally supported; exact historical version replay caveat |
| Kafka history + all derived state | external/source backup required | Not provided by this local reference implementation |

## 12. DR principles

1. Freeze orchestration before intentional destructive repair.
2. Preserve `control.*` unless the scenario specifically requires losing operational history.
3. Treat `_spark_metadata` as authoritative for lake file commitment.
4. Prefer deterministic replay over manual row surgery.
5. Validate Raw = Fact = Gold and Silver unique = Raw before declaring recovery complete.
6. Use `docker compose down -v` only for intentional volume destruction.
