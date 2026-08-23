# Session 29 Runbook — Disaster Recovery / Analytical Layer Rebuild

**Project:** RetailPulse Data Platform  
**Session:** 29  
**Date:** 2026-08-23  
**Theme:** Disaster Recovery / Full Rebuild  
**Outcome:** PASS — analytical warehouse recovery proven; platform rebuild boundaries clarified.

---

## 1. Session objective

The goal of Session 29 was to prove that RetailPulse can recover from destructive loss of the analytical warehouse layer and to validate how much of the platform is reproducible from retained upstream state.

The practical recovery target was:

```text
Committed Silver
      ↓
raw.orders
      ↓
analytics.stg_orders
      ↓
analytics.fct_orders
      ↓
analytics.mart_daily_sales
      ↓
STRICT HEALTHY
```

The session also investigated whether full lake recovery from Bronze was necessary. That investigation exposed a historical schema-evolution limitation and confirmed that lake recovery is outside the required scope for this portfolio project.

---

## 2. Initial system baseline

Before destructive recovery work, strict health was green:

```text
Bronze rows:        29137
Silver rows:        29132
Silver unique:      29130
Silver duplicates:      2
Quarantine rows:        5
Raw orders:         29130
Fact orders:        29130
Gold order count:   29130
Status:             HEALTHY
```

Direct business-layer counts also reconciled at `29130`.

Committed Silver files:

```text
1433
```

Control-state baseline:

```text
control.loaded_files              1465
control.pipeline_metrics           441
control.pipeline_runs              177
control.pipeline_incidents          28
control.event_reprocessing_log       3
```

---

## 3. Cold-start Airflow issue discovered

A full OS + Docker daemon reboot exposed an Airflow startup problem.

On the first:

```cmd
docker compose up -d
```

the Airflow API server could become unhealthy. Retrying Compose then succeeded.

### Root cause

The following services used:

```yaml
restart: unless-stopped
```

- `airflow-api-server`
- `airflow-scheduler`
- `airflow-dag-processor`

After Docker daemon startup, Docker could restart these containers independently before normal Compose dependency orchestration had completed.

Observed sequence:

- Airflow Postgres recovered from an abrupt shutdown.
- Database became ready.
- Airflow API required roughly another 25–30 seconds to finish application startup.
- Scheduler / DAG processor could start independently because of the restart policy.

This bypassed the intended dependency ordering.

### Fix

Changed the three long-running Airflow services to:

```yaml
restart: on-failure
```

`airflow-init` remained:

```yaml
restart: "no"
```

Applied with:

```cmd
docker compose up -d --force-recreate airflow-api-server airflow-scheduler airflow-dag-processor
```

Verified restart policies.

### Controlled cold-start proof

Ran:

```cmd
docker compose down
docker compose up -d
docker compose ps
```

Result:

- Airflow API became healthy on the first startup.
- Scheduler and DAG processor started only after the API was healthy.
- Kafka, Spark and both Postgres services were healthy.
- No second Compose invocation was required.

### Operational consequence

With `on-failure`, these Airflow containers no longer auto-start merely because the Docker daemon starts.

That is intentional for this project: the normal operator action is:

```cmd
docker compose up -d
```

which preserves Compose dependency orchestration.

---

## 4. Freeze orchestration before destructive work

To prevent scheduled Airflow activity during the DR drill:

```cmd
docker compose stop airflow-scheduler airflow-dag-processor
```

Other services remained available.

---

## 5. Warehouse bootstrap path confirmed

The warehouse schema is not automatically mounted through `docker-entrypoint-initdb.d`.

The correct idempotent bootstrap command is:

```cmd
docker compose exec -T postgres sh -lc "psql -v ON_ERROR_STOP=1 -U $POSTGRES_USER -d $POSTGRES_DB" < warehouse\init\001_create_warehouse.sql
```

The SQL uses idempotent `IF NOT EXISTS`-style creation where appropriate, allowing it to recreate missing business objects while preserving existing control tables.

---

## 6. Important Silver discovery — physical orphan file

During DR preparation, physical Silver files were compared with files committed in Spark `_spark_metadata`.

Result:

```text
Physical Silver files:   1434
Committed Silver files:  1433
Difference:                 1
```

The orphan physical file contained 3 rows.

Those 3 events:

- already existed in `raw.orders`
- each also existed exactly once in committed Silver

Therefore the file was non-authoritative physical residue, not part of the committed Silver dataset.

The authoritative Silver state remained:

```text
Committed Silver rows:    29132
Committed Silver unique:  29130
Committed duplicates:         2
```

This exposed an important loader inconsistency:

- health checks used `_spark_metadata`
- the warehouse loader previously globbed all physical Parquet files

A fresh recovery could therefore have processed uncommitted Spark residue.

---

## 7. Loader hardened to committed Silver only

`warehouse/loader/load_orders.py` was changed to use the same committed-file authority as monitoring.

Added:

```python
from warehouse.monitoring.check_pipeline_health import get_committed_files
```

The loader now obtains committed files once:

```python
committed_files = set(get_committed_files(SILVER_ROOT))
print(f"Committed Silver files: {len(committed_files)}")
```

Partition file discovery now filters to committed paths:

```python
def discover_files_in_partition(
    partition_path: Path,
    committed_files: set[Path],
) -> list[Path]:
    return sorted(
        path for path in partition_path.glob("*.parquet")
        if path in committed_files
    )
```

The normal partition-loading flow passes that committed-file set into discovery.

### Test added

Added:

```text
test_discover_files_excludes_uncommitted_parquet
```

The test creates a committed Parquet path plus an uncommitted physical path and verifies that only the committed file is returned.

---

## 8. Loader invocation standardized as a Python module

After the loader began importing another `warehouse.*` module, direct file execution:

```cmd
python warehouse\loader\load_orders.py ...
```

failed with:

```text
ModuleNotFoundError: No module named 'warehouse'
```

Reason: file-path execution changes `sys.path` so the project package root is no longer resolved reliably.

The supported invocation is now:

```cmd
python -m warehouse.loader.load_orders ...
```

### Airflow DAG updated

The loader `BashOperator` was changed from file-path execution to module execution:

```python
run_incremental_loader = BashOperator(
    task_id="run_incremental_loader",
    bash_command=(
        "cd /opt/retailpulse && "
        "python -m warehouse.loader.load_orders "
        '--airflow-run-id "{{ run_id }}"'
    ),
    retries=2,
    retry_delay=timedelta(minutes=1),
    execution_timeout=timedelta(minutes=5),
)
```

The current end-to-end validation documentation was also updated to use module invocation.

Historical runbooks were intentionally left unchanged.

---

## 9. Recovery replay range

Available Silver partitions were:

```text
First partition: 2026-08-11 21
Last partition:  2026-08-22 12
Partitions:      28
```

A wider explicit replay window was selected for simplicity:

```cmd
python -m warehouse.loader.load_orders ^
  --from 2026-08-11T00 ^
  --to 2026-08-23T00 ^
  --replay
```

The loader only processes existing partition directories within the requested range, so the wider time boundary is safe.

---

## 10. Destructive analytical-layer DR drill

The final DR scope deliberately preserved `control.*` operational history.

Destroyed:

- `raw.orders`
- entire `analytics` schema
  - `analytics.stg_orders`
  - `analytics.fct_orders`
  - `analytics.mart_daily_sales`

Command:

```cmd
docker compose exec postgres sh -lc "psql -v ON_ERROR_STOP=1 -U $POSTGRES_USER -d $POSTGRES_DB -c \"DROP SCHEMA IF EXISTS analytics CASCADE; DROP TABLE IF EXISTS raw.orders;\""
```

Then reran warehouse bootstrap:

```cmd
docker compose exec -T postgres sh -lc "psql -v ON_ERROR_STOP=1 -U $POSTGRES_USER -d $POSTGRES_DB" < warehouse\init\001_create_warehouse.sql
```

### Disaster-state verification

After destruction/bootstrap:

```text
raw.orders                       0
control.loaded_files          1465
control.pipeline_metrics       441
control.pipeline_runs          177
control.pipeline_incidents      28
control.event_reprocessing_log   3
```

This proved the business warehouse was empty while operational/audit history was retained.

---

## 11. Raw recovery replay

Executed:

```cmd
python -m warehouse.loader.load_orders ^
  --from 2026-08-11T00 ^
  --to 2026-08-23T00 ^
  --replay
```

Final result:

```text
Load complete.
Files discovered:       1433
Files skipped:             0
Files loaded:           1433
Rows processed:        29132
Rows inserted:         29130
Duplicate rows ignored:    2
```

This is the core Session 29 recovery proof.

It demonstrates:

1. only the 1433 authoritative committed Silver files were replayed
2. the physical orphan file was excluded
3. all 29132 committed Silver deliveries were processed
4. exactly 29130 logical events were restored
5. the two intentional duplicate deliveries had no duplicate business effect

---

## 12. Airflow used to rebuild the analytics layer

Rather than manually invoking dbt after Raw recovery, normal orchestration was resumed:

```cmd
docker compose start airflow-scheduler airflow-dag-processor
```

A manual Airflow run completed successfully:

```text
status:               SUCCEEDED
dbt_status:           SUCCEEDED
health_status:        HEALTHY
loader_rows_inserted: 0
```

`loader_rows_inserted = 0` is correct because Raw had already been completely restored by the replay.

Airflow then:

- validated Raw
- recreated dbt staging
- recreated Fact
- recreated Gold
- ran health reconciliation
- completed the pipeline successfully

Scheduled orchestration also resumed.

---

## 13. Final analytical-layer recovery proof

Business counts after recovery:

```text
raw.orders                         29130
analytics.fct_orders               29130
analytics.mart_daily_sales         29130
```

`analytics.stg_orders` was confirmed as a view.

Strict health:

```text
Bronze rows:        29137
Silver rows:        29132
Silver unique:      29130
Silver duplicates:      2
Quarantine rows:        5
Raw orders:         29130
Fact orders:        29130
Gold order count:   29130
Status:             HEALTHY
```

The analytical warehouse therefore returned exactly to the expected logical business state.

---

## 14. Session 28 index ownership revalidated

`analytics.fct_orders` was dropped during the DR drill and recreated by dbt.

After recovery:

```text
UNIQUE, btree (event_id)
```

was present automatically.

This confirms that the Session 28 index is now fully dbt-owned through the model definition:

```jinja
{{
    config(
        materialized='incremental',
        unique_key='event_id',
        indexes=[
            {'columns': ['event_id'], 'unique': true}
        ]
    )
}}
```

No manual index creation is required anymore.

---

## 15. Bronze / lake recovery investigation

A possible Session 29B experiment considered deleting Silver and rebuilding it from Bronze.

### Bronze authoritative state

Physical vs committed Bronze:

```text
Physical Bronze files:   1415
Committed Bronze files:  1413
Difference:                 2
```

The two uncommitted physical files contained:

```text
3 rows
1 row
```

Therefore:

```text
Physical Bronze rows:   29141
Committed Bronze rows:  29137
```

A normal Spark batch read of the Bronze streaming-sink directory returned:

```text
Spark batch Bronze rows: 29137
```

This proved Spark batch reading respects `_spark_metadata` and automatically ignores the 4 uncommitted physical Bronze rows.

---

## 16. Historical schema-evolution limitation discovered

A read-only replay test applied the **current** transformation rules to the full committed Bronze history.

Result:

```text
Bronze rows:       29137
Silver rows:       28560
Quarantine rows:     577
```

That did not reproduce the known historical state:

```text
Silver rows:       29132
Quarantine rows:       5
```

Difference:

```text
577 - 5 = 572
```

Those 572 records correspond to the pre-contract historical period before `schema_version = 1` became mandatory.

At the time they were originally ingested, those events were valid under the earlier contract. Replaying all historical Bronze through today's V1 contract incorrectly classifies them as missing `schema_version`.

Therefore:

```text
historical Bronze
    +
today's contract rules
    ≠
exact historical Silver
```

Correct full Bronze recovery would require version-aware replay or an explicit migration boundary.

That complexity is intentionally out of scope for RetailPulse.

No Silver or Bronze data was deleted during this investigation.

---

## 17. Final DR boundary

The session clarified the intended architecture.

### Proven experimentally

```text
Retained committed Silver
    ↓
Raw
    ↓
staging
    ↓
Fact
    ↓
Gold
    ↓
HEALTHY
```

Analytical-layer disaster recovery is fully proven.

### Platform reproducibility

The lake and warehouse are derived platform state.

If the entire `data_lake` directory is removed, that also removes Spark checkpoints. Provided Kafka still retains the event history and the Spark stream starts from the beginning of that retained history, Kafka can regenerate the lake.

Conceptually:

```text
Retained Kafka history
        ↓
new Spark state / checkpoints
        ↓
Bronze
        ↓
Silver / Quarantine
        ↓
Raw
        ↓
dbt
        ↓
Fact / Gold
```

Exact historical counts may vary slightly because the project has intentionally created special test/reprocessing states during development.

The required invariant is that the regenerated platform reaches a consistent logical state and becomes HEALTHY.

### Practical conclusion

For RetailPulse:

- Kafka is the replayable upstream event source.
- Spark lake layers are derived state.
- Warehouse analytical layers are derived state.
- committed Silver is a deterministic and proven analytical recovery boundary.
- full version-aware Bronze replay is not required for this portfolio project.

---

## 18. Final quality gate

### dbt-managed Fact index

Confirmed after destructive rebuild:

```text
UNIQUE btree (event_id)
```

### Ruff

```cmd
ruff check .
```

Result:

```text
All checks passed!
```

### Pytest

```cmd
pytest -v
```

Result:

```text
72 passed in 11.56s
```

This includes:

```text
warehouse/tests/test_load_orders.py::test_discover_files_excludes_uncommitted_parquet PASSED
```

### Compose validation

```cmd
docker compose config --quiet
```

Completed without error.

### Strict pipeline health

Final state:

```text
HEALTHY
```

---

## 19. Session 29 final status

```text
Airflow cold-start fix          PASS
Committed Silver authority      PASS
Loader hardening                PASS
Module loader invocation        PASS
Analytical warehouse destroy    PASS
Raw replay                      PASS
dbt analytics rebuild           PASS
Fact unique index recreation    PASS
Strict reconciliation           PASS
Ruff                            PASS
Pytest                          72/72 PASS
Compose validation              PASS
```

**Session 29: COMPLETE**

---

## 20. Key lessons

1. Physical Parquet presence is not sufficient evidence of committed Spark output.
2. `_spark_metadata` must be treated as authoritative for streaming sink files.
3. Recovery tooling must use the same committed-file boundary as monitoring.
4. Python project scripts that import package modules should be executed with `python -m ...`.
5. Compose restart policies can materially affect dependency ordering after Docker daemon startup.
6. Analytical-layer recovery is deterministic when committed Silver is retained.
7. Schema evolution makes blind historical Bronze replay unsafe unless replay is version-aware.
8. dbt-owned indexes survive destructive analytical rebuilds automatically.
9. DR testing should preserve operational/audit history unless the specific scenario requires destroying it.
10. A successful DR drill should prove reconciliation invariants, not just that tables exist.
