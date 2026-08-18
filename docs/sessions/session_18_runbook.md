# Session 18 Runbook — Pipeline Run Lineage & End-to-End Traceability

## Session goal

Add run-level operational lineage that connects one Airflow DAG run to RetailPulse-specific execution metrics and final run status.

Airflow continues to manage its own orchestration metadata. RetailPulse stores only the business/operational summary needed for platform observability.

Target architecture:

```text
Airflow DAG run_id
        ↓
control.pipeline_runs
        ↓
loader metrics
        ↓
dbt result
        ↓
health result
        ↓
SUCCEEDED / FAILED
```

## 1. Baseline

Run:

```cmd
git status
pytest -v
ruff check .
python warehouse\monitoring\check_pipeline_health.py --strict
```

Observed baseline:

```text
40 tests passed
Ruff clean

Bronze rows:       656
Silver rows:       652
Silver unique:     650
Silver duplicates: 2
Quarantine rows:   4
Raw orders:        650
Fact orders:       650
Gold order count:  650

Status: HEALTHY
```

Locate existing orchestration hooks:

```cmd
findstr /S /N /I "pipeline_metrics" airflow\*.py warehouse\*.py
findstr /S /N /I "load_orders.py" airflow\*.py
```

Observed:

```text
airflow\dags\retailpulse_warehouse_pipeline.py:88:    def record_pipeline_metrics(
airflow\dags\retailpulse_warehouse_pipeline.py:97:    metrics = record_pipeline_metrics(validation)
airflow\dags\retailpulse_warehouse_pipeline.py:38:            "cd /opt/retailpulse && " "python warehouse/loader/load_orders.py"
```

## 2. Existing DAG structure

The original DAG flow was:

```text
run_incremental_loader
→ validate_raw_orders
→ run_dbt_build
→ check_pipeline_health
→ record_pipeline_metrics
```

Airflow owns DAG/task internals.

Session 18 adds a compact RetailPulse-specific run summary instead of duplicating Airflow metadata.

## 3. Create control.pipeline_runs

Add to:

```text
warehouse/init/001_create_warehouse.sql
```

Schema:

```sql
CREATE TABLE IF NOT EXISTS control.pipeline_runs (
    pipeline_run_id BIGSERIAL PRIMARY KEY,
    airflow_run_id TEXT NOT NULL UNIQUE,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,

    loader_files_discovered INTEGER,
    loader_files_skipped INTEGER,
    loader_files_loaded INTEGER,
    loader_rows_processed INTEGER,
    loader_rows_inserted INTEGER,
    loader_duplicates INTEGER,

    dbt_status TEXT,
    health_status TEXT,

    raw_orders BIGINT,
    latest_load TIMESTAMPTZ,

    status TEXT NOT NULL DEFAULT 'RUNNING',
    error_message TEXT,

    CONSTRAINT pipeline_runs_status_check
        CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED')),

    CONSTRAINT pipeline_runs_dbt_status_check
        CHECK (
            dbt_status IS NULL
            OR dbt_status IN ('SUCCEEDED', 'FAILED')
        ),

    CONSTRAINT pipeline_runs_health_status_check
        CHECK (
            health_status IS NULL
            OR health_status IN ('HEALTHY', 'WARNING', 'DEGRADED')
        )
);
```

Add index:

```sql
CREATE INDEX IF NOT EXISTS
    idx_pipeline_runs_started_at
ON control.pipeline_runs (started_at DESC);
```

Design separation:

```text
control.pipeline_runs
→ run outcome / orchestration summary

control.pipeline_metrics
→ detailed pipeline-state measurements
```

## 4. Apply table to running warehouse DB

Verify:

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "\d control.pipeline_runs"
```

Initial row count:

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "SELECT COUNT(*) FROM control.pipeline_runs;"
```

## 5. Extend loader CLI with Airflow run identity

File:

```text
warehouse/loader/load_orders.py
```

Add:

```python
parser.add_argument(
    "--airflow-run-id",
    help="Airflow DAG run_id used for pipeline-run lineage",
)
```

The loader already computed:

```text
Files discovered
Files skipped
Files loaded
Rows processed
Rows inserted
Duplicate rows ignored
```

Session 18 persists all six directly.

## 6. Add loader run-metrics writer

Add:

```python
def update_pipeline_run_loader_metrics(
    conn: psycopg.Connection,
    *,
    airflow_run_id: str,
    discovered_files: int,
    skipped_files: int,
    loaded_files: int,
    processed_rows: int,
    inserted_rows: int,
    duplicate_rows: int,
) -> None:
    conn.execute(
        """
        UPDATE control.pipeline_runs
        SET
            loader_files_discovered = %s,
            loader_files_skipped = %s,
            loader_files_loaded = %s,
            loader_rows_processed = %s,
            loader_rows_inserted = %s,
            loader_duplicates = %s
        WHERE airflow_run_id = %s
        """,
        (
            discovered_files,
            skipped_files,
            loaded_files,
            processed_rows,
            inserted_rows,
            duplicate_rows,
            airflow_run_id,
        ),
    )
```

Initialize:

```python
duplicate_rows = 0
```

with the other counters.

## 7. Persist no-op loader runs

If there are no eligible partitions, persist all-zero loader metrics before returning.

## 8. Persist normal loader totals

At the end:

```python
duplicate_rows = processed_rows - inserted_rows
```

then:

```python
if args.airflow_run_id is not None:
    update_pipeline_run_loader_metrics(
        conn,
        airflow_run_id=args.airflow_run_id,
        discovered_files=discovered_files,
        skipped_files=skipped_files,
        loaded_files=loaded_files,
        processed_rows=processed_rows,
        inserted_rows=inserted_rows,
        duplicate_rows=duplicate_rows,
    )
```

Existing console output remains unchanged.

## 9. Loader unit test

Add:

```python
def test_update_pipeline_run_loader_metrics():
    captured = {}

    class FakeConnection:
        def execute(self, sql, params):
            captured["sql"] = sql
            captured["params"] = params

    update_pipeline_run_loader_metrics(
        FakeConnection(),
        airflow_run_id="scheduled__2026-08-18T12:00:00+00:00",
        discovered_files=12,
        skipped_files=5,
        loaded_files=7,
        processed_rows=10,
        inserted_rows=7,
        duplicate_rows=3,
    )

    assert captured["params"] == (
        12,
        5,
        7,
        10,
        7,
        3,
        "scheduled__2026-08-18T12:00:00+00:00",
    )
```

Observed:

```text
8 loader tests passed
41 total tests passed
Ruff clean
```

## 10. Pass Airflow run_id into loader

Final production BashOperator command:

```python
bash_command=(
    "cd /opt/retailpulse && "
    "python warehouse/loader/load_orders.py "
    '--airflow-run-id "{{ run_id }}"'
),
```

## 11. Create lineage row at DAG start

Add a TaskFlow task `start_pipeline_run()` that inserts or resets one row in `control.pipeline_runs` for the current `run_id`, with status `RUNNING`.

The `ON CONFLICT (airflow_run_id)` path resets the run row for retries of the same Airflow run.

## 12. Complete successful run

Add `complete_pipeline_run(validation)` to update:

```text
finished_at
dbt_status = SUCCEEDED
health_status = HEALTHY
raw_orders
latest_load
status = SUCCEEDED
```

Successful path:

```text
start_pipeline_run
→ run_incremental_loader
→ validate_raw_orders
→ run_dbt_build
→ check_pipeline_health
→ record_pipeline_metrics
→ complete_pipeline_run
```

## 13. Successful lineage proof

Trigger:

```cmd
docker compose exec airflow-api-server airflow dags trigger ^
  -r session18_lineage_proof_001 ^
  retailpulse_warehouse_pipeline
```

Observed Airflow result:

```text
session18_lineage_proof_001
→ success
```

Observed lineage row:

```text
airflow_run_id           session18_lineage_proof_001
loader_files_discovered  2
loader_files_skipped     2
loader_files_loaded      0
loader_rows_processed    0
loader_rows_inserted     0
loader_duplicates        0
dbt_status               SUCCEEDED
health_status            HEALTHY
raw_orders               650
status                   SUCCEEDED
error_message            NULL
```

Interpretation:

```text
2 Silver files found
→ both already loaded
→ both skipped
→ no rows loaded
→ dbt succeeded
→ health healthy
→ run succeeded
```

## 14. Add failure watcher

Goal:

```text
RUNNING
→ operational task fails
→ pipeline_runs FAILED
→ finished_at populated
→ failed task captured
→ Airflow DAG remains failed
```

Final watcher implementation:

```python
@task(
    trigger_rule="one_failed",
)
def record_pipeline_failure() -> None:
    context = get_current_context()

    ti = context["ti"]
    airflow_run_id = ti.run_id

    task_breadcrumbs = ti.get_task_breadcrumbs(
        dag_id=ti.dag_id,
        run_id=airflow_run_id,
    )

    failed_tasks = [
        breadcrumb["task_id"]
        for breadcrumb in task_breadcrumbs
        if breadcrumb.get("state") == "failed"
        and breadcrumb["task_id"] != "record_pipeline_failure"
    ]

    error_message = (
        "Pipeline failed. Failed tasks: "
        + ", ".join(sorted(failed_tasks))
    )

    with psycopg.connect(
        host="postgres",
        port=5432,
        dbname="retailpulse",
        user="retailpulse",
        password="retailpulse",
    ) as conn:
        conn.execute(
            """
            UPDATE control.pipeline_runs
            SET
                finished_at = NOW(),
                status = 'FAILED',
                error_message = %s
            WHERE airflow_run_id = %s
            """,
            (
                error_message,
                airflow_run_id,
            ),
        )

    print(error_message)

    raise RuntimeError(error_message)
```

Watcher dependencies:

```python
[
    run_incremental_loader,
    validation,
    run_dbt_build,
    check_pipeline_health,
    metrics,
    completion,
] >> failure
```

The final `raise` is intentional so the watcher itself fails and the DAG remains failed.

## 15. Failure watcher debugging history

First failure attempt:

```text
run_incremental_loader failed
downstream tasks upstream_failed
record_pipeline_failure failed
```

But `control.pipeline_runs` stayed `RUNNING`.

The task log showed:

```text
TypeError:
RuntimeTaskInstance.get_task_states()
missing 1 required positional argument: 'dag_id'
```

A later attempt correctly set the row to `FAILED` but produced an empty failed-task list.

The final implementation switched to run-scoped `get_task_breadcrumbs(...)` and filtered only true `failed` tasks.

## 16. Final isolated failure proof

A temporary loader condition targeted only:

```text
session18_failure_proof_005
```

and set:

```text
POSTGRES_HOST=invalid-postgres-host
```

for that run only.

Observed Airflow:

```text
session18_failure_proof_005
→ failed
```

Observed RetailPulse lineage:

```text
pipeline_run_id  12
airflow_run_id   session18_failure_proof_005
started_at       2026-08-18 15:48:58.37624+00
finished_at      2026-08-18 15:53:11.383267+00
status           FAILED
error_message    Pipeline failed. Failed tasks: run_incremental_loader
```

A normal scheduled run succeeded immediately afterward, proving the failure injection was isolated.

## 17. Restore production command

Remove the temporary failure branch.

Final loader Bash command:

```python
bash_command=(
    "cd /opt/retailpulse && "
    "python warehouse/loader/load_orders.py "
    '--airflow-run-id "{{ run_id }}"'
),
```

## 18. Final regression gate

Run:

```cmd
python -m py_compile airflow\dags\retailpulse_warehouse_pipeline.py
pytest -v
ruff check .
python warehouse\monitoring\check_pipeline_health.py --strict
git status
```

Observed:

```text
41 tests passed
Ruff clean

Bronze rows:       656
Silver rows:       652
Silver unique:     650
Silver duplicates: 2
Quarantine rows:   4
Raw orders:        650
Fact orders:       650
Gold order count:  650

Status: HEALTHY
```

Git showed the intended Session 18 modifications only after removing an accidental zero-byte root file named `dict`.

## 19. Session 18 proven properties

```text
[x] Airflow run_id is the lineage key
[x] one RetailPulse pipeline_runs row per DAG run
[x] RUNNING state persisted at start
[x] loader file metrics persisted
[x] loader row metrics persisted
[x] no-op runs observable
[x] dbt success persisted
[x] health success persisted
[x] raw_orders persisted
[x] latest_load persisted
[x] successful runs finish as SUCCEEDED
[x] failed runs finish as FAILED
[x] finished_at populated for failures
[x] root failed task captured
[x] failure watcher keeps DAG failed
[x] failure proof isolated to one manual run
[x] temporary failure injection removed
[x] 41 tests pass
[x] Ruff passes
[x] strict health HEALTHY
```

## 20. Precise engineering claim

> RetailPulse implements run-level operational lineage keyed by the Airflow DAG run ID, persisting loader execution metrics and final success/failure outcomes while preserving Airflow as the source of truth for orchestration internals.

## 21. Git update

Copy this file to:

```text
docs/sessions/session_18_runbook.md
```

Inspect:

```cmd
git status
git diff
```

Expected files:

```text
airflow/dags/retailpulse_warehouse_pipeline.py
warehouse/init/001_create_warehouse.sql
warehouse/loader/load_orders.py
warehouse/tests/test_load_orders.py
docs/sessions/session_18_runbook.md
```

Stage:

```cmd
git add airflow/dags/retailpulse_warehouse_pipeline.py
git add warehouse/init/001_create_warehouse.sql
git add warehouse/loader/load_orders.py
git add warehouse/tests/test_load_orders.py
git add docs/sessions/session_18_runbook.md
```

Review:

```cmd
git status
git diff --cached
```

Commit:

```cmd
git commit -m "Add pipeline run lineage"
```

Push:

```cmd
git push origin main
```

Final check:

```cmd
git status
```

Expected:

```text
On branch main
Your branch is up to date with 'origin/main'.

nothing to commit, working tree clean
```

Confirm GitHub Actions is green.

Session 18 complete.
