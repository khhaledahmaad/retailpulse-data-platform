# Session 24 Runbook — Retry, Timeout & Failure Policy

## Goal

Replace the old blanket Airflow retry behaviour with explicit per-task retry budgets and execution timeouts.

Target behaviour:

```text
transient dependency failure
→ bounded retry
→ recover if dependency returns

health DEGRADED / deliberate failure path
→ fail fast where retry adds no value

hung task
→ execution_timeout
→ attempt terminated
```

No new service or dependency was added.

---

## 1. Baseline

The DAG previously had a global retry policy:

```python
default_args={
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}
```

This effectively meant most tasks could run up to 3 attempts, regardless of whether retrying made sense.

There were no Airflow `execution_timeout` values.

---

## 2. Remove global retry defaults

Removed the DAG-wide:

```python
"retries": 2,
"retry_delay": timedelta(minutes=1),
```

Retry policy is now explicit per task.

Validation:

```cmd
ruff check airflow\dags\retailpulse_warehouse_pipeline.py
```

---

## 3. Explicit retry policy

Configured:

```text
Task                      Retries   Max attempts
------------------------------------------------
start_pipeline_run           2          3
run_incremental_loader       2          3
validate_raw_orders          1          2
run_dbt_build                1          2
check_pipeline_health        0          1
record_pipeline_metrics      1          2
complete_pipeline_run        1          2
record_pipeline_failure      0          1
```

Retryable tasks use:

```python
retry_delay=timedelta(minutes=1)
```

Important Airflow semantics:

```text
retries=2
→ initial attempt + 2 retries
→ maximum 3 attempts
```

### Why these choices

```text
start_pipeline_run
→ transient Postgres failure may recover
→ safe upsert/reset by run_id

run_incremental_loader
→ DB/I/O failures may be transient
→ loader is idempotent

validate_raw_orders
→ DB connectivity may recover
→ real invalid state will fail again

run_dbt_build
→ one retry for transient DB/network issues

check_pipeline_health
→ DEGRADED is a health decision
→ retries=0

record_pipeline_metrics
→ DB persistence can be retried

complete_pipeline_run
→ DB update can be retried

record_pipeline_failure
→ deliberately records failure and raises
→ retry adds no value
```

Existing:

```python
trigger_rule="one_failed"
```

on `record_pipeline_failure` was preserved.

---

## 4. Execution timeout policy

Configured:

```text
Task                      Timeout
---------------------------------
start_pipeline_run          2 min
run_incremental_loader      5 min
validate_raw_orders         2 min
run_dbt_build              10 min
check_pipeline_health       5 min
record_pipeline_metrics     2 min
complete_pipeline_run       2 min
record_pipeline_failure     2 min
```

Airflow `execution_timeout` applies per attempt.

This changes the previous behaviour from:

```text
task hangs
→ potentially runs indefinitely
```

to:

```text
task exceeds defined ceiling
→ attempt terminated
→ retry policy applies if retries remain
```

---

## 5. Source validation

Used:

```cmd
findstr /N /I /C:"retries=" /C:"retry_delay=" /C:"execution_timeout=" /C:"trigger_rule=" airflow\dags\retailpulse_warehouse_pipeline.py
```

Observed the expected explicit retry/timeout settings for all operational tasks.

---

## 6. Airflow reload

Reloaded DAG processing:

```cmd
docker compose restart airflow-dag-processor
```

Checked imports:

```cmd
docker compose exec airflow-api-server airflow dags list-import-errors
```

Final result:

```text
No data found
```

Meaning there were no DAG import errors.

---

## 7. Airflow 3.3 introspection note

An initial `DagBag(..., include_examples=False)` introspection attempt failed because that argument is not accepted in this Airflow 3.3 environment.

A second direct import initially inspected `m.dag`, but that symbol was the imported decorator function rather than the instantiated DAG.

The working validation command was:

```cmd
docker compose exec airflow-scheduler python -c "import importlib.util; p='/opt/airflow/dags/retailpulse_warehouse_pipeline.py'; s=importlib.util.spec_from_file_location('retailpulse_dag', p); m=importlib.util.module_from_spec(s); s.loader.exec_module(m); d=m.retailpulse_warehouse_pipeline(); [(print(t.task_id, 'retries=', t.retries, 'retry_delay=', t.retry_delay, 'timeout=', t.execution_timeout, 'trigger=', t.trigger_rule)) for t in d.tasks]"
```

Observed:

```text
run_incremental_loader retries=2 timeout=0:05:00
run_dbt_build retries=1 timeout=0:10:00
check_pipeline_health retries=0 timeout=0:05:00
start_pipeline_run retries=2 timeout=0:02:00
validate_raw_orders retries=1 timeout=0:02:00
record_pipeline_metrics retries=1 timeout=0:02:00
complete_pipeline_run retries=1 timeout=0:02:00
record_pipeline_failure retries=0 timeout=0:02:00 trigger=ONE_FAILED
```

Airflow showed its default retry delay for zero-retry tasks, but with `retries=0` that value is never used.

---

## 8. Normal DAG success proof

Triggered:

```cmd
docker compose exec airflow-api-server airflow dags trigger ^
  -r session24_policy_success_001 ^
  retailpulse_warehouse_pipeline
```

Result:

```text
session24_policy_success_001
→ success
```

This proved the new policy did not break the normal pipeline path.

---

## 9. Airflow 3.3 list-runs syntax note

The installed Airflow CLI uses the DAG ID positionally.

Working command:

```cmd
docker compose exec airflow-api-server airflow dags list-runs ^
  retailpulse_warehouse_pipeline
```

---

## 10. Real transient retry proof

Goal:

```text
dependency outage
→ first attempt fails
→ Airflow retries
→ dependency restored
→ same DAG run succeeds
```

Selected:

```text
start_pipeline_run
```

because it writes to Postgres and has:

```text
retries=2
retry_delay=1 minute
```

### Stop Postgres

```cmd
docker compose stop postgres
```

### Trigger proof run

```cmd
docker compose exec airflow-api-server airflow dags trigger ^
  -r session24_retry_proof_001 ^
  retailpulse_warehouse_pipeline
```

The first `start_pipeline_run` attempt failed because Postgres was unavailable.

### Restore Postgres before retry

```cmd
docker compose start postgres
```

Verify:

```cmd
docker compose exec postgres pg_isready -U retailpulse -d retailpulse
```

Expected:

```text
accepting connections
```

### Result

The retry succeeded and the same DAG run completed successfully:

```text
session24_retry_proof_001
→ success
```

This proves the retry policy works in a real transient outage rather than only existing as configuration.

---

## 11. Health failure semantics

The health evaluator returns:

```text
HEALTHY
WARNING
DEGRADED
```

The script maps those to process exit behaviour:

```text
HEALTHY
→ exit 0

WARNING
→ exit 0

DEGRADED
→ exit 1
```

Because Airflow runs the checker through `BashOperator`:

```text
exit 0
→ task SUCCESS

exit 1
→ task FAILED
```

Therefore a genuine `DEGRADED` result intentionally becomes an Airflow task failure.

Session 24 sets:

```text
check_pipeline_health retries=0
```

so that decision is not pointlessly repeated.

---

## 12. DEGRADED operational flow

For a genuine health degradation:

```text
check_pipeline_health
        ↓
DEGRADED
        ↓
control.pipeline_metrics records health snapshot
        ↓
control.pipeline_incidents opens/updates incident
        ↓
new incident sends alert email
        ↓
health script exits 1
        ↓
Airflow marks health task FAILED
        ↓
record_pipeline_failure runs via ONE_FAILED
        ↓
control.pipeline_runs marked FAILED
        ↓
failed task captured in error_message
        ↓
DAG run FAILED
```

Responsibility split:

```text
pipeline_metrics
→ what the pipeline/data state looked like

pipeline_incidents
→ operational incident lifecycle

pipeline_runs
→ which Airflow run failed and where

email
→ notify human operator
```

Nuance:

```text
if the health task itself crashes before it can evaluate/persist health,
Airflow can still record a failed run,
but a health incident/email is not guaranteed
```

---

## 13. Final regression gate

Tests:

```cmd
pytest -v
```

Observed:

```text
70 passed in 10.29s
```

Lint:

```cmd
ruff check .
```

Observed:

```text
All checks passed!
```

Strict health:

```cmd
python -m warehouse.monitoring.check_pipeline_health --strict
```

Observed:

```text
Bronze rows:        3137
Silver rows:        3132
Silver unique:      3130
Silver duplicates:  2
Quarantine rows:    5
Raw orders:         3130
Fact orders:        3130
Gold order count:   3130
Load age:           556 minutes

Status: HEALTHY
```

Exact reconciliation:

```text
3137 = 3132 + 5
3130 = 3130 = 3130 = 3130
3132 - 3130 = 2
```

Airflow import check:

```cmd
docker compose exec airflow-api-server airflow dags list-import-errors
```

Observed:

```text
No data found
```

---

## 14. Before vs after

### Before

```text
most tasks
→ retries=2
→ max 3 attempts regardless of failure type

no task execution timeout
→ hung task could run indefinitely
```

### After

```text
retry only where recovery is realistic

loader / DB write tasks
→ bounded retry

dbt
→ one retry

health DEGRADED
→ fail immediately

failure recorder
→ records once
→ no retry

all operational tasks
→ execution timeout
```

Concise engineering statement:

> RetailPulse now has task-specific retry budgets and execution timeouts based on failure semantics, replacing the previous blanket retry policy and unbounded task runtime.

---

## 15. Validation checklist

```text
[x] global retry policy removed
[x] retry policy explicit per task
[x] retry delay explicit where retries are used
[x] check_pipeline_health retries=0
[x] record_pipeline_failure retries=0
[x] execution timeout on every operational task
[x] Airflow parsed retry values correctly
[x] Airflow parsed timeout values correctly
[x] ONE_FAILED preserved
[x] normal DAG run succeeded
[x] real transient Postgres outage recovered by retry
[x] 70 tests pass
[x] Ruff clean
[x] strict health HEALTHY
[x] no DAG import errors
```

---

## 16. Git update

Copy this runbook into:

```text
docs/sessions/session_24_runbook.md
```

Inspect:

```cmd
git status
git diff
```

Expected Session 24 code change:

```text
airflow/dags/retailpulse_warehouse_pipeline.py
docs/sessions/session_24_runbook.md
```

Do not stage:

```text
.env
Mailtrap credentials
other secrets
```

Stage:

```cmd
git add airflow\dags\retailpulse_warehouse_pipeline.py
git add docs\sessions\session_24_runbook.md
```

Review:

```cmd
git status
git diff --cached
```

Commit:

```cmd
git commit -m "Define Airflow retry and timeout policy"
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

Session 24 complete.

---

## 17. Next planned session

```text
Session 25 — Secrets & Environment Hardening
```

Focus:

```text
local Windows
Docker Compose
Airflow
GitHub Actions
.env.example
required-variable validation
secret-safe logs
environment contract
```
