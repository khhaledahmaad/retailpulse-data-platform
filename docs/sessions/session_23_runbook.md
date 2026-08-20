# Session 23 Runbook — Live Monitoring Fixes & Config-Driven Monitoring

## Session goal

Session 23 originally targeted centrally configurable monitoring thresholds without changing existing behaviour.

During live pipeline validation, a monitoring race was discovered first, so the session was split conceptually into:

```text
Session 23.0
→ fix live monitoring snapshot / alert semantics

Session 23.1–23.4
→ centralise and validate monitoring configuration
```

The final architecture is:

```text
.env / environment
        ↓
MonitoringConfig
        ↓
health thresholds
        ↓
health evaluation
        ↓
metrics / incidents / alerts
```

Default behaviour remains unchanged unless configuration is overridden.

---

## 1. Baseline monitoring policy

Existing monitoring thresholds were:

```text
MAX_LAG_ROWS=60
MAX_LOAD_AGE_MINUTES=2880
```

These were present in:

```text
.env.example
docker-compose.yml
warehouse/monitoring/check_pipeline_health.py
```

The normal Airflow health check is non-strict.

Strict mode remains available with:

```cmd
python -m warehouse.monitoring.check_pipeline_health --strict
```

---

## 2. Live monitoring anomaly discovered

During a continuous live run with the producer, Spark streaming and scheduled Airflow active, health snapshots occasionally showed impossible values such as:

```text
Silver rows:       2047
Silver unique:     2060
Silver duplicates: -13
```

Other live snapshots also showed temporary cross-layer gaps such as:

```text
Bronze != Silver + Quarantine
Silver unique != Raw
```

After stopping the producer and allowing the pipeline to catch up, exact reconciliation returned:

```text
Bronze = Silver + Quarantine
Silver unique = Raw = Fact = Gold
Silver duplicates >= 0
Status: HEALTHY
```

This proved the data pipeline itself was consistent and the issue was in monitoring semantics / snapshot timing rather than data loss.

---

## 3. Root cause — Silver metrics used different snapshots

In:

```text
warehouse/monitoring/check_pipeline_health.py
```

the old implementation independently called:

```python
get_committed_files(SILVER_ROOT)
```

for the Silver row count and Silver unique event count.

While Spark was live, a micro-batch could commit between those calls. That meant Silver rows could be calculated from an older committed-file set while Silver unique could be calculated from a newer committed-file set.

This could produce:

```text
Silver unique > Silver rows
Silver duplicates < 0
```

---

## 4. Add immutable file-list counters

Added reusable helpers in:

```text
warehouse/monitoring/check_pipeline_health.py
```

```python
def count_rows_from_files(
    committed_files: list[Path],
) -> int:
    ...
```

and:

```python
def count_unique_values_from_files(
    committed_files: list[Path],
    column: str,
) -> int:
    ...
```

The existing convenience wrappers remain, preserving compatibility while allowing multiple metrics to reuse one immutable committed-file snapshot.

---

## 5. Fix collect_lake_metrics()

`collect_lake_metrics()` now snapshots each sink once:

```python
bronze_files = get_committed_files(BRONZE_ROOT)
silver_files = get_committed_files(SILVER_ROOT)
quarantine_files = get_committed_files(QUARANTINE_ROOT)
```

Silver rows and Silver unique events are both calculated from the same `silver_files` list.

This guarantees:

```text
0 <= Silver unique <= Silver rows
Silver duplicates >= 0
```

Negative Silver duplicate counts should therefore never recur.

---

## 6. Add regression test for one Silver snapshot

Added:

```text
test_collect_lake_metrics_uses_one_silver_snapshot
```

in:

```text
warehouse/tests/test_pipeline_health.py
```

The test proves `SILVER_ROOT` is requested exactly once by `collect_lake_metrics()`.

---

## 7. Clarify cross-sink live lag

Bronze, Silver and Quarantine are separate Spark Structured Streaming queries with separate checkpoints.

```text
                 Kafka
              /    |     \
         Bronze  Silver  Quarantine
```

Therefore arbitrary wall-clock snapshots are not transactionally atomic across the three sinks.

A live health scan may observe Bronze at one instant and Silver/Quarantine slightly later while Spark is still progressing.

No Spark redesign, locking or retry mechanism was added.

The important invariant remains:

```text
when caught up / strict validation:
Bronze = Silver + Quarantine
```

---

## 8. Silver → Raw live lag semantics

During active production, Spark can continue writing Silver after the Airflow loader has already captured its load state.

Therefore a positive:

```text
Silver unique - Raw
```

gap within tolerance is normal operational lag.

A negative Silver→Raw gap remains suspicious because it means Raw is ahead of the currently visible Silver business state.

---

## 9. Refine WARNING vs incident semantics

Previously, tolerated reconciliation mismatches could still emit `incident_types`, which meant a normal live WARNING could open `control.pipeline_incidents` and send an alert.

The health semantics were refined.

### Normal non-strict Airflow run

```text
gap = 0
→ HEALTHY
→ no incident
→ no alert
```

```text
gap within configured tolerance
→ WARNING
→ issue remains visible
→ recorded in pipeline_metrics
→ incident_types = []
→ no incident
→ no email
```

```text
gap beyond tolerance
→ DEGRADED
→ incident_type emitted
→ incident opens/updates
→ alert sent on first opening
```

### Strict mode

```text
--strict
→ any reconciliation gap is DEGRADED
→ incident_type emitted
```

---

## 10. Bronze incident logic

Bronze reconciliation now emits an incident only when:

```python
strict or abs(bronze_gap) > max_lag_rows
```

A tolerated Bronze live gap remains visible in `health["issues"]` but not in `health["incident_types"]`.

---

## 11. Silver → Raw incident logic

Silver → Raw emits an incident only when:

```python
strict
or silver_raw_gap < 0
or silver_raw_gap > max_lag_rows
```

Therefore:

```text
0 < Silver→Raw gap <= tolerance
```

is a WARNING only.

---

## 12. Incident reconciliation remains unchanged

`reconcile_incidents()` continues to use:

```python
active_types = set(health["incident_types"])
```

Therefore:

```text
WARNING + incident_types=[]
→ no incident insertion
→ no alert
```

and:

```text
DEGRADED + incident_types=[...]
→ incident lifecycle applies
```

No notifier redesign was required.

---

## 13. Monitoring threshold inventory

Windows `rg` was unavailable, so built-in `findstr` was used.

Inventory confirmed only two health-policy thresholds:

```text
MAX_LAG_ROWS
MAX_LOAD_AGE_MINUTES
```

Other environment variables were intentionally left outside this monitoring-policy configuration:

```text
POSTGRES_*                 connection settings
MAILTRAP_*                 notification transport
ALERT_EMAIL_*              notification transport
OPERATIONS_DASHBOARD_*     dashboard runtime
```

The dashboard 60-second refresh interval was also left unchanged because it is UI behaviour, not pipeline health policy.

---

## 14. Create central monitoring config

Created:

```text
warehouse/monitoring/config.py
```

with:

```python
@dataclass(frozen=True)
class MonitoringConfig:
    max_lag_rows: int = 60
    max_load_age_minutes: int = 2880
```

`load_monitoring_config()` reads:

```text
MAX_LAG_ROWS
MAX_LOAD_AGE_MINUTES
```

from the environment and rejects negative values.

---

## 15. Add monitoring config tests

Created:

```text
warehouse/tests/test_monitoring_config.py
```

Tests prove:

```text
default MAX_LAG_ROWS = 60
default MAX_LOAD_AGE_MINUTES = 2880
environment overrides are read
negative MAX_LAG_ROWS is rejected
negative MAX_LOAD_AGE_MINUTES is rejected
```

---

## 16. Wire MonitoringConfig into health checker

`check_pipeline_health.py` now imports:

```python
from warehouse.monitoring.config import (
    load_monitoring_config,
)
```

The project `.env` remains loaded first:

```python
load_dotenv(
    dotenv_path=PROJECT_ROOT / ".env",
    override=False,
)
```

Then:

```python
MONITORING_CONFIG = load_monitoring_config()
```

This order ensures `.env` values are available while preserving already-set process/container environment variables.

---

## 17. Remove duplicated threshold parsing

Direct health-policy parsing for:

```text
MAX_LAG_ROWS
MAX_LOAD_AGE_MINUTES
```

was removed from `check_pipeline_health.py`.

PostgreSQL connection environment parsing remains local because it is connection configuration, not monitoring policy.

---

## 18. Make evaluate_health() threshold-injectable

`evaluate_health()` now accepts:

```python
max_lag_rows=None
max_load_age_minutes=None
```

When not supplied, values come from `MONITORING_CONFIG`.

Warehouse freshness now compares against the injected/configured value rather than a hard-coded module constant.

---

## 19. Preserve CLI compatibility

The existing CLI remains:

```text
--strict
--max-lag-rows
```

No new freshness CLI flag was added.

`--max-lag-rows` now defaults to:

```python
MONITORING_CONFIG.max_lag_rows
```

This preserves the interface while making the default centrally configurable.

---

## 20. Add custom freshness threshold tests

Added two tests to:

```text
warehouse/tests/test_pipeline_health.py
```

A load older than a custom threshold produces `DEGRADED / WAREHOUSE_FRESHNESS`.

A load within the custom threshold remains `HEALTHY`.

These prove freshness policy is genuinely config-driven.

---

## 21. Full local quality gate

Observed:

```text
70 tests collected
70 passed
```

and:

```text
ruff check .
All checks passed!
```

---

## 22. Operational override proof

Temporary CMD overrides:

```cmd
set MAX_LAG_ROWS=25
set MAX_LOAD_AGE_MINUTES=120
```

Then:

```cmd
python -c "from warehouse.monitoring.config import load_monitoring_config; c=load_monitoring_config(); print('max_lag_rows =', c.max_lag_rows); print('max_load_age_minutes =', c.max_load_age_minutes)"
```

Observed:

```text
max_lag_rows = 25
max_load_age_minutes = 120
```

---

## 23. CLI configuration proof

With the temporary override active:

```cmd
python -m warehouse.monitoring.check_pipeline_health --help
```

reported:

```text
Maximum tolerated live row lag (default: 25)
```

This proves the health checker consumes the central configuration.

---

## 24. Restore default configuration

Temporary process overrides were cleared:

```cmd
set MAX_LAG_ROWS=
set MAX_LOAD_AGE_MINUTES=
```

Re-checking the config returned:

```text
max_lag_rows = 60
max_load_age_minutes = 2880
```

The original default behaviour was preserved.

---

## 25. Final strict health validation

Final command:

```cmd
python -m warehouse.monitoring.check_pipeline_health --strict
```

Observed:

```text
Bronze rows:       3137
Silver rows:       3132
Silver unique:     3130
Silver duplicates: 2
Quarantine rows:   5
Raw orders:        3130
Fact orders:       3130
Gold order count:  3130
Load age:          161 minutes

Status: HEALTHY
```

Exact invariants:

```text
3137 = 3132 + 5
3130 = 3130 = 3130 = 3130
3132 - 3130 = 2
```

---

## 26. Docker / .env runtime behaviour

`docker-compose.yml` injects `MAX_LAG_ROWS` and `MAX_LOAD_AGE_MINUTES` into the Airflow container environment when containers are created.

Because Python loads `.env` with:

```python
override=False
```

the running container environment takes precedence.

Therefore:

```text
edit .env while existing Airflow container is running
→ next Airflow health run still uses existing container values
```

To apply changed `.env` values:

```cmd
docker compose up -d --force-recreate
```

After recreation, subsequent scheduled Airflow health checks use the new settings.

This is intentionally not hot-reloaded.

---

## 27. Final operational semantics

### HEALTHY

```text
all required invariants reconcile
freshness within threshold
→ no incident
→ no email
```

### WARNING

```text
tolerated live reconciliation lag
→ visible in health output
→ persisted in pipeline_metrics
→ no incident_type
→ no incident row
→ no email
→ Airflow health command exits normally
```

### DEGRADED

```text
lag beyond tolerance
negative Silver→Raw gap
Raw != Fact
Fact != Gold
warehouse stale/missing
strict reconciliation failure
→ incident_type emitted
→ incident lifecycle applied
→ alert sent for newly opened incident
→ health command exits 1
```

---

## 28. Session 23 proven properties

```text
[x] Silver rows and unique counts use one immutable Silver file snapshot
[x] negative Silver duplicate counts cannot occur from monitoring race
[x] tolerated WARNING does not open an incident
[x] tolerated WARNING does not send an alert
[x] DEGRADED conditions still create incidents
[x] strict mode still requires exact reconciliation
[x] MAX_LAG_ROWS centrally configured
[x] MAX_LOAD_AGE_MINUTES centrally configured
[x] config object immutable
[x] config validates negative thresholds
[x] defaults remain 60 / 2880
[x] process environment overrides work
[x] CLI consumes configured lag default
[x] custom freshness threshold tested
[x] Docker recreation requirement understood
[x] 70 tests pass
[x] Ruff clean
[x] final strict health HEALTHY
```

---

## 29. Architectural outcome

Before:

```text
locally parsed health thresholds
        ↓
health checker

live mismatches
        ↓
WARNING
        ↓
could still become incidents / email alerts
```

After:

```text
.env / environment
        ↓
MonitoringConfig
        ↓
health checker
        ↓
HEALTHY / WARNING / DEGRADED
        ↓
WARNING
→ metrics + logs only

DEGRADED
→ incident lifecycle
→ alerting
```

Silver duplicate monitoring now uses one committed Silver file snapshot for both row count and unique-event count.

---

## 30. Precise engineering claim

> RetailPulse now uses immutable sink-local monitoring snapshots and centrally configurable health thresholds, distinguishes tolerated live-stream lag from alert-worthy degradation, suppresses incidents and alerts for normal WARNING states, and preserves exact strict-mode reconciliation for caught-up validation.

---

## 31. Git update

Inspect:

```cmd
git status
git diff
```

Expected Session 23 core files:

```text
warehouse/monitoring/config.py
warehouse/monitoring/check_pipeline_health.py
warehouse/tests/test_monitoring_config.py
warehouse/tests/test_pipeline_health.py
docs/sessions/session_23_runbook.md
```

Do not stage:

```text
.env
Mailtrap credentials
other secrets
```

Stage:

```cmd
git add warehouse\monitoring\config.py
git add warehouse\monitoring\check_pipeline_health.py
git add warehouse	ests	est_monitoring_config.py
git add warehouse	ests	est_pipeline_health.py
git add docs\sessions\session_23_runbook.md
```

If `.env.example` has an intentional non-secret change you want to retain, inspect it first and then optionally stage:

```cmd
git add .env.example
```

Review:

```cmd
git status
git diff --cached
```

Commit:

```cmd
git commit -m "Make pipeline monitoring config driven"
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

Session 23 complete.
