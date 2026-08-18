# Session 19 Runbook — SLO Breach Detection & Incident Tracking

## Session goal

Add a persistent operational incident lifecycle on top of the existing RetailPulse health monitor.

Before Session 19, the platform could evaluate and snapshot health as:

```text
HEALTHY
WARNING
DEGRADED
```

but it did not persist an incident lifecycle.

Session 19 adds:

```text
health issue appears
→ incident opens

issue remains active
→ same incident updates
→ no duplicate open incident

issue disappears
→ incident resolves

same issue happens again later
→ new historical incident
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
41 tests passed
Ruff clean
strict health HEALTHY
```

Existing health flow:

```text
collect lake metrics
→ collect database metrics
→ evaluate_health
→ record_metrics
→ print_report
→ exit 1 if DEGRADED
```

## 2. Add machine-readable incident types

Extend `evaluate_health()` so it returns stable incident identifiers alongside the existing human-readable issue strings.

Final incident vocabulary:

```text
BRONZE_RECONCILIATION
SILVER_RAW_RECONCILIATION
RAW_FACT_RECONCILIATION
FACT_GOLD_RECONCILIATION
WAREHOUSE_FRESHNESS
```

Mappings:

```text
Bronze != Silver + Quarantine
→ BRONZE_RECONCILIATION

Silver unique != raw.orders
→ SILVER_RAW_RECONCILIATION

raw.orders != analytics.fct_orders
→ RAW_FACT_RECONCILIATION

analytics.fct_orders != Gold order count
→ FACT_GOLD_RECONCILIATION

missing/stale latest load timestamp
→ WAREHOUSE_FRESHNESS
```

Extend the return value:

```python
return {
    "status": status,
    "issues": issues,
    "incident_types": incident_types,
    "load_age_minutes": load_age_minutes,
}
```

This preserves human-readable issue text alongside machine-readable incident identity.

## 3. Extend health tests

Existing health tests were expanded to assert the new machine-readable incident types.

Examples:

```python
assert result["status"] == "HEALTHY"
assert result["incident_types"] == []
```

```python
assert result["incident_types"] == ["BRONZE_RECONCILIATION"]
```

```python
assert result["incident_types"] == ["SILVER_RAW_RECONCILIATION"]
```

```python
assert result["incident_types"] == ["RAW_FACT_RECONCILIATION"]
```

```python
assert result["incident_types"] == ["FACT_GOLD_RECONCILIATION"]
```

```python
assert result["incident_types"] == ["WAREHOUSE_FRESHNESS"]
```

The total remained 41 tests at this stage because assertions were added to existing tests.

## 4. Create control.pipeline_incidents

Add to:

```text
warehouse/init/001_create_warehouse.sql
```

```sql
CREATE TABLE IF NOT EXISTS control.pipeline_incidents (
    incident_id BIGSERIAL PRIMARY KEY,
    incident_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    details TEXT NOT NULL,
    opened_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at TIMESTAMPTZ,
    opened_by_airflow_run_id TEXT,
    resolved_by_airflow_run_id TEXT,

    CONSTRAINT pipeline_incidents_severity_check
        CHECK (
            severity IN (
                'WARNING',
                'DEGRADED'
            )
        )
);

CREATE UNIQUE INDEX IF NOT EXISTS
    idx_pipeline_incidents_open_type
ON control.pipeline_incidents (incident_type)
WHERE resolved_at IS NULL;
```

This enforces one open incident per incident type while still allowing the same incident type to recur later after resolution.

## 5. Add reconcile_incidents()

Add to:

```text
warehouse/monitoring/check_pipeline_health.py
```

Core lifecycle:

```text
new active type
→ INSERT

already-open active type
→ UPDATE severity/details

open type no longer active
→ resolved_at = NOW()
```

The function reads all currently open incident types, compares them with `health["incident_types"]`, opens new incidents, refreshes still-active incidents, and resolves recovered ones. It commits the incident lifecycle transaction before the health command exits.

## 6. Hook incident reconciliation into health execution

After `record_metrics(...)`, call:

```python
reconcile_incidents(
    conn=conn,
    health=health,
)
```

The runtime flow becomes:

```text
collect metrics
→ evaluate health
→ record health snapshot
→ reconcile incidents
→ print report
→ exit 1 if DEGRADED
```

Incident persistence therefore happens before the DEGRADED exit.

## 7. Add three incident lifecycle unit tests

Three tests were added:

```text
test_reconcile_incidents_opens_new_incident
test_reconcile_incidents_updates_existing_incident
test_reconcile_incidents_resolves_recovered_incident
```

They prove:

```text
new issue
→ INSERT

same active issue
→ UPDATE
→ no duplicate INSERT

recovered issue
→ resolved_at populated
```

## 8. Test gate after lifecycle implementation

Run:

```cmd
pytest warehouse\tests\test_pipeline_health.py -v
pytest -v
ruff check .
```

Observed:

```text
44 tests passed
Ruff clean
```

## 9. Apply control.pipeline_incidents to running Postgres

Apply the new table/index and verify:

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "SELECT incident_id, incident_type, severity, opened_at, resolved_at FROM control.pipeline_incidents ORDER BY incident_id;"
```

Initial observed state:

```text
0 rows
```

## 10. Real operational proof — natural Silver → Raw lag

Rather than mutating warehouse rows, create a real lag:

```text
pause RetailPulse DAG
keep producer running
keep Spark streaming running
```

Pause:

```cmd
docker compose exec airflow-api-server airflow dags pause retailpulse_warehouse_pipeline
```

Natural behavior:

```text
producer → Kafka
Spark → Bronze/Silver
Airflow paused → loader does not run
Raw/Fact/Gold remain behind
```

With strict health:

```text
Silver unique > Raw
→ SILVER_RAW_RECONCILIATION
→ DEGRADED
```

This is a realistic downstream orchestration lag.

## 11. First real incident proof

Observed:

```text
incident_id: 1
incident_type: SILVER_RAW_RECONCILIATION
severity: DEGRADED
opened_at: 2026-08-18 20:54:07.021392+00
```

Repeated health checks while the lag remained active did not create a duplicate open incident.

## 12. Recovery proof

Resume orchestration and let the normal pipeline catch up.

Observed resolved row:

```text
incident_id: 1
incident_type: SILVER_RAW_RECONCILIATION
severity: DEGRADED
opened_at:   2026-08-18 20:54:07.021392+00
resolved_at: 2026-08-18 20:58:37.97625+00
```

The same row was resolved rather than duplicated.

## 13. Additional real incident testing

Additional scenarios were exercised by temporarily adjusting `.env` thresholds and other safe test conditions.

Final observed incident history:

```text
1  SILVER_RAW_RECONCILIATION  DEGRADED
   opened  2026-08-18 20:54:07.021392+00
   resolved 2026-08-18 20:58:37.97625+00

2  WAREHOUSE_FRESHNESS        DEGRADED
   opened  2026-08-18 21:02:53.326482+00
   resolved 2026-08-18 21:15:31.629957+00

3  SILVER_RAW_RECONCILIATION  DEGRADED
   opened  2026-08-18 21:05:16.206484+00
   resolved 2026-08-18 21:15:31.629957+00

4  BRONZE_RECONCILIATION      DEGRADED
   opened  2026-08-18 21:05:41.517881+00
   resolved 2026-08-18 21:06:19.026292+00

5  BRONZE_RECONCILIATION      DEGRADED
   opened  2026-08-18 21:07:07.929322+00
   resolved 2026-08-18 21:09:12.699217+00

6  RAW_FACT_RECONCILIATION    DEGRADED
   opened  2026-08-18 21:15:31.629957+00
   resolved 2026-08-18 21:20:11.58858+00

7  WAREHOUSE_FRESHNESS        DEGRADED
   opened  2026-08-18 21:18:01.88683+00
   resolved 2026-08-18 21:19:25.024989+00
```

This proves:

```text
different incident types open independently
active incidents do not duplicate
recovered incidents resolve automatically
same incident type can open again after prior resolution
multiple incident types can coexist and resolve independently
```

## 14. Final validation

Run:

```cmd
pytest -v
ruff check .
python warehouse\monitoring\check_pipeline_health.py --strict
git status
```

Observed:

```text
44 tests passed
Ruff clean
strict health HEALTHY
```

Temporary test thresholds were restored to normal, and all deliberately created incident conditions resolved.

## 15. Session 19 proven properties

```text
[x] health evaluator emits stable incident types
[x] health text remains human-readable
[x] incident table persists lifecycle history
[x] partial unique index prevents duplicate open incident type
[x] new incident opens on first detection
[x] active incident updates rather than duplicates
[x] resolved condition gets resolved_at
[x] historical incident rows are preserved
[x] same incident type can reopen after prior resolution
[x] multiple incident types can coexist
[x] real Silver → Raw lag proven
[x] real automatic recovery proven
[x] freshness incident proven
[x] Bronze reconciliation incident proven
[x] Raw → Fact reconciliation incident proven
[x] 44 tests pass
[x] Ruff clean
[x] strict health HEALTHY
```

## 16. Architectural outcome

Before:

```text
health snapshot
→ HEALTHY / WARNING / DEGRADED
```

After:

```text
health snapshot
        ↓
stable incident types
        ↓
control.pipeline_incidents
        ↓
OPEN
UPDATE WHILE ACTIVE
RESOLVE
REOPEN LATER AS NEW HISTORY
```

## 17. Precise engineering claim

> RetailPulse persists operational incident lifecycles from pipeline health signals, opening each incident type once while active, updating it without duplication, resolving it automatically on recovery, and preserving recurrence as new historical incidents.

## 18. Git update

Copy this runbook to:

```text
docs/sessions/session_19_runbook.md
```

Inspect:

```cmd
git status
git diff
```

Expected core Session 19 files:

```text
warehouse/init/001_create_warehouse.sql
warehouse/monitoring/check_pipeline_health.py
warehouse/tests/test_pipeline_health.py
docs/sessions/session_19_runbook.md
```

Stage:

```cmd
git add warehouse/init/001_create_warehouse.sql
git add warehouse/monitoring/check_pipeline_health.py
git add warehouse/tests/test_pipeline_health.py
git add docs/sessions/session_19_runbook.md
```

Review:

```cmd
git status
git diff --cached
```

Commit:

```cmd
git commit -m "Add pipeline incident lifecycle"
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

Session 19 complete.
