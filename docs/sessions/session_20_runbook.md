# Session 20 Runbook — Alerting & Incident Notification

## Session goal

Add human-facing alerting on top of the Session 19 incident lifecycle.

Target behavior:

```text
incident opens
→ send one alert email
→ persist alert_sent_at

incident remains active
→ do not resend alert

incident resolves
→ send one recovery email
→ persist recovery_sent_at
```

Mailtrap is used as a safe dummy SMTP inbox for end-to-end email proof.

## 1. Baseline

Session 19 already provided:

```text
health issue appears
→ incident opens

same issue remains active
→ same incident updates
→ no duplicate incident

issue disappears
→ incident resolves
```

Session 20 extends that with notification state.

## 2. Add notification columns

Extend `control.pipeline_incidents` in:

```text
warehouse/init/001_create_warehouse.sql
```

with:

```sql
alert_sent_at TIMESTAMPTZ,
recovery_sent_at TIMESTAMPTZ,
```

Apply to the running database:

```sql
ALTER TABLE control.pipeline_incidents
ADD COLUMN IF NOT EXISTS alert_sent_at TIMESTAMPTZ;

ALTER TABLE control.pipeline_incidents
ADD COLUMN IF NOT EXISTS recovery_sent_at TIMESTAMPTZ;
```

Verify:

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "\d control.pipeline_incidents"
```

## 3. Mailtrap environment variables

Use Mailtrap SMTP credentials via `.env`.

```text
MAILTRAP_HOST=<mailtrap smtp host>
MAILTRAP_PORT=2525
MAILTRAP_USERNAME=<filled locally>
MAILTRAP_PASSWORD=<filled locally>
ALERT_EMAIL_FROM=data.admin@retailpulse.com
ALERT_EMAIL_TO=data.team@retailpulse.com
```

Do not commit `.env` or Mailtrap credentials.

## 4. Create notifier module

Create:

```text
warehouse/monitoring/notifier.py
```

Main interface:

```python
send_email(...)
send_incident_alert(...)
send_recovery_alert(...)
```

Load `.env` from the explicit project root:

```python
PROJECT_ROOT = Path(__file__).resolve().parents[2]

load_dotenv(
    dotenv_path=PROJECT_ROOT / ".env",
    override=False,
)
```

## 5. Standalone Mailtrap smoke test

Run:

```cmd
python -c "from warehouse.monitoring.notifier import send_incident_alert; send_incident_alert(incident_type='TEST_INCIDENT', severity='WARNING', details='Session 20 Mailtrap smoke test')"
```

Observed:

```text
Mailtrap test email received successfully
```

This independently proved `.env` loading, SMTP connectivity/authentication, TLS, message construction, and Mailtrap capture.

## 6. Wire alert notification into incident opening

`check_pipeline_health.py` imports:

```python
from warehouse.monitoring.notifier import (
    send_incident_alert,
    send_recovery_alert,
)
```

For a new incident:

```text
INSERT incident
→ send_incident_alert(...)
→ set alert_sent_at = NOW()
```

For an already-open incident:

```text
UPDATE severity/details
→ no email
```

This is the alert anti-spam path.

## 7. Wire recovery notification into incident resolution

When an open incident disappears:

```text
resolve exact incident row
→ send_recovery_alert(...)
→ set recovery_sent_at = NOW()
```

## 8. Make recovery row-specific

The open-incident lookup now returns:

```text
incident_id + incident_type
```

Conceptually:

```python
currently_open = {
    incident_type: incident_id
    for incident_id, incident_type in cur.fetchall()
}

open_types = set(currently_open)
```

Correct resolution SQL:

```sql
UPDATE control.pipeline_incidents
SET
    resolved_at = NOW(),
    resolved_by_airflow_run_id = %s
WHERE
    incident_id = %s
    AND resolved_at IS NULL;
```

Then:

```sql
UPDATE control.pipeline_incidents
SET recovery_sent_at = NOW()
WHERE incident_id = %s
  AND resolved_at IS NOT NULL
  AND recovery_sent_at IS NULL;
```

This prevents older historical incidents of the same type from being modified.

## 9. Update existing fake DB tests

Because the query now returns:

```text
(incident_id, incident_type)
```

fake rows changed from:

```python
[("BRONZE_RECONCILIATION",)]
```

to:

```python
[(1, "BRONZE_RECONCILIATION")]
```

and from:

```python
[("SILVER_RAW_RECONCILIATION",)]
```

to:

```python
[(2, "SILVER_RAW_RECONCILIATION")]
```

The recovery assertion now expects the incident ID:

```python
assert resolutions == [
    (
        "recovery_run_001",
        2,
    )
]
```

The temporary pytest failures were stale test-fixture shape failures, not email-delivery failures.

## 10. Add three notification tests

Add tests for:

```text
new incident
→ send_incident_alert called once

existing active incident
→ no send_incident_alert call

resolved incident
→ send_recovery_alert called once
```

Total test count becomes:

```text
47 tests
```

## 11. Standardize health checker invocation

After adding:

```python
from warehouse.monitoring.notifier import ...
```

direct nested-file execution can fail:

```cmd
python warehouse\monitoring\check_pipeline_health.py --strict
```

with:

```text
ModuleNotFoundError: No module named 'warehouse'
```

Use module execution instead:

```cmd
python -m warehouse.monitoring.check_pipeline_health --strict
```

## 12. Fix Airflow health task invocation

Airflow previously ran:

```text
cd /opt/retailpulse && python warehouse/monitoring/check_pipeline_health.py
```

and failed with `ModuleNotFoundError`.

Change the BashOperator to:

```python
bash_command=(
    "cd /opt/retailpulse && "
    "python -m warehouse.monitoring.check_pipeline_health"
),
```

or preserve strict mode if configured:

```python
bash_command=(
    "cd /opt/retailpulse && "
    "python -m warehouse.monitoring.check_pipeline_health --strict"
),
```

## 13. Airflow DAG pause/unpause commands

Keep these commands for operational testing.

Pause:

```cmd
docker compose exec airflow-api-server airflow dags pause retailpulse_warehouse_pipeline
```

Unpause:

```cmd
docker compose exec airflow-api-server airflow dags unpause retailpulse_warehouse_pipeline
```

These are useful for creating realistic Silver→Raw lag while Kafka/Spark ingestion continues.

## 14. Real end-to-end notification proof

Intentional strict-health state:

```text
Bronze rows:       1003
Silver rows:       999
Silver unique:     997
Silver duplicates: 2
Quarantine rows:   5
Raw orders:        882
Fact orders:       882
Gold order count:  882
Latest load:       2026-08-18 21:14:50.868941+00:00
Load age:          127 minutes

Status: DEGRADED
```

Active incidents:

```text
BRONZE_RECONCILIATION
SILVER_RAW_RECONCILIATION
WAREHOUSE_FRESHNESS
```

Observed Mailtrap behavior:

```text
3 new incident types
→ 3 alert emails

same incidents remain active
→ no repeat emails

incident resolves
→ one recovery email for that incident
```

Each email is keyed to one incident type but intentionally includes the full current pipeline issue context.

## 15. Notification persistence proof

Query:

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "SELECT incident_id, incident_type, severity, opened_at, alert_sent_at, resolved_at, recovery_sent_at FROM control.pipeline_incidents ORDER BY incident_id DESC LIMIT 10;"
```

Observed recent rows included:

```text
11 | BRONZE_RECONCILIATION
10 | WAREHOUSE_FRESHNESS
 9 | SILVER_RAW_RECONCILIATION
 8 | BRONZE_RECONCILIATION
```

All had:

```text
alert_sent_at populated
resolved_at populated
recovery_sent_at populated
```

This proves notification state is persisted against incident history.

## 16. Final validation

Run:

```cmd
pytest -v
ruff check .
python -m warehouse.monitoring.check_pipeline_health --strict
git status
```

Observed:

```text
47 tests passed
Ruff clean
strict health HEALTHY
```

## 17. Session 20 proven properties

```text
[x] Mailtrap SMTP transport works
[x] credentials stay in .env
[x] dummy alert identities configured
[x] notifier separated from health logic
[x] one alert sent for newly opened incident
[x] no repeated alert while incident remains active
[x] one recovery email sent on resolution
[x] alert_sent_at persisted
[x] recovery_sent_at persisted
[x] exact incident row targeted by incident_id
[x] historical incidents protected
[x] existing lifecycle tests updated
[x] three notification tests added
[x] Airflow health task uses module execution
[x] local health command standardized to python -m
[x] multiple simultaneous incident alerts proven
[x] repeated active incidents do not spam
[x] recovery emails proven
[x] 47 tests pass
[x] Ruff clean
[x] strict health HEALTHY
```

## 18. Architectural outcome

```text
health signal
        ↓
incident lifecycle
        ↓
new incident
→ Mailtrap alert
→ alert_sent_at

active incident
→ no repeat alert

resolved incident
→ Mailtrap recovery
→ recovery_sent_at
```

The incident table remains the persistent operational source of truth. A future dashboard can read the same incident and notification timestamps directly.

## 19. Precise engineering claim

> RetailPulse implements incident-aware email alerting with Mailtrap SMTP, sending one notification when an operational incident opens, suppressing duplicate alerts while it remains active, sending one recovery notification when it resolves, and persisting notification timestamps against the exact incident record.

## 20. Git update

Copy this runbook to:

```text
docs/sessions/session_20_runbook.md
```

Inspect:

```cmd
git status
git diff
```

Expected core Session 20 files:

```text
airflow/dags/retailpulse_warehouse_pipeline.py
warehouse/init/001_create_warehouse.sql
warehouse/monitoring/check_pipeline_health.py
warehouse/monitoring/notifier.py
warehouse/tests/test_pipeline_health.py
docs/sessions/session_20_runbook.md
```

Stage:

```cmd
git add airflow/dags/retailpulse_warehouse_pipeline.py
git add warehouse/init/001_create_warehouse.sql
git add warehouse/monitoring/check_pipeline_health.py
git add warehouse/monitoring/notifier.py
git add warehouse/tests/test_pipeline_health.py
git add docs/sessions/session_20_runbook.md
```

Review:

```cmd
git status
git diff --cached
```

Important:

```text
Do not stage .env.
Do not commit Mailtrap credentials.
```

Commit:

```cmd
git commit -m "Add incident email alerting"
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

Session 20 complete.
