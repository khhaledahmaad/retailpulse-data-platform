# RetailPulse Data Platform — Session 21 Runbook

**Session:** 21  
**Topic:** Metrics Dashboard / Operations View  
**Date:** 19 August 2026  
**Status:** Complete

## Objective

Build a simple, operator-focused operations dashboard using the existing RetailPulse operational data:

- `control.pipeline_metrics`
- `control.pipeline_runs`
- `control.pipeline_incidents`

The dashboard should expose:

- current pipeline health,
- active incidents,
- recent pipeline runs,
- logical Silver → Raw lag,
- Silver duplicate deliveries,
- warehouse freshness,
- reconciliation state,
- metric history,
- operational trends.

No new dashboard framework or monitoring platform was introduced.

---

## Existing invariants preserved

RetailPulse keeps the distinction between physical delivery and logical business state:

```text
Kafka / Bronze / Silver
= physical at-least-once delivery

Raw / Fact / Gold
= logical business state
```

Therefore:

```text
Silver physical duplicates are allowed.
Business duplicates are not.
```

Logical reconciliation remains:

```text
Silver unique events
=
Raw orders
=
Fact orders
=
Gold SUM(order_count)
```

Dashboard lag must therefore use:

```text
silver_unique_events - raw_orders
```

and must **not** use:

```text
silver_rows - raw_orders
```

---

# Step 21.1 — Persist duplicate-aware Silver metrics

## Problem identified

`collect_lake_metrics()` already calculated:

```text
silver_rows
silver_unique_events
silver_duplicate_deliveries
```

but `control.pipeline_metrics` persisted only physical `silver_rows`.

That was insufficient for historical dashboard lag because physical Silver rows may legitimately contain duplicate deliveries.

## Schema change

Updated:

```text
warehouse/init/001_create_warehouse.sql
```

to add:

```sql
silver_unique_events BIGINT
```

to `control.pipeline_metrics`.

## Health metric persistence change

Updated:

```text
warehouse/monitoring/check_pipeline_health.py
```

so `record_metrics()` persists:

```text
silver_unique_events
```

alongside the existing metrics.

`silver_duplicate_deliveries` remains derived as:

```sql
silver_rows - silver_unique_events
```

## Existing database migration

Applied to the live PostgreSQL volume:

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "ALTER TABLE control.pipeline_metrics ADD COLUMN IF NOT EXISTS silver_unique_events BIGINT;"
```

Verified the table structure with:

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "\d control.pipeline_metrics"
```

## First duplicate-aware health snapshot

Ran:

```cmd
python -m warehouse.monitoring.check_pipeline_health --strict
```

Observed:

```text
Bronze rows:       1147
Silver rows:       1142
Silver unique:     1140
Silver duplicates: 2
Quarantine rows:   5
Raw orders:        1140
Fact orders:       1140
Gold order count:  1140
Status:             HEALTHY
```

This proves:

```text
Bronze 1147
= Silver 1142
+ Quarantine 5
```

and:

```text
Silver unique 1140
= Raw 1140
= Fact 1140
= Gold 1140
```

with:

```text
Silver physical duplicates = 2
Logical Silver → Raw lag = 0
```

## Metric history verification

Ran:

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "SELECT metric_id, recorded_at, silver_rows, silver_unique_events, silver_rows - silver_unique_events AS silver_duplicate_deliveries, raw_orders, silver_unique_events - raw_orders AS silver_raw_lag, status FROM control.pipeline_metrics ORDER BY metric_id DESC LIMIT 5;"
```

Confirmed that new snapshots populate `silver_unique_events`.

Older historical rows correctly remain:

```text
silver_unique_events = NULL
```

They were intentionally **not backfilled** from physical `silver_rows`, because historical Silver rows may contain legitimate duplicate deliveries.

---

# Step 21.2 — Operations data/query layer

Created:

```text
warehouse/monitoring/operations_view.py
```

Implemented:

```text
fetch_latest_metrics()
fetch_active_incidents()
fetch_recent_runs()
fetch_metric_history()
print_operations_view()
```

The module reads the existing operational tables without introducing a second metrics model.

## Key derived values

Duplicate delivery count:

```sql
silver_rows - silver_unique_events
```

Logical Silver → Raw lag:

```sql
silver_unique_events - raw_orders
```

Warehouse freshness:

```text
recorded_at - latest_loaded_at
```

Metric history intentionally filters to:

```sql
WHERE silver_unique_events IS NOT NULL
```

so pre-Session-21 snapshots without duplicate-aware metrics are not misrepresented.

## CLI validation

Ran:

```cmd
python -m warehouse.monitoring.operations_view
```

Observed:

```text
RetailPulse Operations View
---------------------------
Health:            HEALTHY
Bronze rows:       1147
Silver rows:       1142
Silver unique:     1140
Silver duplicates: 2
Quarantine rows:   5
Raw orders:        1140
Fact orders:       1140
Gold order count:  1140
Silver→Raw lag:    0
Freshness:         863 minutes

Active incidents:  0
```

Recent scheduled Airflow runs were returned as:

```text
SUCCEEDED
health=HEALTHY
```

## Lint validation

Ran:

```cmd
ruff check warehouse\monitoring\operations_view.py
```

Result:

```text
All checks passed!
```

---

# Step 21.3 — Browser operations dashboard

Created:

```text
warehouse/monitoring/operations_dashboard.py
```

## Design decision

No additional framework was introduced.

The dashboard uses:

```text
Python standard library
http.server
HTML
CSS
SVG
existing psycopg connection
```

Default endpoint:

```text
http://127.0.0.1:8084
```

Environment overrides remain available:

```text
OPERATIONS_DASHBOARD_HOST
OPERATIONS_DASHBOARD_PORT
```

No new package dependency was added.

## Dashboard contents

The final chosen dashboard contains five high-signal top cards:

```text
Current health
Active incidents
Silver→Raw lag
Silver duplicates
Freshness
```

Followed by:

```text
Layer reconciliation
Active incidents
Recent pipeline runs
Trends
Metric history
```

A later experiment added additional top cards for latest load, threshold context, and run summary, but these were deliberately reverted because they made the dashboard visually busier without enough additional operator value.

The cleaner five-card version is the final Session 21 design.

## HTTP error handling

Initial code caught:

```python
except Exception:
```

Ruff correctly flagged:

```text
BLE001
```

The handler was corrected to catch only:

```python
from psycopg import Error as PsycopgError
```

and:

```python
except PsycopgError:
```

This keeps database failures controlled while allowing genuine rendering/programming errors to remain visible.

## Browser validation

Ran:

```cmd
python -m warehouse.monitoring.operations_dashboard
```

Expected endpoint:

```text
RetailPulse Operations Dashboard: http://127.0.0.1:8084
```

Opened with:

```cmd
start http://127.0.0.1:8084
```

The dashboard rendered successfully.

---

# Step 21.4 — Trend visualisation and dashboard tests

## Initial trend implementation

Added SVG trend charts for:

```text
Silver→Raw logical lag
Warehouse freshness
```

Historical query order is:

```text
newest → oldest
```

For chart rendering it is intentionally reversed to:

```text
oldest → newest
```

so time progresses left-to-right.

## CSS f-string correction

Because the dashboard HTML is a Python f-string, CSS braces must be escaped as:

```text
{{ ... }}
```

The trend CSS was corrected accordingly.

## Interactive chart refinement

The original simple SVG polyline was improved without adding dependencies.

Final interactive chart behaviour includes:

```text
visible data points
hover/focus behaviour
timestamped SVG <title> tooltips
keyboard-focusable points
horizontal reference/grid lines
y-axis labels
start/end timestamps
snapshot count
min/max values
flat-series handling
```

Example logical lag behaviour:

```text
Silver→Raw logical lag = 0
```

may remain visually flat while still providing individual timestamped observations.

That is expected and proves ongoing logical reconciliation.

## Dashboard security

Dynamic incident detail text is HTML escaped using:

```python
html.escape()
```

so incident content cannot inject raw HTML/script markup into the rendered dashboard.

## Tests added

Created:

```text
warehouse/tests/test_operations_dashboard.py
```

Coverage includes:

```text
core operator metrics render
trend charts render
interactive chart elements exist
incident details are HTML escaped
empty / None trend history handled safely
```

The exact final pytest count was not captured in this runbook, but both the dashboard-specific tests and the complete repository test suite were reported green during Session 21.

---

# Final validation

The following validation sequence completed successfully:

```cmd
ruff check .
pytest -v warehouse\tests\test_operations_dashboard.py
pytest -v
python -m warehouse.monitoring.operations_dashboard
```

Results:

```text
Ruff: green
Dashboard tests: green
Full pytest suite: green
Browser rendering: working
Interactive charts: working
```

---

# Final Session 21 architecture

```text
control.pipeline_metrics
        │
        ├── current health
        ├── reconciliation
        ├── logical lag
        ├── duplicate deliveries
        ├── freshness
        └── historical trends
        │
        ▼
operations_view.py
        │
        ├── latest metrics
        ├── metric history
        ├── active incidents
        └── recent runs
        │
        ▼
operations_dashboard.py
        │
        ▼
http://127.0.0.1:8084
```

Other operational sources:

```text
control.pipeline_incidents
        ↓
active incidents view

control.pipeline_runs
        ↓
recent Airflow run lineage
```

---

# Session 21 outcome

Session 21 successfully added an operator-facing observability layer without changing the core pipeline semantics or adding unnecessary infrastructure.

The dashboard now answers:

```text
Is the pipeline healthy?
Are there active incidents?
Is logical Silver caught up with Raw?
Are physical duplicate deliveries occurring?
How fresh is the warehouse?
Do Bronze/Silver/Quarantine reconcile?
Do Raw/Fact/Gold reconcile?
Are recent Airflow runs succeeding?
How are lag and freshness changing over time?
```

## Important preserved semantics

Do not change:

```text
Silver physical duplicates are allowed.
Business duplicates are not.
```

Do not use:

```text
silver_rows - raw_orders
```

as business lag.

Continue using:

```text
silver_unique_events - raw_orders
```

for logical lag.

Gold reconciliation remains:

```sql
SELECT SUM(order_count)
FROM analytics.mart_daily_sales;
```

not Gold table row count.

---

# Files added / changed

Expected Session 21 repository changes:

```text
warehouse/init/001_create_warehouse.sql
warehouse/monitoring/check_pipeline_health.py
warehouse/monitoring/operations_view.py
warehouse/monitoring/operations_dashboard.py
warehouse/tests/test_operations_dashboard.py
docs/sessions/session_21_runbook.md
```

Review `git status` before staging in case unrelated files are also modified.

Never stage:

```text
.env
credentials
Mailtrap secrets
```

---

# Git update

From:

```text
C:\Users\khhal\retailpulse-data-platform
```

first inspect:

```cmd
git status
git diff
```

Stage only the intended Session 21 files:

```cmd
git add warehouse/init/001_create_warehouse.sql
git add warehouse/monitoring/check_pipeline_health.py
git add warehouse/monitoring/operations_view.py
git add warehouse/monitoring/operations_dashboard.py
git add warehouse/tests/test_operations_dashboard.py
git add docs/sessions/session_21_runbook.md
```

Verify staged content:

```cmd
git status
git diff --cached
```

Commit:

```cmd
git commit -m "Add operations metrics dashboard"
```

Push:

```cmd
git push origin main
```

Final verification:

```cmd
git status
```

Expected:

```text
On branch main
Your branch is up to date with 'origin/main'.

nothing to commit, working tree clean
```

---

# Next session

```text
Session 22 — Data Quality Rules Framework
```

Planned focus:

```text
required values
null checks
quantity constraints
price constraints
supported currency
category constraints
duplicate business keys
persisted quality outcomes where useful
```

Keep Session 22 small, production-aware, and compatible with the existing data-contract and quarantine model.
