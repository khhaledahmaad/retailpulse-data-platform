# Session 25 Runbook — Secrets & Environment Hardening

## Session goal

Harden RetailPulse runtime configuration and secret handling without introducing a new secrets manager or overengineering the local portfolio stack.

Primary goals:

- Remove live database/JWT credentials from tracked runtime configuration.
- Keep `.env` untracked and use `.env.example` as the committed configuration contract.
- Make required runtime secrets fail clearly when missing.
- Preserve convenient non-secret local defaults where appropriate.
- Make Python CLI tools, dbt, Docker Compose, and Airflow use a consistent environment-driven configuration model.
- Keep GitHub Actions independent of developer/private secrets.
- Validate all changes end to end.

## 25.1 — Environment and secret inventory

Reviewed environment-variable usage across the loader, health checker, notifier, dashboard, quarantine reprocessing tool, Airflow DAG, Compose, `.env.example`, and CI.

Sensitive values identified:

- `POSTGRES_PASSWORD`
- `AIRFLOW_DB_PASSWORD`
- `AIRFLOW_JWT_SECRET`
- `MAILTRAP_USERNAME`
- `MAILTRAP_PASSWORD`

Non-secret environment-specific values identified:

- `POSTGRES_DB`
- `POSTGRES_USER`
- `AIRFLOW_DB_NAME`
- `AIRFLOW_DB_USER`

Docker topology values such as container hostnames and internal ports intentionally remained in Compose.

Verified:

```cmd
git ls-files .env
```

returned no output, confirming `.env` is not tracked.

## 25.2 — Removed hard-coded warehouse credentials from the Airflow DAG

The DAG previously opened warehouse connections using repeated hard-coded literals.

Added a single environment-driven helper:

```python
def get_warehouse_connection():
    return psycopg.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
```

Replaced duplicated connections in `start_pipeline_run`, `validate_raw_orders`, `complete_pipeline_run`, and `record_pipeline_failure` with `get_warehouse_connection()`.

Required warehouse variables deliberately use `os.environ[...]` because the Airflow runtime must fail clearly if required DB configuration is absent.

Validation:

```cmd
ruff check airflow\dags\retailpulse_warehouse_pipeline.py
```

Result: `All checks passed!`

Airflow DAG import validation also remained clean:

```cmd
docker compose exec airflow-api-server airflow dags list-import-errors
```

Result: `No data found`

## 25.3 — Verified Airflow runtime environment

Confirmed the Airflow scheduler and DAG processor received the warehouse connection values from their container environment. Sensitive values were checked only as booleans and were not intentionally printed.

Confirmed:

- warehouse password present
- JWT secret present
- PostgreSQL warehouse identity present
- DAG processor imported the updated DAG successfully

## 25.4 — Moved runtime secrets and database identities to `.env`

Externalised:

```text
POSTGRES_DB
POSTGRES_USER
POSTGRES_PASSWORD
AIRFLOW_DB_NAME
AIRFLOW_DB_USER
AIRFLOW_DB_PASSWORD
AIRFLOW_JWT_SECRET
```

`.env.example` documents these with placeholders only.

```env
# RetailPulse warehouse
POSTGRES_DB=<YOUR_POSTGRES_DB>
POSTGRES_USER=<YOUR_POSTGRES_USER>
POSTGRES_PASSWORD=<YOUR_POSTGRES_PASSWORD>

# Airflow internal database/authentication
AIRFLOW_DB_NAME=<YOUR_AIRFLOW_DB_NAME>
AIRFLOW_DB_USER=<YOUR_AIRFLOW_DB_USER>
AIRFLOW_DB_PASSWORD=<YOUR_AIRFLOW_DB_PASSWORD>
AIRFLOW_JWT_SECRET=<YOUR_AIRFLOW_JWT_SECRET>
```

Actual values remain only in the untracked `.env`.

Compose now uses the environment for the warehouse service, Airflow metadata DB, Airflow SQLAlchemy connection, and both Postgres health checks.

Docker-specific hostnames such as `postgres` and `airflow-db` intentionally remain in Compose because they represent container topology rather than secrets.

## 25.5 — Recreated services and proved environment-driven runtime

Recreated the affected services:

```cmd
docker compose up -d --force-recreate ^
  postgres ^
  airflow-db ^
  airflow-api-server ^
  airflow-scheduler ^
  airflow-dag-processor
```

Observed both Postgres services healthy and Airflow API/scheduler/DAG processor running.

Validated both PostgreSQL services with `pg_isready`.

Validated Airflow DAG imports:

```cmd
docker compose exec airflow-api-server airflow dags list-import-errors
```

Result: `No data found`

A real DAG run was triggered with the new environment-driven configuration:

```text
session25_env_proof_001 → success
```

This proved:

```text
.env
→ Docker Compose interpolation
→ PostgreSQL services
→ Airflow runtime
→ environment-driven DAG warehouse connection
→ successful pipeline run
```

## 25.6 — Added fail-fast validation for required secrets

Core runtime secrets were changed to Docker Compose required-variable syntax:

```yaml
"${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}"
"${AIRFLOW_DB_PASSWORD:?AIRFLOW_DB_PASSWORD is required}"
"${AIRFLOW_JWT_SECRET:?AIRFLOW_JWT_SECRET is required}"
```

Controlled temporary env-file tests proved each missing value fails early with a clear error message.

Normal configuration continued to validate successfully afterward.

## 25.7 — Made optional alert configuration explicit

Mailtrap/email configuration remains optional for bringing up the core local development stack.

Compose values now use explicit empty defaults:

```yaml
MAILTRAP_HOST: "${MAILTRAP_HOST:-}"
MAILTRAP_PORT: "${MAILTRAP_PORT:-}"
MAILTRAP_USERNAME: "${MAILTRAP_USERNAME:-}"
MAILTRAP_PASSWORD: "${MAILTRAP_PASSWORD:-}"
ALERT_EMAIL_FROM: "${ALERT_EMAIL_FROM:-}"
ALERT_EMAIL_TO: "${ALERT_EMAIL_TO:-}"
```

This removes unnecessary warnings when alerting configuration is absent while preserving configured values when present.

## 25.8 — Hardened Python-side warehouse password handling

Three Python tools still contained a committed fallback password:

```python
os.getenv("POSTGRES_PASSWORD", "retailpulse")
```

Affected files:

- `warehouse/loader/load_orders.py`
- `warehouse/monitoring/check_pipeline_health.py`
- `warehouse/tools/reprocess_quarantine.py`

New rule:

```text
host / port / database / user
→ safe local defaults allowed

password
→ no code fallback
→ validate only when a DB connection is required
```

The relevant Python CLI tools now load project `.env` using `load_dotenv(..., override=False)` so local Windows execution works while container-provided environment variables still take precedence.

Password loading now uses `os.getenv("POSTGRES_PASSWORD")` and raises:

```python
RuntimeError("POSTGRES_PASSWORD is required")
```

immediately before connecting when absent.

This keeps module imports test-friendly while removing the committed password fallback.

## 25.9 — Hardened dbt profiles

A final runtime credential scan found hard-coded warehouse credentials in:

```text
warehouse/dbt/retailpulse/profiles.yml
```

Both dbt targets were updated to:

```yaml
user: "{{ env_var('POSTGRES_USER') }}"
password: "{{ env_var('POSTGRES_PASSWORD') }}"
dbname: "{{ env_var('POSTGRES_DB') }}"
```

Environment-specific hosts intentionally remain fixed:

```text
dev     → localhost
airflow → postgres
```

Likewise, port, schema, and threads remain fixed because they are not secrets.

Local dbt execution is run through dotenv because dbt does not automatically load `.env`:

```cmd
python -m dotenv run -- dbt debug ^
  --project-dir warehouse\dbt\retailpulse ^
  --profiles-dir warehouse\dbt\retailpulse ^
  --target dev
```

Result: green.

The Airflow dbt target also validated successfully because Compose injects the warehouse environment variables into Airflow.

## 25.10 — Repository secret hygiene review

Runtime scan across `airflow`, `warehouse`, `spark`, and `producer` found no remaining hard-coded live warehouse/Airflow password literals.

Tracked-file check:

```cmd
git ls-files | findstr /I /C:".env" /C:"__pycache__" /C:".pyc"
```

Result:

```text
.env.example
```

Therefore:

- `.env` is not tracked.
- no `__pycache__` is tracked.
- no `.pyc` files are tracked.

A fixed `retailpulse` password remains in `.github/workflows/ci.yml`. This is intentional: it belongs only to the disposable CI Postgres service and keeps CI independent of developer/private secrets.

## Important operational note — `docker compose config`

Running:

```cmd
docker compose config
```

renders interpolated environment values, including secrets, to stdout.

For future validation use:

```cmd
docker compose config -q
```

or:

```cmd
docker compose config > nul
```

## Final validation

### Tests

```cmd
pytest
```

Result:

```text
70 passed
```

### Ruff

```cmd
ruff check .
```

Result:

```text
All checks passed!
```

### Strict pipeline health

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
Status: HEALTHY
```

The established business reconciliation invariant remains intact:

```text
Silver unique events
= Raw orders
= Fact orders
= Gold SUM(order_count)
```

Physical Silver duplicate delivery remains allowed.

## Final Session 25 configuration model

```text
.env
├── runtime secrets
├── database identities
├── monitoring overrides
└── alerting configuration

.env.example
└── placeholder-only committed configuration contract

docker-compose.yml
├── Docker topology
├── environment wiring
├── required-secret validation
└── safe defaults for optional alert configuration

Python CLI tools
├── load project .env
├── preserve existing process env
└── require POSTGRES_PASSWORD only when connecting

Airflow
├── receives environment via Docker Compose
├── warehouse connection has no hard-coded password
└── metadata DB/JWT configuration comes from .env

dbt
├── dev target uses env_var(...)
└── airflow target uses env_var(...)

GitHub Actions
└── uses isolated disposable test credentials only
```

## Files changed

```text
.env.example
airflow/dags/retailpulse_warehouse_pipeline.py
docker-compose.yml
warehouse/dbt/retailpulse/profiles.yml
warehouse/loader/load_orders.py
warehouse/monitoring/check_pipeline_health.py
warehouse/tools/reprocess_quarantine.py
docs/sessions/session_25_runbook.md
```

## Session 25 outcome

**COMPLETE**

RetailPulse now has a clear and testable configuration boundary:

- real runtime secrets are not committed;
- required core secrets fail fast;
- `.env.example` defines the committed placeholder contract;
- Docker, Airflow, Python CLI tools, and dbt use environment-driven credentials;
- CI remains isolated from developer secrets;
- tests, linting, dbt, Airflow and strict health validation all remain green.

Next planned session:

**Session 26 — Container Health Checks / Service Readiness**
