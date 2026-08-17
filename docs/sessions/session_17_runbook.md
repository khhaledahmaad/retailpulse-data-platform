# Session 17 Runbook — Operational Metadata & Failure Traceability

## Session goal

Make quarantine remediation actions persist as first-class operational metadata.

Before this session, remediation was auditable through console output and downstream state.

After this session, every remediation attempt is queryable from PostgreSQL:

```text
Quarantine
    ↓
Remediation CLI
    ↓
validation
    ↓
control.event_reprocessing_log
    ↓
Kafka publish
    ↓
normal pipeline
```

The audit trail records:

```text
what event was remediated
why it originally failed
what correction was applied
whether the action was dry-run or publish
whether the publish succeeded or failed
Kafka topic / partition / offset for successful publishes
exact failure message for failed publishes
```

---

# 1. Baseline

Start services:

```cmd
docker compose up -d
```

Observed:

```text
11/11 services up
retailpulse-airflow-db healthy
retailpulse-postgres healthy
Kafka / Spark / Airflow services running
```

Check Git:

```cmd
git status
```

Observed:

```text
On branch main
Your branch is up to date with 'origin/main'.

nothing to commit, working tree clean
```

Run tests:

```cmd
pytest -v
```

Observed:

```text
37 passed
```

Run lint:

```cmd
ruff check .
```

Observed:

```text
All checks passed!
```

Run strict health:

```cmd
python warehouse\monitoring\check_pipeline_health.py --strict
```

Observed baseline:

```text
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

---

# 2. Locate control-schema DDL

Search:

```cmd
findstr /S /N /I "pipeline_metrics" *.sql
```

Observed:

```text
warehouse\init\001_create_warehouse.sql:52:CREATE TABLE IF NOT EXISTS control.pipeline_metrics (
```

Therefore Session 17 extends:

```text
warehouse/init/001_create_warehouse.sql
```

rather than introducing a new SQL location.

---

# 3. Create the remediation audit table

Add:

```sql
CREATE TABLE IF NOT EXISTS control.event_reprocessing_log (
    reprocessing_id BIGSERIAL PRIMARY KEY,

    event_id UUID NOT NULL,
    order_id TEXT,

    original_contract_error TEXT,
    original_validation_error TEXT,
    original_kafka_timestamp TIMESTAMPTZ,

    corrections JSONB NOT NULL,

    action TEXT NOT NULL,
    status TEXT NOT NULL,

    republished_topic TEXT,
    republished_partition INTEGER,
    republished_offset BIGINT,

    error_message TEXT,

    created_at TIMESTAMPTZ NOT NULL
        DEFAULT NOW(),

    CONSTRAINT event_reprocessing_action_check
        CHECK (action IN ('DRY_RUN', 'PUBLISH')),

    CONSTRAINT event_reprocessing_status_check
        CHECK (
            status IN (
                'DRY_RUN',
                'PUBLISHED',
                'PUBLISH_FAILED'
            )
        )
);
```

Add index:

```sql
CREATE INDEX IF NOT EXISTS
    idx_event_reprocessing_log_event_id
ON control.event_reprocessing_log (event_id);
```

The table belongs in:

```text
retailpulse-postgres
```

not the Airflow metadata database.

---

# 4. Apply table to the running database

Because Docker init SQL only applies during initial database creation, apply the new DDL to the already-running warehouse database:

```cmd
docker compose exec -T postgres psql ^
  -U retailpulse ^
  -d retailpulse ^
  -c "CREATE TABLE IF NOT EXISTS control.event_reprocessing_log (reprocessing_id BIGSERIAL PRIMARY KEY, event_id UUID NOT NULL, order_id TEXT, original_contract_error TEXT, original_validation_error TEXT, original_kafka_timestamp TIMESTAMPTZ, corrections JSONB NOT NULL, action TEXT NOT NULL, status TEXT NOT NULL, republished_topic TEXT, republished_partition INTEGER, republished_offset BIGINT, error_message TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), CONSTRAINT event_reprocessing_action_check CHECK (action IN ('DRY_RUN', 'PUBLISH')), CONSTRAINT event_reprocessing_status_check CHECK (status IN ('DRY_RUN', 'PUBLISHED', 'PUBLISH_FAILED'))); CREATE INDEX IF NOT EXISTS idx_event_reprocessing_log_event_id ON control.event_reprocessing_log (event_id);"
```

Verify:

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "\d control.event_reprocessing_log"
```

Initial row count:

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "SELECT COUNT(*) FROM control.event_reprocessing_log;"
```

Expected initially:

```text
0
```

---

# 5. Add PostgreSQL audit support to remediation CLI

File:

```text
warehouse/tools/reprocess_quarantine.py
```

Add PostgreSQL connection configuration using environment variables:

```text
POSTGRES_HOST
POSTGRES_PORT
POSTGRES_DB
POSTGRES_USER
POSTGRES_PASSWORD
```

Add:

```python
get_connection()
```

using `psycopg.connect(...)`.

Add:

```python
record_reprocessing_attempt(...)
```

which inserts into:

```text
control.event_reprocessing_log
```

and returns:

```text
reprocessing_id
```

Fields persisted:

```text
event_id
order_id
original_contract_error
original_validation_error
original_kafka_timestamp
corrections
action
status
republished_topic
republished_partition
republished_offset
error_message
```

The function commits the audit row before returning.

---

# 6. Audit-write unit tests

Extend:

```text
warehouse/tests/test_reprocess_quarantine.py
```

Add fake-connection tests proving:

```text
PUBLISHED record
DRY_RUN record
PUBLISH_FAILED record
```

without needing a real PostgreSQL instance.

The tests verify:

```text
correct event identity
correct failure metadata
correct action/status
successful publish metadata
NULL publish metadata for dry-run/failure
error_message for failures
commit called
returned reprocessing_id
```

Intermediate validation:

```cmd
pytest warehouse\tests\test_reprocess_quarantine.py -v
pytest -v
ruff check .
```

Observed before final CLI wiring:

```text
14 remediation tests passed
39 total tests passed
Ruff clean
```

---

# 7. Wire audit persistence into CLI behavior

Final desired CLI semantics:

```text
valid dry-run
→ audit DRY_RUN

successful --publish
→ audit PUBLISHED
→ include Kafka topic / partition / offset

failed --publish
→ audit PUBLISH_FAILED
→ include exact error_message
→ re-raise original exception
```

This keeps audit persistence from masking operational failures.

---

# 8. Dry-run audit path

Update dry-run branch so it writes:

```text
action = DRY_RUN
status = DRY_RUN
```

with no Kafka metadata.

CLI still prints:

```text
DRY RUN
Nothing published.
```

and now also:

```text
Audit record: <id>
```

---

# 9. Successful publish audit path

Successful `--publish` writes:

```text
action = PUBLISH
status = PUBLISHED
republished_topic = orders
republished_partition = <Kafka partition>
republished_offset = <Kafka offset>
error_message = NULL
```

The CLI prints the resulting audit ID.

---

# 10. Failed publish audit path

Wrap Kafka publish in:

```text
try / except
```

If publishing fails:

```text
action = PUBLISH
status = PUBLISH_FAILED
republished_topic = NULL
republished_partition = NULL
republished_offset = NULL
error_message = str(exception)
```

Then re-raise the original exception.

This means:

```text
failure is persisted
and
the command still exits as a real failure
```

---

# 11. Final unit-test gate after CLI wiring

Run:

```cmd
pytest warehouse\tests\test_reprocess_quarantine.py -v
```

Observed:

```text
15 passed
```

Run:

```cmd
pytest -v
```

Observed:

```text
40 passed
```

Run:

```cmd
ruff check .
```

Observed:

```text
All checks passed!
```

---

# 12. Real DRY_RUN audit proof

Use the existing quarantined invalid-quantity event:

```text
event_id:
7fbec326-c180-41bd-8a7d-a24b0f35b68f

order_id:
ORD-DQ-BAD-QUANTITY

original_validation_error:
invalid_quantity

original quantity:
0

repair:
quantity = 2
```

Run:

```cmd
python -m warehouse.tools.reprocess_quarantine ^
  --event-id 7fbec326-c180-41bd-8a7d-a24b0f35b68f ^
  --set quantity=2
```

Observed:

```text
Contract validation: PASS
Data-quality validation: PASS

DRY RUN
Nothing published.
Audit record: 1
```

Observed audit row:

```text
reprocessing_id:              1
event_id:                     7fbec326-c180-41bd-8a7d-a24b0f35b68f
order_id:                     ORD-DQ-BAD-QUANTITY
original_validation_error:    invalid_quantity
corrections:                  {"quantity": 2}
action:                       DRY_RUN
status:                       DRY_RUN
republished_topic:            NULL
republished_partition:        NULL
republished_offset:           NULL
error_message:                NULL
```

---

# 13. Real PUBLISHED audit proof

Run:

```cmd
python -m warehouse.tools.reprocess_quarantine ^
  --event-id 7fbec326-c180-41bd-8a7d-a24b0f35b68f ^
  --set quantity=2 ^
  --publish
```

Observed:

```text
Contract validation: PASS
Data-quality validation: PASS

PUBLISHED
Topic:     orders
Partition: 0
Offset:    216
Audit record: 2
```

Observed second audit row:

```text
reprocessing_id:        2
event_id:               7fbec326-c180-41bd-8a7d-a24b0f35b68f
corrections:            {"quantity": 2}
action:                 PUBLISH
status:                 PUBLISHED
republished_topic:      orders
republished_partition:  0
republished_offset:     216
error_message:          NULL
```

This was another physical duplicate of an event already successfully remediated in Session 16.

---

# 14. Real PUBLISH_FAILED audit proof

Force only this CLI process to use an invalid Kafka bootstrap endpoint:

```cmd
set KAFKA_BOOTSTRAP_SERVERS=localhost:1 && python -m warehouse.tools.reprocess_quarantine ^
  --event-id 7fbec326-c180-41bd-8a7d-a24b0f35b68f ^
  --set quantity=2 ^
  --publish
```

Reset:

```cmd
set KAFKA_BOOTSTRAP_SERVERS=
```

Observed third audit row:

```text
reprocessing_id:        3
action:                 PUBLISH
status:                 PUBLISH_FAILED
republished_topic:      NULL
republished_partition:  NULL
republished_offset:     NULL
error_message:          KafkaTimeoutError: Unable to bootstrap from localhost:1
```

This proves failure traceability without disrupting the real Kafka broker.

---

# 15. Queryable operator history

The audit table can now answer:

```text
Which events were manually remediated?
Why did they originally fail?
What values were changed?
Was the action only a dry-run?
Was an event actually republished?
What Kafka offset was produced?
Did publishing fail?
What was the exact failure?
Was an event republished multiple times?
```

Example:

```sql
SELECT
    reprocessing_id,
    event_id,
    corrections,
    action,
    status,
    republished_topic,
    republished_partition,
    republished_offset,
    error_message,
    created_at
FROM control.event_reprocessing_log
ORDER BY created_at DESC;
```

---

# 16. Airflow database clarification

RetailPulse runs two separate PostgreSQL services:

```text
retailpulse-airflow-db
retailpulse-postgres
```

Responsibilities:

```text
retailpulse-airflow-db
→ Airflow metadata database
→ database service provided by us
→ schema/tables managed by Airflow
→ DAG runs
→ task instances
→ scheduling state
→ retries
→ execution history
→ XCom / connections / variables / internal metadata
```

```text
retailpulse-postgres
→ RetailPulse application/warehouse database
→ schemas designed by us
→ raw.*
→ analytics.*
→ control.*
```

Therefore `control.event_reprocessing_log` correctly belongs in `retailpulse-postgres`.

---

# 17. Final validation gate

Reset Kafka override if necessary:

```cmd
set KAFKA_BOOTSTRAP_SERVERS=
```

Run:

```cmd
pytest warehouse\tests\test_reprocess_quarantine.py -v
pytest -v
ruff check .
python warehouse\monitoring\check_pipeline_health.py --strict
git status
```

Observed:

```text
15 remediation tests passed
40 total tests passed
Ruff clean
strict pipeline health HEALTHY
```

---

# 18. Session 17 proven properties

```text
[x] remediation attempts persist to PostgreSQL
[x] audit table is in control schema
[x] audit table is separate from Airflow metadata DB
[x] event_id is persisted
[x] original failure metadata is persisted
[x] corrections are persisted as JSONB
[x] dry-run actions are persisted
[x] successful publish actions are persisted
[x] successful Kafka topic is persisted
[x] successful Kafka partition is persisted
[x] successful Kafka offset is persisted
[x] publish failures are persisted
[x] publish failures preserve exact error messages
[x] failed publishes do not fabricate Kafka metadata
[x] failed publishes still raise the original exception
[x] dry-runs remain non-mutating
[x] duplicate republish remains logically idempotent
[x] 15 remediation tests pass
[x] 40 total tests pass
[x] Ruff passes
[x] strict health is HEALTHY
```

---

# 19. Precise engineering claim

> RetailPulse persists a queryable audit trail for quarantine remediation attempts, including dry-runs, successful Kafka republishes, and failed publish attempts, while preserving original failure metadata, correction details, transport metadata, and downstream idempotency.

---

# 20. Relationship to Sessions 12–17

```text
Session 12 → backfill / replay recovery
Session 13 → data contracts / schema evolution
Session 14 → late-arriving event-time correctness
Session 15 → duplicate delivery / exactly-once business effect
Session 16 → quarantine remediation / dead-letter reprocessing
Session 17 → operational metadata / failure traceability
```

---

# 21. Git update

Inspect:

```cmd
git status
git diff
```

Expected Session 17 changes:

```text
warehouse/init/001_create_warehouse.sql
warehouse/tools/reprocess_quarantine.py
warehouse/tests/test_reprocess_quarantine.py
docs/sessions/session_17_runbook.md
```

Stage:

```cmd
git add warehouse/init/001_create_warehouse.sql
git add warehouse/tools/reprocess_quarantine.py
git add warehouse/tests/test_reprocess_quarantine.py
git add docs/sessions/session_17_runbook.md
```

Review:

```cmd
git status
git diff --cached
```

Commit:

```cmd
git commit -m "Add remediation audit trail"
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

Session 17 complete.
