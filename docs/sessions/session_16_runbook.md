# Session 16 Runbook — Quarantine Remediation / Dead-Letter Reprocessing

## Session goal

Turn Quarantine from a passive failure sink into a controlled, auditable remediation workflow.

Session 16 proves that RetailPulse can:

```text
invalid event
→ Quarantine
→ inspect failure
→ apply explicit correction
→ validate correction
→ dry-run by default
→ explicitly republish
→ re-enter normal pipeline
→ reach Silver / Raw / Fact / Gold
```

while preserving:

```text
original quarantined failure
original event_id
original event_timestamp
downstream idempotency
```

The original quarantined record is never edited or deleted.

---

# 1. Baseline

Run:

```cmd
git status
pytest -v
ruff check .
python warehouse\monitoring\check_pipeline_health.py --strict
```

Observed before Session 16:

```text
On branch main
Your branch is up to date with 'origin/main'.

nothing to commit, working tree clean

25 tests passed
Ruff clean

Bronze rows:       654
Silver rows:       650
Silver unique:     649
Silver duplicates: 1
Quarantine rows:   4
Raw orders:        649
Fact orders:       649
Gold order count:  649

Status: HEALTHY
```

---

# 2. Inspect existing Quarantine records

Inspect:

```cmd
docker compose exec spark-master sh -lc "PYTHONPATH=/opt/spark/python:$(echo /opt/spark/python/lib/py4j-*.zip) python3 -c \"from pyspark.sql import SparkSession; s=SparkSession.builder.master('local[1]').appName('InspectQuarantine').getOrCreate(); d=s.read.option('mergeSchema','true').parquet('/opt/retailpulse/data_lake/quarantine/orders'); d.orderBy('kafka_timestamp').select('schema_version','contract_error','validation_error','raw_payload','kafka_timestamp').show(20, truncate=False); s.stop()\""
```

Observed four known failure classes:

```text
contract_missing_order_id
contract_unsupported_schema_version
contract_invalid_payload
invalid_quantity
```

Chosen remediation target:

```text
event_id:
7fbec326-c180-41bd-8a7d-a24b0f35b68f

order_id:
ORD-DQ-BAD-QUANTITY

schema_version:
1

contract_error:
NULL

validation_error:
invalid_quantity

quantity:
0

unit_price:
149.35

event_timestamp:
2026-08-16T12:43:22.858263+00:00
```

Reason for choosing this event:

```text
schema is valid
identity is intact
contract is supported
only a business-quality value is invalid
```

Repair:

```text
quantity: 0 → 2
```

---

# 3. Remediation identity rule

For a corrected event that never entered the business warehouse:

```text
preserve event_id
preserve event_timestamp
```

Reason:

```text
event_id
→ logical event identity

event_timestamp
→ original business occurrence time
```

A repair creates a new physical Kafka delivery, not a new logical business event.

---

# 4. RED tests

Create:

```text
warehouse/tests/test_reprocess_quarantine.py
```

Initial tests covered:

```text
apply_corrections preserves identity
event_id change is rejected
event_timestamp change is rejected
FIELD=VALUE parsing preserves scalar types
```

Initial RED run:

```cmd
pytest warehouse\tests\test_reprocess_quarantine.py -v
```

Correct RED failure:

```text
ModuleNotFoundError:
No module named 'warehouse.tools'
```

This proved the tests were valid and the remediation implementation was missing.

---

# 5. Create remediation module

Create:

```text
warehouse/tools/__init__.py
warehouse/tools/reprocess_quarantine.py
```

Core protected fields:

```python
PROTECTED_FIELDS = {
    "event_id",
    "event_timestamp",
}
```

Core behavior:

```text
parse_set_values(...)
apply_corrections(...)
```

`parse_set_values()` uses JSON parsing so CLI values retain scalar types:

```text
quantity=2
→ int

unit_price=149.35
→ float

enabled=true
→ bool

category=home
→ str
```

`apply_corrections()` deep-copies the payload so the original quarantined payload is not mutated.

---

# 6. GREEN correction logic

Run:

```cmd
pytest warehouse\tests\test_reprocess_quarantine.py -v
```

Observed:

```text
4 passed
```

Full suite:

```cmd
pytest -v
```

Observed:

```text
29 passed
```

Ruff initially flagged a nested `if`.

Fix:

```python
if (
    field in PROTECTED_FIELDS
    and value != payload.get(field)
):
```

Then:

```cmd
ruff check .
```

Observed:

```text
All checks passed!
```

---

# 7. Add Quarantine lookup

Add:

```text
QUARANTINE_ROOT = data_lake/quarantine/orders
```

Add:

```text
discover_quarantine_files()
find_quarantined_event(event_id)
```

Lookup semantics:

```text
0 matches
→ error

1 match
→ return record

>1 matches
→ error
```

The tool reads:

```text
raw_payload
contract_error
validation_error
kafka_timestamp
```

and parses `raw_payload` JSON to find the requested `event_id`.

Malformed JSON rows are skipped during event-id lookup because they cannot contain a recoverable logical event identity.

---

# 8. Dry-run CLI

Run remediation as a module:

```cmd
python -m warehouse.tools.reprocess_quarantine ^
  --event-id 7fbec326-c180-41bd-8a7d-a24b0f35b68f ^
  --set quantity=2
```

Using `python -m` is intentional.

Direct script execution:

```cmd
python warehouse\tools\reprocess_quarantine.py
```

could not resolve the top-level `spark` package when shared validators were imported.

The module form correctly runs from the repository package root.

Observed dry-run:

```text
RetailPulse Quarantine Remediation
---------------------------------
Event ID:          7fbec326-c180-41bd-8a7d-a24b0f35b68f
Order ID:          ORD-DQ-BAD-QUANTITY
Contract error:    None
Validation error:  invalid_quantity
Kafka timestamp:   2026-08-16 12:43:22.972000

Corrections:
- quantity: 0 -> 2

Repaired payload:
...
quantity: 2
...

DRY RUN
Nothing published.
```

---

# 9. Canonical contract validation

Reuse:

```text
spark/common/order_contract.py
```

Canonical API:

```python
validate_event_contract(event) -> str | None
```

Add remediation wrapper:

```text
validate_repaired_contract(...)
```

Behavior:

```text
contract validator returns None
→ PASS

contract validator returns error
→ raise ValueError
```

New tests:

```text
valid V1 repair accepted
unsupported schema version rejected
```

Observed:

```text
6 remediation tests passed
31 total tests passed
Ruff clean
```

Dry-run output now includes:

```text
Contract validation: PASS
```

---

# 10. Add canonical-aligned data-quality validation

Contract validation does not reject business-value errors such as:

```text
quantity = 0
```

So create:

```text
spark/common/order_quality.py
```

Pure-Python validator:

```text
missing_or_invalid_event_id
missing_order_id
missing_product_id
invalid_event_timestamp
invalid_quantity
invalid_unit_price
unsupported_currency
```

Rules align with the current Spark stream:

```text
event_id must be present
order_id must be present
product_id must be present
event_timestamp must parse
quantity must be int > 0
unit_price must be numeric >= 0
currency must be GBP
```

Add:

```text
validate_repaired_quality(...)
```

to the remediation CLI.

---

# 11. Data-quality safety tests

Add tests for:

```text
valid repaired event
quantity = 0
negative unit_price
unsupported currency
bad timestamp
```

Run:

```cmd
pytest warehouse\tests\test_reprocess_quarantine.py -v
```

Observed:

```text
11 passed
```

Full suite:

```cmd
pytest -v
```

Observed:

```text
36 passed
```

Ruff initially removed an unnecessary final `return None`.

Final:

```text
Ruff clean
```

---

# 12. Prove invalid remediation is blocked

Run:

```cmd
python -m warehouse.tools.reprocess_quarantine ^
  --event-id 7fbec326-c180-41bd-8a7d-a24b0f35b68f ^
  --set quantity=0
```

Observed:

```text
ValueError:
Repaired payload failed data-quality validation:
invalid_quantity
```

No publish occurred.

This proves:

```text
contract-valid
but data-quality-invalid
```

repairs cannot pass the safety gate.

---

# 13. Prove valid remediation remains dry-run

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

...
DRY RUN
Nothing published.
```

---

# 14. Add explicit Kafka publishing

Add:

```text
--publish
```

CLI rule:

```text
without --publish
→ dry-run only

with --publish
→ publish only after both validators pass
```

Add:

```text
publish_repaired_event(...)
```

with injectable:

```text
producer_factory
```

for unit testing.

Publishing preserves:

```text
same event_id
same event_timestamp
same order_id
corrected payload
```

Kafka generates a new physical:

```text
partition
offset
kafka_timestamp
```

---

# 15. Publish unit test

Add test:

```text
test_publish_repaired_event_preserves_payload_and_identity
```

Fake producer verifies:

```text
topic = orders
key = order_id
payload preserved
event_id preserved
flush called
close called
metadata returned
```

Run:

```cmd
pytest warehouse\tests\test_reprocess_quarantine.py -v
```

Observed:

```text
12 passed
```

Full suite:

```cmd
pytest -v
```

Observed:

```text
37 passed
```

Ruff:

```text
All checks passed!
```

Dry-run reconfirmed:

```text
DRY RUN
Nothing published.
```

---

# 16. Pre-publish business baseline

Verify target event was not in Raw or Fact:

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "SELECT COUNT(*) AS raw_count FROM raw.orders WHERE event_id = '7fbec326-c180-41bd-8a7d-a24b0f35b68f'; SELECT COUNT(*) AS fact_count FROM analytics.fct_orders WHERE event_id = '7fbec326-c180-41bd-8a7d-a24b0f35b68f';"
```

Observed:

```text
raw_count = 0
fact_count = 0
```

Strict baseline:

```text
Bronze rows:       654
Silver rows:       650
Silver unique:     649
Silver duplicates: 1
Quarantine rows:   4
Raw orders:        649
Fact orders:       649
Gold order count:  649

Status: HEALTHY
```

Historical Gold baseline for:

```text
2026-08-16
```

was:

```text
order_count            76
units_sold            216
gross_revenue    16063.23
average_order_value   211.36
```

Repair contribution:

```text
quantity   = 2
unit_price = 149.35
order_value = 298.70
```

Expected Gold delta:

```text
order_count   +1
units_sold    +2
gross_revenue +298.70
```

---

# 17. Publish the repaired event

Run:

```cmd
python -m warehouse.tools.reprocess_quarantine ^
  --event-id 7fbec326-c180-41bd-8a7d-a24b0f35b68f ^
  --set quantity=2 ^
  --publish
```

Observed validation:

```text
Contract validation: PASS
Data-quality validation: PASS
```

Observed publish:

```text
PUBLISHED
Topic:     orders
Partition: 0
Offset:    214
```

---

# 18. Post-Spark state

After Spark consumed the repaired event:

```text
Bronze rows:       655
Silver rows:       651
Silver unique:     650
Silver duplicates: 1
Quarantine rows:   4
Raw orders:        649
Fact orders:       649
Gold order count:  649

Status: WARNING
```

Issue:

```text
Silver unique events do not reconcile with raw.orders:
650 != 649
gap = 1
```

Physical invariant remained correct:

```text
Bronze = Silver physical + Quarantine
655 = 651 + 4
```

Quarantine count remained unchanged.

---

# 19. Preserve original failure record

Inspect original Quarantine row.

Observed:

```text
event_id:
7fbec326-c180-41bd-8a7d-a24b0f35b68f

quantity:
0

validation_error:
invalid_quantity

kafka_timestamp:
2026-08-16 12:43:22.972
```

This proves the original failed physical delivery remains untouched.

---

# 20. Repaired Silver row

Inspect Silver.

Observed:

```text
event_id:
7fbec326-c180-41bd-8a7d-a24b0f35b68f

order_id:
ORD-DQ-BAD-QUANTITY

event_timestamp:
2026-08-16 12:43:22.858263

quantity:
2

unit_price:
149.35

order_value:
298.70

partition:
0

offset:
214

kafka_timestamp:
2026-08-17 13:12:30.373

ingested_at:
2026-08-17 13:14:16.514
```

Audit story:

```text
same logical identity
same original event time
new physical transport metadata
corrected business value
```

---

# 21. Automatic Airflow catch-up

Airflow was not paused before the first real publish.

This did not invalidate the test.

Instead, Airflow automatically completed the downstream workflow.

Loader observed:

```text
processed=1
inserted=1
duplicates=0
```

dbt observed:

```text
fct_orders
INSERT 0 1
```

All dbt checks:

```text
PASS=22
WARN=0
ERROR=0
```

Final Airflow health:

```text
Bronze rows:       655
Silver rows:       651
Silver unique:     650
Silver duplicates: 1
Quarantine rows:   4
Raw orders:        650
Fact orders:       650
Gold order count:  650

Status: HEALTHY
```

This provided a realistic production-like proof that remediation re-enters the ordinary orchestrated workflow.

---

# 22. Raw and Fact proof

Query:

```text
event_id =
7fbec326-c180-41bd-8a7d-a24b0f35b68f
```

Observed exactly one Raw row:

```text
order_id:
ORD-DQ-BAD-QUANTITY

quantity:
2

unit_price:
149.35

order_value:
298.70

event_timestamp:
2026-08-16 12:43:22.858263+00

loaded_at:
2026-08-17 13:20:02.111496+00
```

Fact also contains one logical row.

---

# 23. Historical Gold correction

After remediation:

```text
2026-08-16
order_count            77
units_sold            218
gross_revenue    16361.93
average_order_value   212.49
```

Compared to baseline:

```text
order_count:
76 → 77

units_sold:
216 → 218

gross_revenue:
16063.23 → 16361.93

delta:
+1 order
+2 units
+298.70 revenue
```

This proves remediation composes correctly with Session 14 event-time semantics:

```text
repair published Aug 17
but original event_timestamp preserved as Aug 16
→ historical Aug 16 mart corrected
```

---

# 24. Prove remediation idempotency

Pause Airflow before second publish:

```cmd
docker compose exec airflow-api-server airflow dags pause retailpulse_warehouse_pipeline
```

Publish the same correction again:

```cmd
python -m warehouse.tools.reprocess_quarantine ^
  --event-id 7fbec326-c180-41bd-8a7d-a24b0f35b68f ^
  --set quantity=2 ^
  --publish
```

Second publish metadata:

```text
Topic:     orders
Partition: 0
Offset:    215
```

After Spark:

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

Interpretation:

```text
new physical delivery
→ Bronze +1
→ Silver physical +1
→ Silver unique unchanged
→ business state unchanged
```

---

# 25. Loader idempotency proof

Run normal loader:

```cmd
python warehouse\loader\load_orders.py
```

Observed:

```text
Rows processed:          1
Rows inserted:           0
Duplicate rows ignored:  1
```

This confirms the warehouse boundary still enforces:

```text
event_id uniqueness
```

---

# 26. dbt idempotency proof

Run:

```cmd
cd warehouse\dbt\retailpulse
dbt build --no-partial-parse --target dev
cd ..\..\..
```

Observed:

```text
fct_orders
INSERT 0 0

PASS=22
WARN=0
ERROR=0
```

No second Fact row was created.

---

# 27. Gold idempotency proof

Re-query:

```text
2026-08-16
```

Observed unchanged:

```text
order_count            77
units_sold            218
gross_revenue    16361.93
average_order_value   212.49
```

Therefore the repeated remediation had no second business effect.

---

# 28. Kafka serializer deprecation cleanup

Initial publish worked but emitted:

```text
DeprecationWarning:
key_serializer does not implement kafka.serializer.Serializer

DeprecationWarning:
value_serializer does not implement kafka.serializer.Serializer
```

Replace callable serializers with:

```python
from kafka.serializer import (
    DefaultSerializer,
    JsonSerializer,
)
```

Producer now uses:

```python
key_serializer=DefaultSerializer()
value_serializer=JsonSerializer()
```

Validation:

```cmd
python -W error::DeprecationWarning -c "from kafka import KafkaProducer; from kafka.serializer import DefaultSerializer, JsonSerializer; p=KafkaProducer(bootstrap_servers='localhost:9092', key_serializer=DefaultSerializer(), value_serializer=JsonSerializer()); p.close(); print('Kafka serializers OK')"
```

Observed:

```text
Kafka serializers OK
```

---

# 29. Final test gate

Run:

```cmd
pytest warehouse\tests\test_reprocess_quarantine.py -v
```

Observed:

```text
12 passed
```

Run:

```cmd
pytest -v
```

Observed:

```text
37 passed
```

Run:

```cmd
ruff check .
```

Observed:

```text
All checks passed!
```

Run:

```cmd
python warehouse\monitoring\check_pipeline_health.py --strict
```

Observed:

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

# 30. Restore Airflow

Unpause:

```cmd
docker compose exec airflow-api-server airflow dags unpause retailpulse_warehouse_pipeline
```

Follow-up:

```cmd
docker compose exec airflow-api-server airflow dags list
```

Confirmed effective state:

```text
retailpulse_warehouse_pipeline
is_paused = False
```

Multiple duplicate rows appeared in `airflow dags list`, but all referenced the same DAG file and all reported:

```text
is_paused = False
```

So normal scheduling was restored.

---

# 31. Session 16 proven properties

```text
[x] quarantine record can be located by event_id
[x] malformed records are not incorrectly treated as recoverable logical events
[x] original quarantined record remains unchanged
[x] explicit corrections are isolated from original payload
[x] event_id cannot be changed
[x] event_timestamp cannot be changed
[x] scalar correction values preserve types
[x] canonical contract validator reused
[x] data-quality validator added
[x] invalid repairs blocked
[x] dry-run is default
[x] explicit --publish required
[x] Kafka publishing unit-tested
[x] corrected event uses same logical identity
[x] corrected event creates new physical Kafka delivery
[x] repaired event reaches Silver
[x] original Quarantine row remains
[x] Quarantine count does not decrease
[x] Raw receives exactly one logical row
[x] Fact receives exactly one logical row
[x] historical Gold period is corrected
[x] repeat remediation produces new physical delivery
[x] repeat remediation does not create second Raw row
[x] repeat remediation does not create second Fact row
[x] repeat remediation does not change Gold again
[x] serializer deprecation warnings removed
[x] 12 remediation tests pass
[x] 37 total tests pass
[x] Ruff passes
[x] strict health is HEALTHY
[x] Airflow scheduling restored
```

---

# 32. Architectural outcome

RetailPulse Quarantine is now an auditable recovery path:

```text
original bad delivery
        ↓
    Quarantine
        ↓
   inspect reason
        ↓
explicit correction
        ↓
contract validation
        ↓
data-quality validation
        ↓
dry-run by default
        ↓
 explicit --publish
        ↓
      Kafka
        ↓
normal Spark pipeline
        ↓
      Silver
        ↓
   raw.orders
        ↓
       Fact
        ↓
       Gold
```

The original failed record remains permanently available for audit.

---

# 33. Precise engineering claim

> RetailPulse supports controlled, auditable quarantine remediation: failed events can be explicitly corrected, validated, and republished through the normal ingestion path while preserving original failure history, logical event identity, event-time semantics, and downstream idempotency.

---

# 34. Relationship to Sessions 12–16

```text
Session 12
→ backfill / replay recovery

Session 13
→ data contracts / schema evolution

Session 14
→ late-arriving event-time correctness

Session 15
→ duplicate delivery / exactly-once business effect

Session 16
→ quarantine remediation / dead-letter reprocessing
```

Together:

```text
recovery
idempotency
schema safety
event-time correctness
duplicate tolerance
quarantine remediation
auditability
```

---

# 35. Git update

Inspect:

```cmd
git status
git diff
```

Expected Session 16 changes include:

```text
spark/common/order_quality.py
warehouse/tools/__init__.py
warehouse/tools/reprocess_quarantine.py
warehouse/tests/test_reprocess_quarantine.py
docs/sessions/session_16_runbook.md
```

Stage explicitly:

```cmd
git add spark/common/order_quality.py
git add warehouse/tools/__init__.py
git add warehouse/tools/reprocess_quarantine.py
git add warehouse/tests/test_reprocess_quarantine.py
git add docs/sessions/session_16_runbook.md
```

Review:

```cmd
git status
git diff --cached
```

Commit:

```cmd
git commit -m "Add quarantine remediation workflow"
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

Confirm GitHub Actions returns green.

---

# 36. Final validation gate

```text
[x] 37 tests pass
[x] Ruff passes
[x] Kafka serializers clean
[x] strict health HEALTHY
[x] Quarantine preserved
[x] repaired event reached Raw / Fact / Gold
[x] historical Gold corrected once
[x] repeat remediation had no second business effect
[x] Airflow unpaused
[x] working tree clean after push
[x] GitHub Actions green
```

Session 16 complete.
