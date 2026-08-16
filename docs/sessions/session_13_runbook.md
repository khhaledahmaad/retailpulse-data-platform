# Session 13 Runbook — Data Contracts / Schema Evolution

## Session goal

Add an explicit versioned data contract between the RetailPulse producer and Spark consumer, then prove compatible and incompatible schema evolution in the real Kafka → Spark → lake → warehouse path.

Session 13 proves:

- producer emits `schema_version = 1`
- canonical V1 contract exists in code
- current V1 event is accepted
- additive unknown fields are backward compatible
- missing required fields are quarantined as contract failures
- unsupported schema versions are quarantined as contract failures
- malformed JSON is quarantined as a contract failure
- structurally valid but bad business data is quarantined as a data-quality failure
- `contract_error` and `validation_error` remain distinct
- valid contract events still flow through Silver → Raw → dbt Fact → Gold
- Quarantine data does not flow into the warehouse
- existing downstream Silver/warehouse schema is preserved

Important scope boundary:

- Session 13 does NOT introduce Schema Registry, Avro, or Protobuf
- Session 13 does NOT persist the test-only extra field `promotion_code`
- `promotion_code` survives only in the Kafka message and Bronze `raw_payload`
- Silver ignores unknown fields unless they are explicitly added to `ORDER_SCHEMA` and selected
- PostgreSQL/dbt do not see `promotion_code` unless downstream schemas and loader are deliberately evolved

---

# 1. Files changed

Expected Session 13 code changes:

```text
producer/src/producer.py
spark/common/order_contract.py
spark/jobs/stream_orders_to_lake.py
spark/tests/test_order_contract.py
```

Runbook:

```text
docs/sessions/session_13_runbook.md
```

Verify the actual Git diff before committing.

---

# 2. Baseline

After Session 12:

```cmd
git status
```

Observed clean working tree.

Baseline:

```cmd
pytest -v
```

Observed:

```text
18 passed
```

```cmd
ruff check .
```

Observed:

```text
All checks passed!
```

Strict health:

```cmd
python warehouse\monitoring\check_pipeline_health.py --strict
```

Observed:

```text
Bronze rows:       572
Silver rows:       572
Quarantine rows:   0
Raw orders:        572
Fact orders:       572
Gold order count:  572
Status: HEALTHY
```

---

# 3. Initial contract

Producer originally emitted:

```text
event_id
event_type
event_timestamp
order_id
customer_id
product_id
category
quantity
unit_price
currency
```

No version field existed.

Spark parsed the same fields and used one `validation_error` path for all failures.

---

# 4. Define V1 contract

Chosen contract:

```text
schema_version = 1
```

Required:

```text
schema_version
event_id
event_type
event_timestamp
order_id
product_id
category
quantity
unit_price
currency
```

Optional:

```text
customer_id
```

Unknown extra fields:

```text
allowed
```

Distinction:

```text
CONTRACT validation
→ can the consumer understand this message under a supported schema?

DATA QUALITY validation
→ are values inside a structurally valid message acceptable?
```

---

# 5. RED tests

Create:

```text
spark/tests/test_order_contract.py
```

Add tests:

```text
test_producer_emits_schema_version_v1
test_v1_contract_accepts_current_order_event
test_contract_accepts_unknown_extra_field
test_contract_rejects_missing_required_field
test_contract_rejects_unsupported_schema_version
```

The additive-evolution test adds:

```python
event["promotion_code"] = "SUMMER26"
```

Run:

```cmd
pytest spark\tests\test_order_contract.py -v
```

Observed:

```text
5 failed
```

Expected reasons:

```text
KeyError: schema_version
ModuleNotFoundError: spark.common.order_contract
```

---

# 6. Canonical contract module

Create:

```text
spark/common/order_contract.py
```

Core constants:

```python
CURRENT_SCHEMA_VERSION = 1

SUPPORTED_SCHEMA_VERSIONS = {
    CURRENT_SCHEMA_VERSION,
}

V1_REQUIRED_FIELDS = (
    "event_id",
    "event_type",
    "event_timestamp",
    "order_id",
    "product_id",
    "category",
    "quantity",
    "unit_price",
    "currency",
)
```

Validator behavior:

```text
non-dict payload
→ contract_invalid_payload

missing schema_version
→ contract_missing_schema_version

unsupported version
→ contract_unsupported_schema_version

missing/null required field
→ contract_missing_<field>

unknown extra field
→ accepted
```

---

# 7. Version producer events

Update:

```text
producer/src/producer.py
```

Add:

```python
SCHEMA_VERSION = 1
```

and:

```python
"schema_version": SCHEMA_VERSION,
```

to each generated order event.

Do not make the producer import the constant from Spark.

---

# 8. GREEN contract tests

Run:

```cmd
pytest spark\tests\test_order_contract.py -v
```

Observed:

```text
5 passed
```

Full suite:

```cmd
pytest -v
```

Observed:

```text
23 passed
```

Lint:

```cmd
ruff check .
```

Observed:

```text
All checks passed!
```

---

# 9. Add schema version to Spark

Update:

```text
spark/jobs/stream_orders_to_lake.py
```

Add `schema_version` to `ORDER_SCHEMA` as nullable `IntegerType`.

Add:

```python
RAW_EVENT_SCHEMA = MapType(
    StringType(),
    StringType(),
)
```

and supported-version/required-field constants for Spark-native validation.

---

# 10. Preserve raw key presence

`parse_orders()` now parses both:

```text
raw_payload → raw_event map
raw_payload → typed ORDER_SCHEMA event
```

Reason:

```text
missing field
```

and:

```text
present but invalid typed field
```

can both become typed `NULL`.

Keeping the raw map lets Spark distinguish contract absence from value invalidity.

---

# 11. Spark contract validation

Add:

```text
add_contract_validation()
```

Behavior:

```text
raw_event NULL
→ contract_invalid_payload

missing raw schema_version
→ contract_missing_schema_version

typed schema_version NULL
→ contract_invalid_schema_version

schema_version != 1
→ contract_unsupported_schema_version

missing required raw field
→ contract_missing_<field>
```

Unknown keys are not rejected.

---

# 12. Separate contract and data-quality failures

Existing data-quality validation remains in `add_validation()`.

New behavior:

```text
contract_error IS NOT NULL
→ validation_error remains NULL
```

Existing value checks continue only for structurally compatible messages.

---

# 13. Routing

Silver now requires:

```text
contract_error IS NULL
AND
validation_error IS NULL
```

Quarantine now accepts:

```text
contract_error IS NOT NULL
OR
validation_error IS NOT NULL
```

Quarantine persists:

```text
raw_payload
schema_version
contract_error
validation_error
topic
partition
offset
kafka_timestamp
ingested_at
```

Do not add `schema_version` or `promotion_code` to persisted Silver in this session.

---

# 14. Wire main Spark flow

Logical flow:

```text
Kafka
→ Bronze
→ parse_orders
→ add_contract_validation
→ add_validation
→ Silver / Quarantine
```

No Python UDF is required.

---

# 15. Static regression

Run:

```cmd
ruff check .
pytest -v
```

Observed:

```text
All checks passed!
23 passed
```

---

# 16. Real schema-evolution proof

Pause Airflow:

```cmd
docker compose exec airflow-api-server airflow dags pause retailpulse_warehouse_pipeline
```

Keep ordinary producer stopped.

Restart updated Spark stream without deleting checkpoints:

```cmd
docker compose exec spark-master /opt/spark/bin/spark-submit ^
  --master spark://spark-master:7077 ^
  --conf spark.jars.ivy=/tmp/.ivy2 ^
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3 ^
  /opt/retailpulse/spark/jobs/stream_orders_to_lake.py
```

Inject four controlled messages:

```text
A. valid V1
B. valid V1 + promotion_code
C. V1 missing order_id
D. schema_version = 99
```

Controlled order IDs:

```text
ORD-CONTRACT-V1
ORD-CONTRACT-EXTRA
ORD-CONTRACT-V99
```

---

# 17. First runtime routing proof

After injection:

```cmd
python warehouse\monitoring\check_pipeline_health.py
```

Observed:

```text
Bronze rows:       576
Silver rows:       574
Quarantine rows:   2
Raw orders:        572
Fact orders:       572
Gold order count:  572
Status: WARNING
```

Invariant:

```text
576 = 574 + 2
```

The WARNING is expected because valid Silver leads paused Raw by 2 rows.

---

# 18. Ad-hoc PySpark inspection note

Direct container Python existed:

```text
/usr/bin/python3
```

but:

```text
ModuleNotFoundError: No module named 'pyspark'
```

Spark's bundled Python libraries are under:

```text
/opt/spark/python
/opt/spark/python/lib/py4j-*.zip
```

Working inspection pattern:

```cmd
docker compose exec spark-master sh -lc "PYTHONPATH=/opt/spark/python:$(echo /opt/spark/python/lib/py4j-*.zip) python3 -c 'import pyspark; print(pyspark.__version__)'"
```

Observed:

```text
4.1.3
```

Use this pattern for ad-hoc PySpark reads inside the container.

---

# 19. Quarantine contract proof

Observed:

```text
schema_version = 1
contract_error = contract_missing_order_id
validation_error = NULL
```

and:

```text
schema_version = 99
contract_error = contract_unsupported_schema_version
validation_error = NULL
```

---

# 20. Silver additive-evolution proof

Silver inspection showed:

```text
ORD-CONTRACT-V1
ORD-CONTRACT-EXTRA
```

and did not show:

```text
ORD-CONTRACT-V99
```

So:

```text
normal V1
→ accepted

V1 + unknown promotion_code
→ accepted

V99
→ quarantined
```

---

# 21. promotion_code lifecycle

Current behavior:

```text
Kafka message
→ contains promotion_code

Bronze raw_payload
→ contains promotion_code inside the original JSON

Spark raw_event map
→ can see it transiently

ORDER_SCHEMA
→ promotion_code not declared

Persisted Silver
→ promotion_code absent

PostgreSQL raw.orders
→ promotion_code absent

dbt staging/fact/mart
→ promotion_code absent
```

This was deliberate.

Session 13 proves compatibility with an additive field, not persistence of that field.

To persist it later:

```text
1. add nullable promotion_code to Spark ORDER_SCHEMA
2. select it into Silver
3. add nullable promotion_code column to raw.orders
4. update warehouse loader inserts
5. expose it in dbt staging
6. add to fact/mart only if analytically useful
```

A full rebuild is not necessarily required; an additive database migration can be used.

---

# 22. Contract failure vs data-quality failure

Inject:

```text
E. malformed JSON
F. valid V1 with quantity = 0
```

Observed counts:

```text
Bronze rows:       578
Silver rows:       574
Quarantine rows:   4
Raw orders:        572
Fact orders:       572
Gold order count:  572
Status: WARNING
```

Invariant:

```text
578 = 574 + 4
```

Quarantine inspection showed:

Malformed JSON:

```text
schema_version   = NULL
contract_error   = contract_invalid_payload
validation_error = NULL
```

Bad quantity:

```text
schema_version   = 1
contract_error   = NULL
validation_error = invalid_quantity
```

This proves the two failure classes are independent.

---

# 23. Warehouse catch-up

Run:

```cmd
python warehouse\loader\load_orders.py
```

Observed:

```text
Current watermark: (datetime.date(2026, 8, 15), 23)
Mode: NORMAL
Eligible partitions: 2
```

New 2026-08-16 hour 12 Silver files:

```text
2 files loaded
2 rows processed
2 rows inserted
0 duplicates
```

Only valid Silver events entered `raw.orders`.

---

# 24. dbt build

Run:

```cmd
cd warehouse\dbt\retailpulse
dbt build --no-partial-parse --target dev
cd ..\..\..
```

Observed:

```text
PASS=22
WARN=0
ERROR=0
SKIP=0
TOTAL=22
```

Fact inserted:

```text
2 rows
```

Gold mart rebuilt successfully.

---

# 25. Final strict reconciliation

Run:

```cmd
python warehouse\monitoring\check_pipeline_health.py --strict
```

Observed:

```text
Bronze rows:       578
Silver rows:       574
Quarantine rows:   4
Raw orders:        574
Fact orders:       574
Gold order count:  574
Status: HEALTHY
```

Core invariants:

```text
Bronze = Silver + Quarantine
578 = 574 + 4
```

and:

```text
Silver = Raw = Fact = Gold
574 = 574 = 574 = 574
```

---

# 26. Final regression gate

Run:

```cmd
pytest -v
```

Observed:

```text
23 passed
```

Run:

```cmd
ruff check .
```

Observed:

```text
All checks passed!
```

Confirm producer:

```cmd
python -c "from producer.src.producer import create_order_event; e=create_order_event(); print(e['schema_version']); print(sorted(e.keys()))"
```

Observed version:

```text
1
```

Final strict health:

```text
HEALTHY
```

---

# 27. Session 13 proven properties

```text
[x] explicit schema_version introduced
[x] producer emits V1
[x] canonical V1 contract module exists
[x] required fields defined
[x] customer_id intentionally optional
[x] unknown additive field accepted
[x] required field removal rejected
[x] unsupported schema version rejected
[x] malformed JSON rejected
[x] contract_error separated from validation_error
[x] quantity = 0 classified as data-quality failure
[x] contract-invalid rows have validation_error NULL
[x] data-quality-invalid rows have contract_error NULL
[x] Silver requires both validation classes to pass
[x] Quarantine stores diagnostic error classes
[x] V1 normal event reaches Silver
[x] V1 + promotion_code reaches Silver based on known fields
[x] promotion_code intentionally not persisted downstream
[x] V99 does not reach Silver
[x] Bronze = Silver + Quarantine
[x] only valid Silver rows reach raw.orders
[x] dbt Fact receives only valid rows
[x] Gold reconciles
[x] 23 tests pass
[x] Ruff passes
[x] strict health is HEALTHY
```

---

# 28. Architectural outcome

```text
Producer
  ↓
schema_version = 1 JSON
  ↓
Kafka
  ↓
Bronze raw payload preserved
  ↓
Spark contract validation
  ├── compatible
  │      ↓
  │   data-quality validation
  │      ├── valid → Silver
  │      └── invalid values → Quarantine
  │
  └── incompatible/malformed
         → Quarantine
```

Then:

```text
Silver
→ warehouse loader
→ raw.orders
→ dbt staging
→ dbt Fact
→ Gold mart
```

Only valid Silver records enter the warehouse.

---

# 29. Git update

Inspect actual changes:

```cmd
git status
git diff
```

Expected Session 13 paths:

```text
producer/src/producer.py
spark/common/order_contract.py
spark/jobs/stream_orders_to_lake.py
spark/tests/test_order_contract.py
docs/sessions/session_13_runbook.md
```

Stage explicitly:

```cmd
git add producer/src/producer.py
git add spark/common/order_contract.py
git add spark/jobs/stream_orders_to_lake.py
git add spark/tests/test_order_contract.py
git add docs/sessions/session_13_runbook.md
```

Review:

```cmd
git status
git diff --cached
```

Commit:

```cmd
git commit -m "Add versioned data contracts and schema validation"
```

Push:

```cmd
git push origin main
```

Final:

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

# 30. Validation gate

Before moving on:

```text
[x] 23 tests pass
[x] Ruff passes
[x] strict health is HEALTHY
[x] Bronze = Silver + Quarantine
[x] Silver = Raw = Fact = Gold
[x] working tree clean after push
[x] GitHub Actions green
```

Session 13 complete.
