# Session 22 Runbook — Data Quality Rules Framework

## Session goal

Build a practical data-quality framework for RetailPulse that:

- formalises row-level quality rules;
- keeps Spark validation aligned with canonical Python validation;
- safely quarantines malformed events rather than crashing the stream;
- distinguishes physical duplicate delivery from business-key duplication;
- detects duplicate order business keys in dbt;
- repairs the historical collision discovered by the new rule;
- strengthens future `order_id` generation;
- preserves the existing at-least-once physical delivery architecture.

---

## Architecture invariants preserved

RetailPulse continues to use two different notions of state:

### Physical delivery state

Kafka → Bronze → Silver

- Physical at-least-once delivery is allowed.
- The same `event_id` may physically appear more than once in Silver.
- Spark stateful deduplication was **not** introduced.

### Logical business state

Raw → Fact → Gold

- `raw.orders` keeps only one logical event for an `event_id`.
- Loader idempotency prevents a repeated `event_id` from creating another Raw row.
- Business reconciliation remains:

```text
Silver unique event_ids = Raw orders = Fact orders = Gold SUM(order_count)
```

A repeated physical event is therefore different from a duplicated business order identity.

---

# 22.1 — Canonical row-level quality rules

Created:

```text
spark/common/order_quality.py
```

Canonical rules:

| Rule | Error code |
|---|---|
| `event_id` required | `missing_or_invalid_event_id` |
| `order_id` required | `missing_order_id` |
| `product_id` required | `missing_product_id` |
| supported `event_type` | `unsupported_event_type` |
| valid event timestamp | `invalid_event_timestamp` |
| quantity > 0 | `invalid_quantity` |
| unit price > 0 | `invalid_unit_price` |
| supported currency | `unsupported_currency` |
| supported category | `unsupported_category` |

Supported values remain:

```text
event_type:
- order_created

currency:
- GBP

categories:
- electronics
- home
- fashion
- sports
- books
```

`validate_event_quality(event)` returns the first failing quality error, otherwise `None`.

A useful alignment correction was made during this work:

```text
unit_price must be strictly > 0
```

This keeps Spark validation consistent with downstream business expectations.

Focused unit tests were added in:

```text
spark/tests/test_order_quality.py
```

---

# 22.2 — Spark/Python validation parity

The live Spark validation in:

```text
spark/jobs/stream_orders_to_lake.py
```

was aligned with the canonical rules using Spark-native expressions.

A parity tool was added:

```text
spark/tools/check_order_quality_parity.py
```

Cases covered:

1. valid event
2. blank event ID
3. blank order ID
4. blank product ID
5. unsupported event type
6. invalid timestamp
7. zero quantity
8. zero unit price
9. unsupported currency
10. unsupported category

Final parity result:

```text
Quality parity PASS: 10/10 cases
```

---

## Spark 4.1 malformed-timestamp issue

The parity test exposed an important Spark 4.1 ANSI-mode behaviour.

Using:

```python
to_timestamp(...)
```

on a malformed value could raise `CAST_INVALID_INPUT` and terminate processing before the event reached the normal quality classification.

The parsing path was therefore changed to safe timestamp conversion:

```text
try_to_timestamp(...)
```

Result:

```text
malformed timestamp
    ↓
NULL timestamp
    ↓
invalid_event_timestamp
    ↓
Quarantine
```

The event is now treated as a data-quality problem rather than a streaming-query failure.

---

# 22.3 — Duplicate business-key rule

A critical distinction was formalised.

## Allowed physical duplicate

```text
event_id A → order_id X
event_id A → order_id X
```

This represents the same event being delivered more than once.

Expected behaviour:

```text
Silver: multiple physical copies may exist
Raw:    one logical row only
```

The first successful Raw insert effectively wins because `event_id` is the logical idempotency key.

## Forbidden business-key collision

```text
event_id A → order_id X
event_id B → order_id X
```

With the current `order_created`-only model, these are two distinct creation events claiming the same order identity.

That is invalid.

A dbt singular test was added:

```text
warehouse/dbt/retailpulse/tests/assert_unique_order_business_keys.sql
```

Core logic:

```sql
select
    order_id,
    count(distinct event_id) as distinct_event_count
from {{ ref('stg_orders') }}
group by order_id
having count(distinct event_id) > 1
```

Because this is a dbt test, it is executed by the normal Airflow `dbt build` path. It is **not** limited to full-refresh builds.

---

# 22.3A — Real historical collision discovered

The new rule immediately found a genuine historical collision:

```text
order_id = ORD-598743
```

Two distinct logical events had independently received that same old six-digit random order ID.

Earlier order:

```text
event_id:
0da09aeb-8e1e-476e-8f98-75d7ae1fe32d

order_id:
ORD-598743

event_timestamp:
2026-08-14 22:49:07.523417+00
```

Later order:

```text
event_id:
757e2c76-1dc1-4683-b627-619225731546

order_id:
ORD-598743

event_timestamp:
2026-08-18 23:19:24.290836+00
```

The rows represented clearly different orders.

---

# 22.3B — Root cause

The producer previously used a finite random six-digit business-key space:

```python
"order_id": f"ORD-{random.randint(100000, 999999)}"
```

This provides only approximately:

```text
900,000
```

possible values.

At an unbounded or sufficiently large event volume, collisions are inevitable.

---

# 22.3C — Controlled historical repair

The first historical order retained:

```text
ORD-598743
```

The later event was repaired to:

```text
ORD-CC50-27CE-F52D
```

while preserving its technical/source identity:

```text
event_id
event_timestamp
Kafka partition
Kafka offset
Kafka key
```

Bronze remained immutable.

The repair was performed **Silver first**, because corrected durable Silver must remain safe as the source for any future warehouse rebuild.

A controlled repair utility was added:

```text
warehouse/tools/repair_order_business_key.py
```

Properties:

- dry-run by default;
- requires explicit event ID;
- requires expected old order ID;
- requires replacement order ID;
- scans Silver parquet files;
- updates all physical copies of the same event if necessary;
- validates the replacement file before swap;
- uses an atomic file replacement;
- temporary files do not end in `.parquet`, so the loader does not discover incomplete temporary output.

Dry-run verification found exactly one matching Silver row.

The durable sequence was:

```text
1. pause Airflow
2. repair durable Silver
3. verify Silver
4. update exact Raw row
5. confirm duplicate business key disappeared
6. run dbt uniqueness test
7. dbt full refresh
8. strict health check
9. pytest + Ruff
10. unpause Airflow
```

The Raw update returned exactly one row.

The historical duplicate business-key query returned zero collisions after repair.

The dbt uniqueness rule passed.

The full-refresh build passed.

Strict health remained healthy and reconciliation counts were preserved.

---

# 22.3D — Final order identity design

An intermediate human-readable UUID-derived format was considered:

```text
ORD-XXXX-XXXX-XXXX
```

However, for RetailPulse the simpler and stronger design was chosen:

```text
event_id → independent full UUID4
order_id → independent full UUID4
```

Producer logic is therefore conceptually:

```python
"event_id": str(uuid.uuid4()),
"order_id": str(uuid.uuid4()),
```

The two calls are deliberately independent.

This means:

```text
same event_id
→ same logical event redelivered
→ Raw idempotency handles it

different event_id + same order_id
→ invalid duplicate order creation
→ dbt business-key rule catches it
```

Accidentally generating the same full UUID4 `order_id` is practically negligible at RetailPulse scale.

The dbt uniqueness test remains important even with UUIDs because it also protects against:

- producer bugs;
- bad replay/reprocessing logic;
- manual corruption;
- upstream identity defects.

Historical `order_id` values were **not** rewritten simply to match the new format.

Therefore old and new values can coexist:

```text
historical:
ORD-598743
ORD-CC50-27CE-F52D

newly produced:
full UUID4 strings
```

All downstream layers already treat `order_id` as a string, so no schema redesign was required.

---

## Producer UUID test

The producer test was updated to prove independent UUID generation.

Conceptually:

```python
generated = iter(
    [
        uuid.UUID("11111111-1111-1111-1111-111111111111"),
        uuid.UUID("22222222-2222-2222-2222-123456789abc"),
    ]
)
```

Expected:

```text
event_id = 11111111-1111-1111-1111-111111111111
order_id = 22222222-2222-2222-2222-123456789abc
```

and:

```python
assert event["event_id"] != event["order_id"]
```

The temporary short-order-ID helper is no longer needed.

---

# 22.4 — Persistence decision

No additional `control.data_quality_results` table was introduced.

Existing persistence is sufficient:

```text
row-level validation failure
→ Quarantine + validation_error

remediation/reprocessing
→ control.event_reprocessing_log

dbt/business-rule failure
→ dbt build/test failure

pipeline execution/failure history
→ control.pipeline_runs

operational failures
→ existing incident lifecycle
```

Adding another quality-result datastore would duplicate existing observability without enough additional value.

---

# Spark command update — repository imports

Session 22 introduced shared imports such as:

```python
from spark.common.order_quality import ...
```

When `stream_orders_to_lake.py` is launched directly by `spark-submit`, the repository root must therefore be available on Python's import path.

Without that, the driver can fail with:

```text
ModuleNotFoundError: No module named 'spark'
```

## Updated manual Spark lake-stream command

From Windows CMD:

```cmd
docker compose exec -e PYTHONPATH=/opt/retailpulse spark-master /opt/spark/bin/spark-submit ^
  --master spark://spark-master:7077 ^
  --conf spark.jars.ivy=/tmp/.ivy2 ^
  --conf spark.executorEnv.PYTHONPATH=/opt/retailpulse ^
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3 ^
  /opt/retailpulse/spark/jobs/stream_orders_to_lake.py
```

Why both settings are used:

```text
-e PYTHONPATH=/opt/retailpulse
→ makes repo-level packages importable by the Spark driver

--conf spark.executorEnv.PYTHONPATH=/opt/retailpulse
→ supplies the same Python path to Spark executors/workers
```

This is now the preferred manual submit command while the shared quality module is imported from the repository package tree.

## Quality-parity Spark command

```cmd
docker compose exec -e PYTHONPATH=/opt/retailpulse spark-master /opt/spark/bin/spark-submit ^
  --master local[1] ^
  /opt/retailpulse/spark/tools/check_order_quality_parity.py
```

Expected:

```text
Quality parity PASS: 10/10 cases
```

A future convenience improvement could place:

```text
PYTHONPATH=/opt/retailpulse
```

directly in the Spark master/worker container environment, but that is not required for the Session 22 architecture itself.

---

# Final Session 22 validation

Reported final state:

```text
Spark/Python quality parity   PASS 10/10
dbt business-key test         PASS
dbt build                     PASS
historical collision repair   PASS
strict pipeline health        HEALTHY
pytest                        GREEN
Ruff                          GREEN
```

The latest explicitly counted suite during the session contained:

```text
63 tests
```

The later UUID-order-ID adjustment modified the existing producer test rather than adding a new test, and the final pytest run was reported green.

---

# Session 22 outcome

RetailPulse now has three complementary protections:

```text
1. Event contract
   → structural/schema validity

2. Row-level quality framework
   → semantic validity
   → bad rows quarantined safely

3. Dataset/business-key tests
   → cross-row business invariants
```

Identity semantics are now:

```text
event_id
→ independent UUID4
→ logical event identity / Raw idempotency

order_id
→ independent UUID4 for newly produced orders
→ logical order identity

physical Silver duplicate event_id
→ allowed

duplicate Raw event_id
→ prevented

same order_id across distinct event_ids
→ forbidden and detected by dbt
```

This improves production realism while preserving the existing RetailPulse architecture and avoiding unnecessary stateful Spark deduplication or additional observability infrastructure.
