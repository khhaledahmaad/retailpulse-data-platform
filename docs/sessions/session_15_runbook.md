# Session 15 Runbook — Duplicate Delivery / Exactly-Once Business Effect

## Session goal

Prove that RetailPulse tolerates duplicate physical delivery without creating duplicate business state.

The architectural target is:

```text
at-least-once physical delivery
+
exactly-once business effect
```

Session 15 proves that:

- Kafka may contain duplicate deliveries of the same logical `event_id`
- Bronze preserves both physical deliveries
- Silver preserves both valid physical deliveries
- monitoring distinguishes physical Silver rows from unique logical events
- `raw.orders` deduplicates on `event_id`
- loader reports duplicate rows explicitly
- dbt Fact contains one business row
- Gold counts the event once
- strict health remains HEALTHY even when physical Silver rows exceed logical warehouse rows

No Spark stateful deduplication was added.

---

# 1. Baseline

```cmd
git status
pytest -v
ruff check .
python warehouse\monitoring\check_pipeline_health.py --strict
```

Observed baseline:

```text
23 tests passed
Ruff clean

Bronze rows:       652
Silver rows:       648
Quarantine rows:   4
Raw orders:        648
Fact orders:       648
Gold order count:  648
Status: HEALTHY
```

---

# 2. Existing deduplication boundary

Before Session 15:

```text
Kafka
→ may physically redeliver

Bronze
→ append physical deliveries

Silver
→ append valid physical deliveries

raw.orders
→ event_id business uniqueness
→ ON CONFLICT (event_id) DO NOTHING

Fact / Gold
→ derived from deduplicated warehouse state
```

The missing piece was monitoring: it still assumed physical Silver rows must equal logical Raw rows.

---

# 3. Design decision

Do NOT add stateful Spark deduplication.

Chosen architecture:

```text
Kafka / Bronze / Silver
→ preserve physical delivery history

raw.orders / Fact / Gold
→ enforce logical business uniqueness
```

This avoids unnecessary Spark state/checkpoint migration and stays compatible with late-arriving events.

---

# 4. RED tests

Add two tests to:

```text
warehouse/tests/test_pipeline_health.py
```

Case 1:

```text
649 physical Silver
648 unique Silver
648 Raw
648 Fact
648 Gold
```

Expected:

```text
HEALTHY
```

Case 2:

```text
649 physical Silver
649 unique Silver
648 Raw
648 Fact
648 Gold
```

Expected:

```text
DEGRADED
```

Correct RED failure:

```text
TypeError:
evaluate_health() got an unexpected keyword argument 'silver_unique_events'
```

---

# 5. Add unique-event monitoring

Update:

```text
warehouse/monitoring/check_pipeline_health.py
```

Add:

```python
def count_spark_unique_values(
    root: Path,
    column: str,
) -> int:
    committed_files = get_committed_files(root)

    unique_values = set()

    for path in committed_files:
        if not path.exists():
            raise RuntimeError(
                "Spark-committed Parquet file is "
                f"missing: {path}"
            )

        table = pq.ParquetFile(path).read(
            columns=[column]
        )

        for value in table.column(column).to_pylist():
            if value is not None:
                unique_values.add(value)

    return len(unique_values)
```

This uses the same authoritative Spark `_spark_metadata` committed-file view as the existing monitoring logic.

---

# 6. Extend lake metrics

`collect_lake_metrics()` now exposes:

```text
bronze_rows
silver_rows
silver_unique_events
silver_duplicate_deliveries
quarantine_rows
```

where:

```text
silver_duplicate_deliveries
=
silver_rows - silver_unique_events
```

Meaning:

```text
Silver rows
→ physical valid deliveries

Silver unique events
→ logical event_id population

Silver duplicates
→ physical duplicate-delivery count
```

---

# 7. Extend evaluate_health()

Add optional:

```python
silver_unique_events=None
```

and:

```python
if silver_unique_events is None:
    silver_unique_events = silver_rows
```

This keeps previous health tests backward compatible.

---

# 8. Physical vs logical reconciliation

Physical lake reconciliation:

```text
Bronze
=
Silver physical
+
Quarantine
```

Logical business reconciliation:

```text
Silver unique
=
Raw
=
Fact
=
Gold
```

So:

```python
silver_raw_gap = silver_unique_events - raw_orders
```

---

# 9. Update report output

Health report now includes:

```text
Silver rows
Silver unique
Silver duplicates
```

Example before duplicate injection:

```text
Bronze rows:       652
Silver rows:       648
Silver unique:     648
Silver duplicates: 0
Quarantine rows:   4
Raw orders:        648
Fact orders:       648
Gold order count:  648
Status: HEALTHY
```

---

# 10. GREEN tests

Run:

```cmd
pytest warehouse\tests\test_pipeline_health.py -v
pytest -v
ruff check .
```

Observed:

```text
13 health tests passed
25 total tests passed
All checks passed!
```

Strict health before duplicate injection:

```text
Bronze rows:       652
Silver rows:       648
Silver unique:     648
Silver duplicates: 0
Quarantine rows:   4
Raw orders:        648
Fact orders:       648
Gold order count:  648
Status: HEALTHY
```

---

# 11. Inject one logical event twice

Controlled event:

```text
event_id   315131d4-d4c2-4042-96b1-f8cf59451484
order_id   ORD-DUPLICATE-001
quantity   2
unit_price 30.00
order_value 60.00
```

Send the exact same event twice to Kafka.

Observed:

```text
SENT TWICE WITH SAME event_id
```

---

# 12. Prove physical duplicate delivery

After Spark processing:

```text
Bronze rows:       654
Silver rows:       650
Silver unique:     649
Silver duplicates: 1
Quarantine rows:   4
Raw orders:        648
Fact orders:       648
Gold order count:  648

Status: WARNING
```

Physical invariant:

```text
654 = 650 + 4
```

Logical lag:

```text
649 unique Silver
648 Raw
gap = 1
```

---

# 13. Inspect duplicate Silver records

Both rows had the same:

```text
event_id = 315131d4-d4c2-4042-96b1-f8cf59451484
order_id = ORD-DUPLICATE-001
```

but different Kafka offsets:

```text
212
213
```

Observed:

```text
PHYSICAL_ROWS= 2
UNIQUE_EVENT_IDS= 1
```

This proves:

```text
2 physical deliveries
1 logical event
```

---

# 14. Load duplicate rows

Run:

```cmd
python warehouse\loader\load_orders.py
```

Observed:

```text
Files loaded:            1
Rows processed:          2
Rows inserted:           1
Duplicate rows ignored:  1
```

`ON CONFLICT (event_id) DO NOTHING` protected the business state.

---

# 15. Intermediate warehouse state

After loader, before dbt:

```text
Bronze rows:       654
Silver rows:       650
Silver unique:     649
Silver duplicates: 1
Quarantine rows:   4
Raw orders:        649
Fact orders:       648
Gold order count:  648

Status: DEGRADED
```

Expected issue:

```text
raw.orders does not reconcile with fct_orders
```

---

# 16. Run dbt

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
INSERT 0 1
```

—not two.

---

# 17. Prove one Raw and one Fact row

For:

```text
ORD-DUPLICATE-001
```

Observed:

```text
raw.orders
→ 1 row

analytics.fct_orders
→ 1 row
```

Both reference the same single logical `event_id`.

---

# 18. Final strict reconciliation

Run:

```cmd
python warehouse\monitoring\check_pipeline_health.py --strict
```

Observed:

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

Physical invariant:

```text
Bronze = Silver physical + Quarantine
654 = 650 + 4
```

Logical invariant:

```text
Silver unique = Raw = Fact = Gold
649 = 649 = 649 = 649
```

---

# 19. Fix monitoring issue text

During the runtime proof, the gap calculation was already logical, but the issue text initially printed the physical Silver count.

Correct final text:

```python
issues.append(
    "Silver unique events do not reconcile with "
    "raw.orders: "
    f"{silver_unique_events} != "
    f"{raw_orders} "
    f"(gap={silver_raw_gap})"
)
```

This makes the warning internally consistent.

---

# 20. Final regression gate

Run:

```cmd
pytest -v
ruff check .
python warehouse\monitoring\check_pipeline_health.py --strict
```

Observed:

```text
25 passed
All checks passed!

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

# 21. Proven properties

```text
[x] duplicate source delivery simulated
[x] same event_id delivered twice
[x] separate Kafka offsets proved physical duplication
[x] Bronze preserves physical deliveries
[x] Silver preserves physical valid deliveries
[x] Silver unique count exposed
[x] Silver duplicate count exposed
[x] monitoring separates physical and logical reconciliation
[x] loader processed both duplicate rows
[x] loader inserted one business row
[x] duplicate row explicitly ignored
[x] Fact contains one logical row
[x] Gold counts the event once
[x] strict health tolerates physical duplicates when logical state reconciles
[x] strict health detects genuine missing logical events
[x] issue text corrected
[x] 25 tests pass
[x] Ruff passes
[x] final strict health is HEALTHY
```

---

# 22. Architectural outcome

Physical transport layer:

```text
Kafka
→ Bronze
→ Silver
```

supports:

```text
at-least-once physical delivery
```

Logical business layer:

```text
raw.orders
→ Fact
→ Gold
```

enforces:

```text
exactly-once business effect
```

using `event_id` as the idempotency key.

---

# 23. Precise engineering claim

> RetailPulse supports at-least-once physical event delivery while enforcing exactly-once business effect through event_id-based idempotency at the warehouse boundary.

Do not claim universal exactly-once transport semantics for the whole distributed system.

---

# 24. Relationship to Sessions 12–15

```text
Session 12
→ idempotent backfill and replay

Session 13
→ backward-compatible additive schema evolution

Session 14
→ late-arriving event-time correctness

Session 15
→ duplicate-delivery tolerance / exactly-once business effect
```

---

# 25. Git update

Inspect:

```cmd
git status
git diff
```

Expected Session 15 code changes:

```text
warehouse/monitoring/check_pipeline_health.py
warehouse/tests/test_pipeline_health.py
docs/sessions/session_15_runbook.md
```

Stage explicitly:

```cmd
git add warehouse/monitoring/check_pipeline_health.py
git add warehouse/tests/test_pipeline_health.py
git add docs/sessions/session_15_runbook.md
```

Review:

```cmd
git status
git diff --cached
```

Commit:

```cmd
git commit -m "Add duplicate-aware pipeline reconciliation"
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

# 26. Validation gate

```text
[x] 25 tests pass
[x] Ruff passes
[x] strict health is HEALTHY
[x] Bronze = Silver physical + Quarantine
[x] Silver unique = Raw = Fact = Gold
[x] duplicate-delivery proof complete
[x] Git working tree clean after push
[x] GitHub Actions green
```

Session 15 complete.
