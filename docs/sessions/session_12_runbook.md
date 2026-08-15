# Session 12 Runbook — Backfill / Replay Workflow

## Session goal

Add a safe, bounded historical backfill/replay workflow to the RetailPulse warehouse loader without disturbing normal forward incremental processing.

Session 12 proves:

- explicit historical range discovery
- backfill that skips already-loaded files
- replay that deliberately rereads already-loaded files
- no forward watermark movement during historical operations
- loader idempotency during replay
- real recovery of a deliberately removed historical warehouse row/file-control record
- normal incremental behavior remains unchanged
- strict health returns to HEALTHY after recovery

Important scope boundary:

- this is a warehouse backfill/replay workflow
- Spark/Kafka checkpoint rewind/reset is NOT part of Session 12
- existing Spark checkpoint recovery behavior remains unchanged

---

# 1. Files changed

```text
warehouse/loader/load_orders.py
warehouse/tests/test_load_orders.py
```

No schema changes were required.

---

# 2. Baseline before Session 12

Repository state:

```cmd
git status
```

Expected:

```text
working tree clean
```

Strict health:

```cmd
python warehouse\monitoring\check_pipeline_health.py --strict
```

Captured baseline:

```text
Bronze        525
Silver        525
Quarantine      0
Raw           525
Fact          525
Gold          525
Status        HEALTHY
```

Forward loader watermark:

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "SELECT * FROM control.loader_watermarks WHERE dataset_name = 'silver_orders';"
```

Captured:

```text
watermark_date = 2026-08-15
watermark_hour = 11
```

Loaded Silver files:

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "SELECT COUNT(*) AS loaded_files FROM control.loaded_files WHERE dataset_name = 'silver_orders';"
```

Captured:

```text
276
```

Raw orders:

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "SELECT COUNT(*) AS raw_orders FROM raw.orders;"
```

Captured:

```text
525
```

Baseline tests:

```cmd
pytest -v
```

Expected before Session 12 additions:

```text
14 passed
```

Lint:

```cmd
ruff check .
```

Expected:

```text
All checks passed!
```

---

# 3. Session 12 loader mode design

Normal mode:

```text
watermark
→ discover current/new partitions
→ skip files already registered in control.loaded_files
→ load unseen files
→ advance/maintain forward watermark
```

Backfill mode:

```text
explicit --from / --to historical range
→ ignore normal watermark for discovery
→ skip already-loaded files
→ load only unseen files in the range
→ DO NOT advance forward watermark
```

Replay mode:

```text
explicit --from / --to historical range
→ ignore normal watermark for discovery
→ deliberately reread already-loaded files
→ event_id uniqueness / ON CONFLICT prevents duplicate Raw rows
→ DO NOT advance forward watermark
```

Important distinction:

```text
BACKFILL
= recover previously missed historical data

REPLAY
= intentionally reprocess already-loaded historical data
```

---

# 4. Add tests first — RED phase

Append new tests to:

```text
warehouse/tests/test_load_orders.py
```

The new contracts cover:

```text
historical partition range overrides normal watermark
backfill skips already-loaded files
replay rereads already-loaded files
historical operations do not advance the normal watermark
```

Run:

```cmd
pytest warehouse\tests\test_load_orders.py -v
```

Observed RED result:

```text
3 passed
4 failed
```

The failures correctly showed that the new contracts were not yet implemented.

---

# 5. Implement historical-mode primitives

Update:

```text
warehouse/loader/load_orders.py
```

## 5.1 Extend partition discovery

`discover_partitions()` now accepts:

```python
watermark
start_partition
end_partition
```

Behavior:

```text
historical range supplied
→ use explicit range
→ ignore watermark for discovery

no historical range
→ preserve current forward-watermark behavior
```

Range boundaries are inclusive.

Example:

```text
--from 2026-08-12T20
--to   2026-08-13T00
```

includes matching partitions from that bounded range.

## 5.2 Add loaded-file decision helper

```python
def should_skip_file(
    is_loaded: bool,
    replay: bool,
) -> bool:
    return is_loaded and not replay
```

Meaning:

```text
unseen file + normal/backfill
→ process

loaded file + backfill
→ skip

loaded file + replay
→ process again
```

## 5.3 Add watermark decision helper

```python
def should_advance_watermark(
    historical_mode: bool,
) -> bool:
    return not historical_mode
```

Meaning:

```text
NORMAL
→ watermark may advance

BACKFILL
→ watermark unchanged

REPLAY
→ watermark unchanged
```

---

# 6. Validate primitives — GREEN phase

Run:

```cmd
pytest warehouse\tests\test_load_orders.py -v
```

Observed:

```text
7 passed
```

Run:

```cmd
pytest -v
```

Observed:

```text
18 passed
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

# 7. Add historical CLI contract

New CLI options:

```text
--from
--to
--replay
```

Accepted partition format:

```text
YYYY-MM-DDTHH
```

Backfill example:

```cmd
python warehouse\loader\load_orders.py ^
  --from 2026-08-12T20 ^
  --to 2026-08-13T00
```

Replay example:

```cmd
python warehouse\loader\load_orders.py ^
  --from 2026-08-12T20 ^
  --to 2026-08-13T00 ^
  --replay
```

---

# 8. CLI safety rules

Replay without range:

```cmd
python warehouse\loader\load_orders.py --replay
```

Expected:

```text
error: --replay requires --from and --to
```

Only one boundary:

```cmd
python warehouse\loader\load_orders.py ^
  --from 2026-08-13T00
```

Expected:

```text
error: --from and --to must be supplied together
```

Reverse range:

```cmd
python warehouse\loader\load_orders.py ^
  --from 2026-08-13T00 ^
  --to 2026-08-12T20
```

Expected:

```text
error: --from must be earlier than or equal to --to
```

---

# 9. Partition parsing implementation

The CLI value represents a lake partition, not a timezone-aware timestamp.

Parse:

```text
YYYY-MM-DDTHH
→ date
→ validated hour 00..23
→ (date, hour)
```

This avoids constructing a naive `datetime` and keeps Ruff clean.

---

# 10. Real BACKFILL discovery test

Run:

```cmd
python warehouse\loader\load_orders.py ^
  --from 2026-08-12T20 ^
  --to 2026-08-13T00
```

Observed:

```text
Current watermark: (datetime.date(2026, 8, 15), 11)
Mode: BACKFILL
Historical range: (datetime.date(2026, 8, 12), 20) to (datetime.date(2026, 8, 13), 0)
Eligible partitions: 1
```

The loader correctly scanned:

```text
2026-08-12 hour 20
```

and did not scan the forward watermark partition.

The historical partition contained 6 files, all already registered:

```text
Files discovered: 6
Files skipped: 6
Files loaded: 0
Rows processed: 0
Rows inserted: 0
Duplicate rows ignored: 0
```

---

# 11. Confirm backfill does not move forward watermark

Observed after backfill:

```text
2026-08-15 | 11
```

unchanged.

---

# 12. Real REPLAY test

Run:

```cmd
python warehouse\loader\load_orders.py ^
  --from 2026-08-12T20 ^
  --to 2026-08-12T20 ^
  --replay
```

Observed:

```text
Mode: REPLAY
Eligible partitions: 1
Scanning 2026-08-12 hour 20: 6 files
```

Summary:

```text
Files discovered:        6
Files skipped:           0
Files loaded:            6
Rows processed:         14
Rows inserted:           0
Duplicate rows ignored: 14
```

Replay idempotency is therefore proven.

---

# 13. Replay state validation

Raw before:

```text
525
```

Raw after:

```text
525
```

Loaded files before:

```text
276
```

Loaded files after:

```text
276
```

Watermark remained:

```text
2026-08-15 | 11
```

Strict health remained:

```text
HEALTHY
```

---

# 14. Controlled historical recovery test

Chosen historical file:

```text
data_lake/silver/orders/ingestion_date=2026-08-12/ingestion_hour=20/part-00000-33b5428e-0f9b-42fd-8da6-e64fa90f0306.c000.snappy.parquet
```

Its event:

```text
event_id = 68aa31d4-9274-42b3-a77c-8bca96bdc5b6
order_id = ORD-545344
row_count = 1
```

Delete exactly the one Raw event and its one `control.loaded_files` row in one transaction.

Observed:

```text
DELETE 1
DELETE 1
COMMIT
```

State after simulated loss:

```text
Raw orders   = 524
Loaded files = 275
Watermark    = 2026-08-15 11
```

Strict health correctly became:

```text
DEGRADED
```

with:

```text
Silver != Raw
Raw != Fact
```

---

# 15. Recover missing historical data using BACKFILL

Run:

```cmd
python warehouse\loader\load_orders.py ^
  --from 2026-08-12T20 ^
  --to 2026-08-12T20
```

Observed:

```text
Mode: BACKFILL
Eligible partitions: 1
```

Five files were skipped as already loaded.

The deliberately missing file was restored:

```text
processed=1
inserted=1
duplicates=0
```

Summary:

```text
Files discovered:       6
Files skipped:          5
Files loaded:           1
Rows processed:         1
Rows inserted:          1
Duplicate rows ignored: 0
```

---

# 16. Validate recovery

Observed:

```text
Raw orders   = 525
Loaded files = 276
Watermark    = 2026-08-15 11
```

Strict health returned:

```text
HEALTHY
```

No dbt rerun was required for this particular test because Fact never lost the event; restoring Raw re-established reconciliation.

---

# 17. Regression-check NORMAL mode

Run:

```cmd
python warehouse\loader\load_orders.py
```

Observed:

```text
Current watermark: (datetime.date(2026, 8, 15), 11)
Mode: NORMAL
Eligible partitions: 1
```

It scanned the current forward partition and skipped all 7 already-loaded files:

```text
Files discovered: 7
Files skipped: 7
Files loaded: 0
Rows processed: 0
Rows inserted: 0
Duplicate rows ignored: 0
```

Normal incremental behavior therefore remains unchanged.

---

# 18. Final quality gate

Run:

```cmd
pytest -v
```

Observed:

```text
18 passed
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
Bronze        525
Silver        525
Quarantine      0
Raw           525
Fact          525
Gold          525
Status        HEALTHY
```

CLI help:

```cmd
python warehouse\loader\load_orders.py --help
```

Observed options:

```text
--from START_PARTITION
--to END_PARTITION
--replay
```

---

# 19. Freshness metric interpretation

Historical backfill inserted a row with a new warehouse `loaded_at`.

Therefore:

```text
latest_loaded_at
= most recent warehouse load activity
```

It does not mean:

```text
latest source-event timestamp
```

This distinction should remain explicit in future observability work.

---

# 20. Session 12 proven properties

```text
[x] bounded historical partition discovery
[x] historical range overrides forward watermark for discovery only
[x] --from / --to CLI
[x] replay requires explicit bounded range
[x] incomplete ranges rejected
[x] reverse ranges rejected
[x] backfill skips already-loaded files
[x] replay rereads already-loaded files
[x] historical mode does not move normal watermark
[x] replay idempotency proven with real files
[x] 14 replayed rows produced 0 duplicate inserts
[x] replay leaves Raw count unchanged
[x] replay leaves loaded-file count unchanged
[x] replay leaves forward watermark unchanged
[x] strict health stays HEALTHY after replay
[x] real historical missing-load scenario simulated
[x] strict health detected the missing historical Raw row
[x] bounded backfill restored exactly one missing file / one row
[x] loaded-file control record restored
[x] forward watermark remained unchanged
[x] strict health returned to HEALTHY
[x] normal incremental mode regression-tested
[x] 18 tests passing
[x] Ruff passing
[x] CLI help validated
```

---

# 21. Architectural outcome

RetailPulse loader now has three explicit operating modes:

```text
NORMAL
→ forward incremental processing

BACKFILL
→ recover previously missed historical data

REPLAY
→ deliberately reprocess already-known historical data
```

State responsibilities remain separated:

```text
control.loader_watermarks
→ normal forward-processing position

control.loaded_files
→ file-level idempotency / processing history

raw.orders event_id uniqueness
→ business-level idempotency
```

Historical processing does not mutate normal forward-processing position.

---

# 22. Git update

Inspect:

```cmd
git status
git diff
```

Stage Session 12 changes:

```cmd
git add warehouse/loader/load_orders.py
git add warehouse/tests/test_load_orders.py
git add session_12_runbook.md
```

If runbooks live in an existing docs/runbooks directory, stage the actual copied path instead.

Review:

```cmd
git status
git diff --cached
```

Commit:

```cmd
git commit -m "Add controlled backfill and replay workflow"
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

GitHub Actions should return green after the push.

---

# 23. Validation gate before Session 13

```text
[x] 18 tests pass
[x] Ruff passes
[x] strict health returns HEALTHY
[x] replay proof completed
[x] real backfill recovery proof completed
[x] forward watermark unchanged by historical operations
[x] normal loader regression passes
[x] Git working tree clean after push
[x] GitHub Actions green
```

Next planned session:

```text
Session 13 — Data Contracts / Schema Evolution
```

Potential scope:

```text
producer schema version
required field contract
Spark schema validation
unknown/new fields
compatibility handling
invalid schema/data quarantine
contract tests
```
