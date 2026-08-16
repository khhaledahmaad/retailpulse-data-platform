# Session 14 Runbook — Late-Arriving Data / Event-Time Correctness

## Session goal

Prove that RetailPulse correctly handles late-arriving business events without requiring backfill or replay.

A late-arriving event is:

```text
ingested now
but
event_timestamp belongs to an older business date
```

Session 14 validates that:

- event time and ingestion time remain distinct
- late events land in the current ingestion partition
- historical `event_date` is preserved
- the normal forward loader discovers the event
- dbt incremental Fact accepts it because incrementality is based on `loaded_at`
- Gold retroactively corrects the historical business date
- no backfill/replay is needed
- strict reconciliation remains HEALTHY

No production code change was required.

---

# 1. Baseline

```cmd
git status
pytest -v
ruff check .
python warehouse\monitoring\check_pipeline_health.py --strict
```

Observed:

```text
23 tests passed
Ruff clean

Bronze rows:       651
Silver rows:       647
Quarantine rows:   4
Raw orders:        647
Fact orders:       647
Gold order count:  647
Status: HEALTHY
```

---

# 2. Confirm dbt incremental strategy is late-arrival safe

`fct_orders.sql` increments on:

```sql
where loaded_at > (
    select coalesce(
        max(loaded_at),
        '1900-01-01'::timestamptz
    )
    from {{ this }}
)
```

This is correct for late-arriving data because incrementality is driven by warehouse load time, not by business `event_timestamp`.

The Gold mart groups by:

```sql
event_date
```

Therefore a newly loaded event with an old business date should still enter Fact and then correct the historical Gold date.

---

# 3. Capture historical Gold baseline

Query:

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse -c "SELECT event_date, order_count, units_sold, gross_revenue, average_order_value FROM analytics.mart_daily_sales WHERE event_date = DATE '2026-08-14';"
```

Observed:

```text
event_date            2026-08-14
order_count           363
units_sold            1057
gross_revenue         82159.91
average_order_value   226.34
```

---

# 4. Send one deliberately late event

Controlled event:

```text
event_id        0bf3a402-2dec-4580-a42b-093b1201d859
order_id        ORD-LATE-ARRIVAL-001
schema_version  1
event_timestamp 2026-08-14T15:30:00+00:00
quantity        2
unit_price      25.00
order_value     50.00
```

The event arrived on 2026-08-16 but belongs to the 2026-08-14 business date.

---

# 5. Verify expected streaming lag

Run:

```cmd
python warehouse\monitoring\check_pipeline_health.py
```

Observed:

```text
Bronze rows:       652
Silver rows:       648
Quarantine rows:   4
Raw orders:        647
Fact orders:       647
Gold order count:  647

Status: WARNING
```

Expected issue:

```text
Silver leads Raw by 1
```

Core invariant:

```text
Bronze = Silver + Quarantine
652 = 648 + 4
```

---

# 6. Prove event time differs from ingestion time

Silver inspection showed:

```text
event_id         0bf3a402-2dec-4580-a42b-093b1201d859
order_id         ORD-LATE-ARRIVAL-001

event_timestamp  2026-08-14 15:30:00
event_date       2026-08-14

ingested_at      2026-08-16 22:11:00.898
ingestion_date   2026-08-16
ingestion_hour   22

quantity         2
unit_price       25.0
order_value      50.0
```

This proves:

```text
business event time
!=
platform ingestion time
```

The event belongs analytically to 2026-08-14 but physically arrived in the 2026-08-16 hour-22 ingestion partition.

---

# 7. Load through NORMAL mode

Run:

```cmd
python warehouse\loader\load_orders.py
```

Observed:

```text
Current watermark: (datetime.date(2026, 8, 16), 21)
Mode: NORMAL
Eligible partitions: 2
```

The current ingestion partition:

```text
2026-08-16 hour 22
```

contained one new file.

Observed:

```text
Files discovered: 24
Files skipped:    23
Files loaded:      1
Rows processed:    1
Rows inserted:     1
Duplicate rows ignored: 0
```

Key proof:

```text
late business event
→ normal forward ingestion path
→ no backfill
→ no replay
```

---

# 8. Run dbt

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

Fact result:

```text
INSERT 0 1
```

The old `ordered_at` did not prevent the event from entering Fact.

---

# 9. Confirm late event in Fact

Observed:

```text
event_id      0bf3a402-2dec-4580-a42b-093b1201d859
order_id      ORD-LATE-ARRIVAL-001
ordered_at    2026-08-14 15:30:00+00
event_date    2026-08-14
quantity      2
unit_price    25.00
order_value   50.00
loaded_at     2026-08-16 22:13:22.846883+00
```

This confirms:

```text
ordered_at/event_date
→ historical business time

loaded_at
→ current warehouse load time
```

---

# 10. Prove historical Gold correction

Before:

```text
2026-08-14
order_count    363
units_sold     1057
gross_revenue  82159.91
```

After:

```text
2026-08-14
order_count            364
units_sold             1059
gross_revenue          82209.91
average_order_value    225.85
```

Exact deltas:

```text
order_count    +1
units_sold     +2
gross_revenue  +50.00
```

matching:

```text
2 × 25.00 = 50.00
```

The historical analytical day was corrected automatically.

---

# 11. Final strict reconciliation

```cmd
python warehouse\monitoring\check_pipeline_health.py --strict
```

Observed:

```text
Bronze rows:       652
Silver rows:       648
Quarantine rows:   4
Raw orders:        648
Fact orders:       648
Gold order count:  648

Status: HEALTHY
```

Invariants:

```text
Bronze = Silver + Quarantine
652 = 648 + 4
```

and:

```text
Silver = Raw = Fact = Gold
648 = 648 = 648 = 648
```

---

# 12. Session 14 proven properties

```text
[x] late event accepted through normal Kafka/Spark path
[x] historical event_timestamp preserved
[x] historical event_date preserved
[x] current ingestion_date preserved separately
[x] event stored in current ingestion partition
[x] normal forward loader discovered it
[x] no backfill required
[x] no replay required
[x] dbt incremental Fact accepted the event
[x] incrementality based on loaded_at proved late-arrival safe
[x] historical Gold date corrected automatically
[x] order_count increased by exactly 1
[x] units_sold increased by exactly 2
[x] gross_revenue increased by exactly 50.00
[x] strict health returned HEALTHY
```

---

# 13. Architectural outcome

RetailPulse now has explicit late-arrival semantics:

```text
event_timestamp / event_date
→ business semantics

kafka_timestamp / ingested_at / ingestion_date / ingestion_hour
→ processing semantics

loaded_at
→ warehouse incremental-processing semantics
```

This allows:

```text
old business event
+
new ingestion/load time
→ normal incremental processing
→ historical analytical correction
```

without historical pipeline rewinds.

---

# 14. Late arrival vs backfill

```text
Late-arriving event
→ new event arrives now
→ carries older event_timestamp
→ normal forward path

Backfill
→ historical ingestion/file was previously missed
→ explicitly rescan historical ingestion partition/range
```

Session 12 handles backfill/replay.

Session 14 proves late-arriving event handling.

---

# 15. Final regression gate

Run:

```cmd
pytest -v
ruff check .
python warehouse\monitoring\check_pipeline_health.py --strict
git status
```

Expected:

```text
23 tests passed
Ruff clean
Status: HEALTHY
```

No production code change is required for Session 14.

---

# 16. Git update

Copy this runbook to:

```text
docs/sessions/session_14_runbook.md
```

Inspect:

```cmd
git status
git diff
```

Since Session 14 is a validation session, the runbook should be the only intended repository change.

Stage:

```cmd
git add docs/sessions/session_14_runbook.md
```

Review:

```cmd
git status
git diff --cached
```

Commit:

```cmd
git commit -m "Validate late-arriving event-time handling"
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

# 17. Session summary

RetailPulse now demonstrates:

```text
resilient forward processing
+
idempotent replay/backfill
+
versioned schema compatibility
+
late-arriving event-time correctness
```

Late business events are ingested normally, retain their original historical event time, and update historical analytical aggregates without requiring a backfill or rebuild.

Session 14 complete.
