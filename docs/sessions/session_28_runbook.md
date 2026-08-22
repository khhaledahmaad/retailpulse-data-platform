# Session 28 Runbook — Warehouse Optimisation

**Project:** RetailPulse Data Platform  
**Session:** 28  
**Date:** 2026-08-22  
**Goal:** Use Session 27 performance measurements to optimise the PostgreSQL/dbt warehouse only where measurements justify a change.

---

## 1. Starting point

Session 27 established that the platform handled 1K, 5K, and 20K event bursts without data loss and that the 20K catch-up load completed successfully.

Relevant Session 27 baseline:

- Logical business rows: **29,130**
- Silver physical rows: **29,132**
- Silver unique events: **29,130**
- Physical Silver duplicate deliveries: **2**
- Quarantine rows: **5**
- Raw = Fact = Gold = **29,130**
- 20K loader time: **6.75 s**
- 20K loader throughput: **~2,963 rows/s**
- dbt at 20K scale: **12.77 s** in the full scheduled pipeline
- Health check remained the dominant fixed-cost step because of many small historical Parquet files.

Session 28 deliberately avoided optimisation-by-assumption. Each candidate was measured first.

---

## 2. Warehouse model inventory

### `raw.orders`

PostgreSQL landing table loaded from Spark Silver.

Important existing constraint/index:

```sql
PRIMARY KEY (event_id)
```

This automatically provides a unique B-tree index on `event_id`.

The loader uses:

```sql
ON CONFLICT (event_id) DO NOTHING
```

so the existing primary-key index already supports exactly-once logical insertion efficiently.

### `analytics.stg_orders`

A dbt view directly over `raw.orders`.

```sql
from {{ source('raw', 'orders') }}
```

Any physical index needed for a staging predicate would therefore belong on `raw.orders`, not on the view.

### `analytics.fct_orders`

dbt incremental analytical fact table.

Initial config:

```jinja
{{
    config(
        materialized='incremental',
        unique_key='event_id'
    )
}}
```

Incremental filter:

```sql
where loaded_at > (
    select coalesce(
        max(loaded_at),
        '1900-01-01'::timestamptz
    )
    from {{ this }}
)
```

### `analytics.mart_daily_sales`

dbt table rebuilt from the fact table and grouped by `event_date`.

---

## 3. Baseline: `loaded_at` incremental-query plans

### `MAX(loaded_at)` on `fct_orders`

Baseline:

```text
Seq Scan on fct_orders
Rows scanned: 29,130
Execution Time: 6.648 ms
Buffers: shared hit=644
```

PostgreSQL correctly chose a sequential scan because the table is still small.

### Raw incremental-filter lookup

Query pattern:

```sql
SELECT COUNT(*)
FROM analytics.stg_orders
WHERE loaded_at > (
    SELECT COALESCE(
        MAX(loaded_at),
        '1900-01-01'::timestamptz
    )
    FROM analytics.fct_orders
);
```

Baseline:

```text
Seq Scan on fct_orders
Seq Scan on raw.orders
Rows removed by raw filter: 29,130
Execution Time: 12.534 ms
```

---

## 4. Experimental `loaded_at` indexes

Temporary indexes were created:

```sql
CREATE INDEX idx_raw_orders_loaded_at
ON raw.orders (loaded_at);

CREATE INDEX idx_fct_orders_loaded_at
ON analytics.fct_orders (loaded_at);
```

### Query-plan improvement

`MAX(fct_orders.loaded_at)`:

```text
Before: Seq Scan, ~6.65 ms
After:  Index Only Scan Backward, ~0.09 ms
Heap Fetches: 0
```

Raw incremental-filter lookup:

```text
Before: Seq Scan across raw + fact, ~12.5 ms
After:  Index Only Scan, sub-millisecond scan
Heap Fetches: 0
```

The micro-query improvement was large.

### Actual dbt workload test

With the temporary indexes:

```text
Run 1: 17.94 s  (startup/cold outlier)
Run 2:  6.19 s  (warm comparison)
```

Indexes were then removed.

Without the indexes:

```text
6.49 s
6.35 s
6.29 s
6.37 s
Average: ~6.38 s
```

### Decision

**Rejected.**

The query-plan improvement did not translate into a meaningful end-to-end dbt improvement. The roughly 0.2-second difference was within normal process/startup variation.

No permanent indexes were added on:

- `raw.orders.loaded_at`
- `analytics.fct_orders.loaded_at`

---

## 5. Gold aggregation analysis

The full Gold aggregation was measured directly:

```sql
SELECT
    event_date,
    COUNT(*) AS order_count,
    SUM(quantity) AS units_sold,
    ROUND(SUM(order_value), 2) AS gross_revenue,
    ROUND(AVG(order_value), 2) AS average_order_value
FROM analytics.fct_orders
GROUP BY event_date;
```

Plan:

```text
HashAggregate
Rows scanned: 29,130
Output groups: 10
Memory Usage: 24 kB
Execution Time: 12.149 ms
```

### Decision

No optimisation justified.

Rejected:

- index on `mart_daily_sales.event_date`
- fact-table partitioning
- incremental Gold mart

At ~29K rows, a complete aggregation in ~12 ms is already trivial.

---

## 6. `fct_orders.event_id` index experiment

`fct_orders` declared:

```jinja
unique_key='event_id'
```

but initially had no PostgreSQL index on `event_id`.

### Baseline lookup

```sql
SELECT *
FROM analytics.fct_orders
WHERE event_id = (
    SELECT event_id
    FROM analytics.fct_orders
    LIMIT 1
);
```

Plan:

```text
Seq Scan on fct_orders
Rows Removed by Filter: 29,129
Execution Time: 4.598 ms
```

### Experimental unique index

```sql
CREATE UNIQUE INDEX idx_fct_orders_event_id
ON analytics.fct_orders (event_id);
```

The creation succeeded, proving no duplicate non-null `event_id` values existed.

Lookup after index:

```text
Index Scan using idx_fct_orders_event_id
Execution Time: 0.243 ms
```

Approximately a **19x lookup improvement**.

Additional integrity validation:

```text
rows              = 29,130
unique_event_ids  = 29,130
null_event_ids    = 0
```

### Why this index was different

The retained index has two purposes:

1. **Performance** — fast event identity lookup.
2. **Integrity** — PostgreSQL itself enforces one fact row per `event_id`.

This is stronger justification than a micro-performance-only index.

---

## 7. Make the Fact index dbt-owned

Because `analytics.fct_orders` is owned by dbt, the index should not be manually defined in the warehouse init SQL.

The model config was updated to:

```jinja
{{
    config(
        materialized='incremental',
        unique_key='event_id',
        indexes=[
            {'columns': ['event_id'], 'unique': true}
        ]
    )
}}
```

This preserves clear ownership:

```text
Warehouse init SQL
    -> raw/control infrastructure

dbt
    -> analytics models and their physical indexes
```

### Full-refresh proof

A dbt full refresh was run for `fct_orders`.

Result:

```text
SELECT 29130
Completed successfully
```

Post-refresh inspection showed that dbt recreated a generated-name index:

```text
UNIQUE, btree (event_id)
```

This proved the index is reproducible infrastructure and survives table recreation/full refresh.

---

## 8. Raw loader index validation

`raw.orders(event_id)` already has a primary-key B-tree index.

First lookup:

```text
Index Scan using orders_pkey
Execution Time: 6.553 ms
Buffers: shared hit=3 read=3
```

The higher first timing was cold-cache I/O.

Immediate repeat:

```text
Index Scan using orders_pkey
Buffers: shared hit=6
Execution Time: 0.123 ms
```

### Decision

No Raw index change required.

The existing primary key already provides efficient lookup and conflict detection for:

```sql
ON CONFLICT (event_id) DO NOTHING
```

---

## 9. Loader SQL / COPY decision

The loader currently uses `executemany()` and `ON CONFLICT (event_id) DO NOTHING`.

Session 27 already demonstrated:

```text
20,000 rows inserted
Loader time: 6.75 s
Throughput: ~2,963 rows/s
```

Replacing this with PostgreSQL `COPY` would complicate conflict handling and the existing exactly-once business semantics.

### Decision

**Do not migrate to COPY.**

Current loader throughput is sufficient for the project workload and schedule.

---

## 10. Control-table index review

Existing warehouse-init indexes/constraints were reviewed.

Important examples:

- `raw.orders(event_id)` primary key
- `control.loaded_files(file_path)` primary key
- `control.loader_watermarks(dataset_name)` primary key
- `control.pipeline_metrics(metric_id)` primary key
- `control.pipeline_runs(pipeline_run_id)` primary key
- `control.pipeline_runs(airflow_run_id)` unique
- `idx_pipeline_runs_started_at`
- `idx_event_reprocessing_log_event_id`
- partial unique `idx_pipeline_incidents_open_type`

The partial incident index enforces one unresolved incident per incident type:

```sql
CREATE UNIQUE INDEX IF NOT EXISTS
    idx_pipeline_incidents_open_type
ON control.pipeline_incidents (incident_type)
WHERE resolved_at IS NULL;
```

---

## 11. Operations Dashboard query analysis

The dashboard uses real query patterns including:

```sql
FROM control.pipeline_metrics
ORDER BY recorded_at DESC
LIMIT 1
```

and:

```sql
FROM control.pipeline_metrics
WHERE silver_unique_events IS NOT NULL
ORDER BY recorded_at DESC
LIMIT 24
```

### Latest-metric query

Approximate table size:

```text
434 rows
```

Plan:

```text
Seq Scan
Top-N heapsort
Memory: 25 kB
Execution Time: 0.155 ms
```

### 24-row metric-history query

Plan:

```text
434 total metric rows
142 matching
292 removed by filter
Seq Scan + top-N sort
Memory: 28 kB
Execution Time: 0.258 ms
```

### Decision

No `pipeline_metrics.recorded_at` index.

The current dashboard queries are already effectively free.

---

## 12. Partitioning decision

Partitioning was reviewed conceptually.

Example monthly partitioning would split a logical fact table into child tables such as:

```text
fct_orders
├── fct_orders_2026_01
├── fct_orders_2026_02
├── ...
└── fct_orders_2026_08
```

At large scale, PostgreSQL can use partition pruning to avoid scanning irrelevant monthly partitions.

For RetailPulse today:

- Fact rows: ~29K
- Full Gold aggregation: ~12 ms
- Fact-key indexed lookup: sub-millisecond
- 10-minute pipeline cadence

### Decision

**Do not partition `fct_orders`.**

Partitioning would add operational/dbt/index complexity without solving a current performance problem.

It would become worth reconsidering only at much larger scale, especially if fact data reaches tens/hundreds of millions of rows and date-range queries dominate.

---

## 13. Lake vs warehouse terminology clarified

RetailPulse has two separate processing/storage layers.

### Lake / streaming side

```text
Kafka
  -> Bronze Parquet
  -> Silver Parquet
  -> Quarantine Parquet
```

These Parquet files are the project's **lake data**.

### Warehouse side

```text
Spark Silver
  -> raw.orders
  -> stg_orders
  -> fct_orders
  -> mart_daily_sales
```

Useful mental model:

```text
Spark Bronze      = raw lake data
Spark Silver      = cleaned/validated lake data
raw.orders        = warehouse landing copy of cleaned Silver data
fct_orders        = curated analytical fact / warehouse Silver-like layer
mart_daily_sales  = warehouse Gold/business aggregate
```

`raw.orders` should not be described as identical to Spark Bronze because it is populated from Spark Silver.

---

## 14. Final optimisation decisions

| Candidate | Decision | Evidence |
|---|---|---|
| `raw.orders(event_id)` | KEEP existing PK | warm indexed lookup ~0.123 ms |
| `fct_orders(event_id)` | **ADD/KEEP dbt-owned UNIQUE index** | ~4.60 ms -> ~0.24 ms + integrity |
| `raw.orders.loaded_at` | REJECT | micro-query gain, negligible dbt gain |
| `fct_orders.loaded_at` | REJECT | micro-query gain, negligible dbt gain |
| `mart_daily_sales.event_date` index | REJECT | full aggregation ~12 ms |
| `pipeline_metrics.recorded_at` index | REJECT | dashboard ~0.155-0.258 ms |
| Loader `executemany()` -> `COPY` | REJECT | 20K load already 6.75 s |
| Fact partitioning | REJECT | current scale does not justify complexity |
| Gold incrementalisation | REJECT | full rebuild ~12 ms at SQL level |

The key outcome of Session 28 was therefore not "add lots of optimisation." It was **measure, prove, and keep only one justified change**.

---

## 15. Final quality gate

### Ruff

```text
All checks passed!
```

### Pytest

```text
71 passed in 13.96s
```

### dbt build

```text
PASS=23
WARN=0
ERROR=0
SKIP=0
TOTAL=23
```

Notable model timings from the final build:

```text
fct_orders incremental: 0.28 s
mart_daily_sales:       0.14 s
```

### Strict health

```text
Bronze rows:        29137
Silver rows:        29132
Silver unique:      29130
Silver duplicates:  2
Quarantine rows:    5
Raw orders:         29130
Fact orders:        29130
Gold order count:   29130
Status:             HEALTHY
```

Business reconciliation remained exact:

```text
Silver unique = Raw = Fact = Gold = 29,130
```

The two physical Silver duplicate deliveries remain valid and correctly tolerated by the architecture.

---

## 16. Session 28 final state

Session 28 is complete.

Permanent code change:

```text
analytics.fct_orders
-> dbt-managed UNIQUE B-tree index on event_id
```

No unnecessary warehouse complexity was added.

The platform remains:

- fully reconciled
- tested
- lint-clean
- dbt-green
- strict-health HEALTHY
- ready for Session 29: Disaster Recovery / Full Rebuild
