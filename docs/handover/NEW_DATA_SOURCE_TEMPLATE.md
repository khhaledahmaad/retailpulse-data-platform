# New Data Source Template — Reuse the RetailPulse Skeleton

## 1. Purpose

Use this document to adapt RetailPulse to a new event source without rebuilding the data-engineering workflow from zero.

The reusable skeleton is:

```text
SOURCE ADAPTER                  CHANGE
      ↓
KAFKA                           KEEP / CONFIGURE
      ↓
CONTRACT + QUALITY              CHANGE
      ↓
SPARK BRONZE/SILVER/QUARANTINE  KEEP PATTERN / ADAPT SCHEMA
      ↓
INCREMENTAL LOADER              KEEP MECHANICS / ADAPT MAPPING
      ↓
RAW WAREHOUSE                   ADAPT SCHEMA
      ↓
DBT STAGING / FACT / MART       CHANGE BUSINESS MODEL
      ↓
AIRFLOW                         MOSTLY KEEP
      ↓
HEALTH / METRICS / INCIDENTS    KEEP FRAMEWORK / ADAPT METRICS
      ↓
DASHBOARD                       KEEP SHELL / ADAPT PRESENTATION
```

## 2. Change map

| Capability | RetailPulse path | Default action for new source |
|---|---|---|
| Source adapter | `producer/src/producer.py` | **Replace** |
| Kafka infrastructure | `docker-compose.yml` | Keep; change topic/partitions as needed |
| Contract | `spark/common/order_contract.py` | **Replace/version** |
| Quality rules | `spark/common/order_quality.py` | **Replace domain rules** |
| Spark job | `spark/jobs/stream_orders_to_lake.py` | Adapt schema, names and derived fields |
| Quality parity | `spark/tools/check_order_quality_parity.py` | Keep pattern; replace fixtures/rules |
| Lake pattern | `data_lake/bronze|silver|quarantine` | Keep pattern; rename dataset paths |
| Warehouse DDL | `warehouse/init/001_create_warehouse.sql` | Adjust Raw/business columns; retain control framework where possible |
| Loader | `warehouse/loader/load_orders.py` | Adapt target columns/table; retain committed-file/idempotent patterns |
| dbt staging | `warehouse/dbt/retailpulse/models/staging/` | Redefine |
| dbt fact | `warehouse/dbt/retailpulse/models/facts/` | Redefine grain/key |
| dbt marts | `warehouse/dbt/retailpulse/models/marts/` | Redefine business outputs |
| Airflow | `airflow/dags/retailpulse_warehouse_pipeline.py` | Mostly retain workflow; rename tasks/models if needed |
| Health | `warehouse/monitoring/check_pipeline_health.py` | Redefine reconciliation/domain metrics |
| Monitoring config | `warehouse/monitoring/config.py` | Adjust SLO defaults |
| Alerts | `warehouse/monitoring/notifier.py` | Usually retain |
| Operations view/dashboard | `warehouse/monitoring/operations_*` | Adjust labels/metrics |
| Quarantine repair | `warehouse/tools/reprocess_quarantine.py` | Retain workflow; adapt fields/identity invariants |
| Targeted repair tools | `warehouse/tools/` | Create only for real domain correction needs |
| CI | `.github/workflows/ci.yml` | Retain quality gates; update model/source checks |
| Tests | test directories | Replace/add fixtures and domain assertions |

## 3. New-source design form

Copy this section for each source.

```text
Source name:
Business owner:
Technical owner:
Source protocol: REST / CDC / files / telemetry / other
Source system:
Expected average events/sec:
Expected burst events/sec:
Kafka topic:
Kafka key:
Schema version strategy:
Event identity field:
Business identity field(s):
Event-time field:
Required fields:
Optional fields:
Late-arrival expectation:
Duplicate-delivery expectation:
Retention/replay requirement:
PII/sensitivity classification:
```

### Contract

```text
Current schema version:
Supported versions:
Required structural fields:
Unknown-field policy:
Unsupported-version policy:
Contract error codes:
```

### Quality

```text
Rule name:
Error code:
Acceptance condition:
Business reason:
```

Repeat for every rule.

### Lake

```text
Bronze path:
Bronze partitioning:
Silver path:
Silver partitioning:
Quarantine path:
Checkpoint names:
Derived Silver fields:
```

### Warehouse

```text
Raw schema/table:
Raw grain:
Raw primary/idempotency key:
Staging model:
Fact model:
Fact grain:
Fact unique key:
Business marts:
Mart grain(s):
```

### Monitoring

```text
Primary reconciliation equation:
Allowed live lag:
Freshness SLO:
Required domain metrics:
Incident types:
Alert recipients:
Dashboard KPIs:
```

### Recovery

```text
Deepest retained replay source:
Lake recovery method:
Warehouse recovery method:
Schema-version replay strategy:
Backfill boundaries:
```

## 4. Example adaptation — decoded Sentinel-style telemetry

This example shows how the order-domain implementation could become a telemetry pipeline without replacing the overall architecture.

### Current source

```text
producer/src/producer.py
  ↓
Kafka topic: orders
```

### Replacement source

Possible adapter:

```text
decoded telemetry / REST polling / decoder output
  ↓
Kafka topic: telemetry
```

The adapter could publish already decoded events or redirect decoded records into Kafka. Kafka remains the event buffer and replay transport.

### Example telemetry contract

Instead of order fields:

```text
event_id
order_id
customer_id
product_id
category
quantity
unit_price
currency
```

use a domain contract such as:

```text
schema_version
event_id
event_type
event_timestamp
wagon_id
vehicle_number
message_type
location
speed
sensor_value
...
```

The exact fields must come from the real telemetry domain; this list is illustrative, not a prescribed Sentinel schema.

### Example telemetry quality rules

Replace retail rules such as positive price/GBP/category with domain rules such as:

```text
wagon_id present
message_type supported
event timestamp valid
location representation valid
sensor value inside plausible domain range
speed inside plausible domain range
```

Again, actual ranges should come from domain specification, not assumptions in the framework.

### Spark adaptation

Create/rename the domain modules, for example:

```text
spark/common/telemetry_contract.py
spark/common/telemetry_quality.py
spark/jobs/stream_telemetry_to_lake.py
spark/tools/check_telemetry_quality_parity.py
```

Retain the processing pattern:

```text
Kafka
→ Bronze
→ parse
→ contract
→ quality
→ Silver / Quarantine
```

### Warehouse adaptation

Instead of:

```text
raw.orders
stg_orders
fct_orders
mart_daily_sales
```

use domain-oriented names, e.g.:

```text
raw.telemetry_events
stg_telemetry
fct_telemetry_events
mart_wagon_daily_health
mart_location_activity
```

The actual marts should be driven by business/operational questions, not by copying the retail mart shape.

### Airflow adaptation

The workflow can remain structurally similar:

```text
start run
→ incremental load
→ validate Raw
→ dbt build
→ health
→ metrics
→ complete/fail run
```

Change table/model names and validation queries, not the orchestration philosophy.

### Monitoring adaptation

Generic metrics can remain conceptually similar:

```text
Bronze rows
Silver rows
Silver unique events
Quarantine rows
Raw rows
Fact rows
freshness
run status
incidents
```

Domain metrics should change. For telemetry examples:

```text
reporting wagons
events per wagon
missing-location rate
invalid sensor rate
latest event age
```

## 5. Implementation sequence for a new source

### Phase A — source and contract

1. Define event identity, business identity and event time.
2. Define versioned contract.
3. Implement/replace source adapter.
4. Create contract tests.

### Phase B — lake

5. Adapt Spark schema and dataset paths.
6. Implement domain quality rules.
7. Implement/retain parity checks.
8. Validate Bronze = Silver + Quarantine under stable input.

### Phase C — warehouse

9. Define Raw grain and idempotency key.
10. Adapt `001_create_warehouse.sql`.
11. Adapt loader mapping while retaining committed-file authority.
12. Define dbt staging/fact/mart grains.
13. Add uniqueness/business tests.

### Phase D — orchestration and operations

14. Adapt Airflow task names/queries without changing workflow unnecessarily.
15. Define reconciliation equations and freshness SLO.
16. Adapt incident types and dashboard metrics.
17. Exercise retry/failure/recovery.

### Phase E — production-readiness validation

18. Fresh start from documented build.
19. Small E2E smoke batch.
20. Representative burst/load test.
21. Backfill/replay test.
22. Quarantine remediation test.
23. Analytical DR test.
24. Documentation and handover review.

## 6. Definition of done for a reused skeleton

- [ ] source publishes the agreed versioned contract;
- [ ] invalid structure and invalid domain data are distinguishable;
- [ ] committed lake state is authoritative;
- [ ] loader is idempotent at the chosen logical event key;
- [ ] Fact grain/key is documented and tested;
- [ ] business marts have explicit grains/definitions;
- [ ] Airflow run lineage records success/failure and loader metrics;
- [ ] strict reconciliation is defined and passes after catch-up;
- [ ] replay/backfill behaviour is tested;
- [ ] quarantine remediation is auditable;
- [ ] DR source and limitations are documented;
- [ ] build/start instructions work from a clean clone;
- [ ] dashboard/monitoring terminology matches the new domain;
- [ ] README, contract, catalogue and glossary are updated.
