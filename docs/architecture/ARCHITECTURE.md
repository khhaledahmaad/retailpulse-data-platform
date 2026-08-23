# RetailPulse Architecture

## 1. Purpose

RetailPulse is a local production-style reference architecture for streaming event ingestion and analytical processing. The order domain is replaceable; the engineering skeleton is the primary artifact.

The architecture is divided into a **data plane** and a **control/operations plane**.

![Architecture diagram](diagrams/retailpulse_architecture.svg)

Editable source: [`diagrams/retailpulse_architecture.drawio`](diagrams/retailpulse_architecture.drawio).

## 2. Data plane

```text
Source adapter
  ↓
Kafka
  ↓
Spark Structured Streaming
  ├─ Bronze
  ├─ Silver
  └─ Quarantine
  ↓
Incremental loader
  ↓
raw.orders
  ↓
dbt staging
  ↓
dbt fact
  ↓
dbt mart
```

### Source adapter

**Implementation:** `producer/src/producer.py`

The included adapter creates synthetic `order_created` events and publishes them to Kafka topic `orders`. In a real deployment this is the replaceable boundary: REST ingestion, CDC, telemetry decoding, file ingestion or another event publisher can publish the domain event contract instead.

### Kafka

**Infrastructure:** `docker-compose.yml`

Kafka 4.1.0 runs as a single local KRaft broker. External producer access is `localhost:9092`; Spark uses the internal Docker address `kafka:29092`.

Kafka is the upstream event transport and deepest practical replay boundary in the local architecture, subject to retained topic history.

### Spark Structured Streaming

**Canonical job:** `spark/jobs/stream_orders_to_lake.py`

**Contract/quality modules:**

- `spark/common/order_contract.py`
- `spark/common/order_quality.py`

**Parity utility:** `spark/tools/check_order_quality_parity.py`

The stream starts from `earliest` when there is no existing checkpoint. It reads Kafka once and derives three append-mode Parquet sinks:

- Bronze: `data_lake/bronze/orders`
- Silver: `data_lake/silver/orders`
- Quarantine: `data_lake/quarantine/orders`

Checkpoints live under `data_lake/checkpoints/`.

`stream_orders.py` is an earlier console/debug stream and is not the normal v1 lake pipeline.

### Bronze

Bronze preserves the Kafka envelope and raw payload:

- `kafka_key`
- `raw_payload`
- `topic`
- `partition`
- `offset`
- `kafka_timestamp`
- `ingested_at`
- `ingestion_date`

It is partitioned by `ingestion_date`.

### Contract and quality validation

Contract validation answers: **is this a supported event shape/version?**

Quality validation answers: **is the event acceptable for the order domain?**

Contract failures and quality failures are routed to Quarantine. Valid events are enriched and routed to Silver.

See [Data Contract](../data/DATA_CONTRACT.md).

### Silver

Silver contains validated analytical events plus Kafka lineage and derived fields such as `order_value` and `event_date`.

It is partitioned by:

```text
ingestion_date
ingestion_hour
```

Physical duplicate delivery is permitted at this layer. Business-state deduplication occurs at the warehouse boundary.

### Quarantine

Quarantine preserves rejected payloads and the reason for rejection:

- `raw_payload`
- `schema_version`
- `contract_error`
- `validation_error`
- Kafka lineage/timestamps

`warehouse/tools/reprocess_quarantine.py` provides audited dry-run repair and optional republication.

### Incremental warehouse loader

**Implementation:** `warehouse/loader/load_orders.py`

The loader:

1. discovers eligible hourly Silver partitions;
2. obtains the committed Silver file set from Spark `_spark_metadata`;
3. ignores physical Parquet files not present in the commit log;
4. skips already-loaded files in normal/backfill mode;
5. reads Parquet and inserts to `raw.orders`;
6. uses `ON CONFLICT (event_id) DO NOTHING` for logical idempotency;
7. records processed files and advances the normal-mode watermark;
8. optionally records loader metrics against an Airflow run id.

This makes `event_id` the warehouse exactly-once business boundary.

### PostgreSQL and dbt

**Warehouse bootstrap:** `warehouse/init/001_create_warehouse.sql`

**dbt project:** `warehouse/dbt/retailpulse/`

```text
raw.orders
   ↓
analytics.stg_orders       VIEW
   ↓
analytics.fct_orders       INCREMENTAL TABLE
   ↓
analytics.mart_daily_sales TABLE
```

`fct_orders` is keyed by `event_id` and has a dbt-managed unique B-tree index. `mart_daily_sales` aggregates by `event_date`.

## 3. Control and operations plane

### Airflow

**DAG:** `airflow/dags/retailpulse_warehouse_pipeline.py`

Schedule:

```text
*/10 * * * *
catchup=False
max_active_runs=1
```

Task chain:

```text
start_pipeline_run
→ run_incremental_loader
→ validate_raw_orders
→ run_dbt_build
→ check_pipeline_health
→ record_pipeline_metrics task marker
→ complete_pipeline_run
```

A `record_pipeline_failure` task uses `one_failed` to record failed runs in `control.pipeline_runs`.

The final local cold-start configuration intentionally uses explicit Compose startup rather than independently auto-restarting dependent Airflow services. After a PC/Docker restart the operator runs `docker compose up -d`; the Spark streaming application is then started separately.

### Monitoring

**Core health:** `warehouse/monitoring/check_pipeline_health.py`

**Configuration:** `warehouse/monitoring/config.py`

**Alerting:** `warehouse/monitoring/notifier.py`

**Terminal view:** `warehouse/monitoring/operations_view.py`

**Dashboard:** `warehouse/monitoring/operations_dashboard.py`

Health evaluates:

```text
Bronze ↔ Silver + Quarantine
Silver unique ↔ Raw
Raw ↔ Fact
Fact ↔ Gold order count
warehouse freshness
```

Default live tolerances:

```text
MAX_LAG_ROWS=60
MAX_LOAD_AGE_MINUTES=2880
```

`--strict` removes row-lag tolerance and requires exact reconciliation.

Every health execution writes a snapshot to `control.pipeline_metrics`, including `DEGRADED` snapshots. Incident state is reconciled in `control.pipeline_incidents`; new incidents and recoveries can send email notifications.

### Run lineage

`control.pipeline_runs` records:

- Airflow run id;
- start/end times;
- loader file/row counts;
- duplicate count;
- dbt/health status;
- final run status;
- error message.

## 4. Reliability semantics

### Delivery semantics

```text
Kafka / Spark / lake physical delivery: at-least-once
Warehouse logical business effect:      exactly-once by event_id
```

A repeated `event_id` can physically appear more than once in committed Silver. The Raw primary key plus `ON CONFLICT DO NOTHING` ensures it contributes once to business state.

A different `event_id` reusing the same `order_id` is treated as a business-key collision and is rejected by the dbt singular test `assert_unique_order_business_keys.sql`.

### Committed-file authority

Spark `_spark_metadata` is authoritative for streaming Parquet sinks. Raw filesystem presence is not enough to declare a file committed.

This rule is used by both health monitoring and the warehouse loader.

### Reconciliation invariants

When fully caught up:

```text
Bronze rows = Silver rows + Quarantine rows
Silver distinct(event_id) = raw.orders rows
raw.orders rows = analytics.fct_orders rows
analytics.fct_orders rows = SUM(analytics.mart_daily_sales.order_count)
```

## 5. Deployment topology

RetailPulse uses Docker Compose for local reproducibility:

| Service | Container | Role |
|---|---|---|
| PostgreSQL | `retailpulse-postgres` | Warehouse/control database |
| Kafka | `retailpulse-kafka` | Event broker |
| Kafka UI | `retailpulse-kafka-ui` | Broker inspection |
| Spark master | `retailpulse-spark-master` | Spark cluster master |
| Spark worker | `retailpulse-spark-worker` | Spark executor worker |
| Airflow DB | `retailpulse-airflow-db` | Airflow metadata |
| Airflow API | `retailpulse-airflow-api-server` | Airflow UI/API |
| Airflow scheduler | `retailpulse-airflow-scheduler` | DAG scheduling |
| Airflow DAG processor | `retailpulse-airflow-dag-processor` | DAG parsing/processing |

Named Docker volumes retain PostgreSQL, Airflow DB and Kafka data. The `data_lake/` directory is bind-mounted from the repository workspace and intentionally ignored by Git except for `.gitkeep` placeholders.

## 6. What is reusable vs domain-specific

| Component | Reuse for a new source? |
|---|---|
| Docker topology | Mostly keep |
| Kafka pattern | Keep; topic/config may change |
| Source adapter | Replace |
| Event contract | Replace/domain-version |
| Quality rules | Replace/domain-specific |
| Bronze/Silver/Quarantine pattern | Keep |
| Incremental loader mechanics | Mostly keep; remap columns/table |
| Raw schema | Adjust |
| dbt staging/fact/marts | Redefine |
| Airflow workflow pattern | Mostly keep |
| Run lineage | Keep |
| Reconciliation framework | Keep; redefine domain metrics |
| Incident lifecycle | Keep |
| Dashboard shell | Keep; adjust metrics/presentation |
| Replay/remediation patterns | Keep; adapt fields |

See [New Data Source Template](../handover/NEW_DATA_SOURCE_TEMPLATE.md).

## 7. Scope boundaries

RetailPulse is intentionally a local reference architecture. It does not claim:

- multi-broker Kafka high availability;
- multi-node PostgreSQL failover;
- managed object storage;
- Kubernetes/cloud deployment;
- enterprise IAM around the local dashboard;
- unbounded historical replay across every schema version.

Those would be deployment evolutions, not required to demonstrate the core data-engineering workflow.
