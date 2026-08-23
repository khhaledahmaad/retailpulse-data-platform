# RetailPulse Business and Engineering Glossary

## Business terms

**Order**  
A customer purchase identified by `order_id`. In the reference generator each created event receives a new independent order id.

**Order event**  
An immutable emitted event identified by `event_id`. RetailPulse currently supports the event type `order_created`.

**Order value**  
`quantity × unit_price`, rounded to two decimal places in Silver.

**Units sold**  
Sum of `quantity` across fact rows for a reporting grain.

**Gross revenue**  
Sum of `order_value`. The project treats this as a simple gross analytical measure; it does not model refunds, tax, discounts or payment settlement.

**Average order value**  
Average `order_value` for the daily mart grain.

**Daily sales**  
The `mart_daily_sales` aggregation at one row per `event_date`.

## Identity terms

**`event_id`**  
Logical event identity. The producer generates UUID4 strings. It is the Raw primary key and warehouse idempotency boundary.

**`order_id`**  
Business order identity, independent of `event_id`.

**Duplicate delivery**  
The same `event_id` appears more than once physically. This is allowed in at-least-once streaming delivery and must have one logical warehouse effect.

**Business-key collision**  
The same `order_id` is associated with different `event_id` values. RetailPulse treats this as inconsistent business identity and detects it with a dbt singular test.

## Data-layer terms

**Bronze**  
Raw committed Kafka delivery data plus broker/ingestion lineage. Bronze is not guaranteed to be domain-valid.

**Silver**  
Events that passed the current contract and domain quality rules, enriched for warehouse loading. Silver may contain physical duplicate deliveries.

**Silver unique event**  
A distinct `event_id` present in committed Silver. This is the logical Silver count used for warehouse reconciliation.

**Quarantine**  
Rejected events retained with contract/quality error details and Kafka lineage for investigation/remediation.

**Raw warehouse**  
`raw.orders`, the PostgreSQL load boundary populated from committed Silver. Despite the name, this table contains already validated Silver events rather than unvalidated Kafka payloads.

**Staging model**  
`analytics.stg_orders`, a dbt view that creates the analytical transformation boundary over Raw.

**Fact table**  
`analytics.fct_orders`, the incremental event-level analytical table.

**Mart**  
A business-oriented analytical output. RetailPulse v1 uses `analytics.mart_daily_sales` at daily grain.

**Grain**  
What one row represents. For example, one `fct_orders` row represents one logical event; one `mart_daily_sales` row represents one event date.

## Governance terms

**Data contract**  
Rules defining the supported structure/version of incoming events. It answers what producers must send for the payload to be structurally accepted.

**Data quality rule**  
A domain acceptance rule applied after the contract passes, such as positive quantity or supported currency.

**Data catalogue**  
Technical inventory of datasets/tables, grains, columns, keys and lineage.

**Business glossary**  
Shared definitions for business and engineering terms so people use the same language consistently.

**Schema version**  
Version marker inside the event payload. RetailPulse currently supports version `1`.

## Operational terms

**Committed file**  
A Parquet file present in Spark streaming `_spark_metadata`. Physical presence alone does not make a file authoritative.

**Physical orphan file**  
A file that exists on disk but is absent from the Spark commit metadata. It must not be treated as committed source data.

**Loader watermark**  
Latest normal-mode Silver ingestion partition tracked for incremental discovery. Historical backfill/replay does not advance it.

**Loaded-file registry**  
`control.loaded_files`, which records Silver files already processed by the warehouse loader.

**Backfill**  
Bounded historical load using `--from` and `--to`; already-loaded files remain skipped.

**Replay**  
Bounded historical load with `--replay`, causing already-registered files in the range to be reread. Warehouse `event_id` idempotency prevents duplicate logical effects.

**Reprocessing**  
Repairing a quarantined event, revalidating it and optionally republishing it to Kafka with an audit record.

**Reconciliation**  
Cross-layer equality checks proving logical state agrees, e.g. Silver unique = Raw = Fact = Gold order count.

**Live lag**  
Temporary difference between an upstream committed count and a downstream count while the system is catching up.

**Strict health**  
Health mode where cross-layer row reconciliation must be exact; no live lag tolerance is applied.

**Freshness**  
Age in minutes since the latest `raw.orders.loaded_at`. Default tolerated age is configured by `MAX_LOAD_AGE_MINUTES`.

**HEALTHY**  
No reconciliation or freshness issue detected.

**WARNING**  
A non-strict live reconciliation difference exists but remains within configured tolerance.

**DEGRADED**  
A reconciliation/freshness condition exceeds tolerance, is logically impossible (for example Raw ahead of Silver unique), or any mismatch occurs in strict mode.

**Incident**  
Persisted operational problem in `control.pipeline_incidents`, opened/updated/resolved by health evaluation.

**Pipeline run lineage**  
Per-Airflow-run operational history in `control.pipeline_runs`, including loader metrics and final run state.
