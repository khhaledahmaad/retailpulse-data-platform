# RetailPulse Event Data Contract

## 1. Contract identity

```text
Domain: Retail order events
Current schema_version: 1
Supported schema versions: {1}
Kafka topic: orders
Canonical contract module: spark/common/order_contract.py
Spark enforcement: spark/jobs/stream_orders_to_lake.py
```

The contract defines whether an incoming event is structurally acceptable for the current version. Domain-quality rules are evaluated separately after the contract passes.

## 2. Version 1 fields

| Field | Contract requirement | Producer representation | Meaning |
|---|---|---|---|
| `schema_version` | Required; supported value `1` | integer | Contract version |
| `event_id` | Required/non-null | UUID4 string | Immutable event identity and warehouse idempotency key |
| `event_type` | Required/non-null | string | Event action; quality currently permits `order_created` |
| `event_timestamp` | Required/non-null | ISO timestamp string | Business/event time |
| `order_id` | Required/non-null | UUID4 string | Order/business identity |
| `customer_id` | Optional | `CUS-####` string | Customer identifier |
| `product_id` | Required/non-null | `PRD-###` string | Product identifier |
| `category` | Required/non-null | string | Product category |
| `quantity` | Required/non-null | integer | Units ordered |
| `unit_price` | Required/non-null | number | Price per unit |
| `currency` | Required/non-null | string | Currency code |

`customer_id` is intentionally optional in the current V1 contract.

Unknown extra fields are accepted by the canonical contract validator. The Spark typed order struct selects known analytical fields while the raw Bronze payload remains available for lineage/debugging.

## 3. Structural contract rules

Canonical validation in `spark/common/order_contract.py` checks:

1. payload is a dictionary/object;
2. `schema_version` exists;
3. schema version is supported;
4. every V1 required field exists and is not `null`.

Spark additionally distinguishes a present but unparseable schema version.

### Contract error codes

| Error | Meaning |
|---|---|
| `contract_invalid_payload` | Payload cannot be represented as the expected JSON object |
| `contract_missing_schema_version` | Source payload omits `schema_version` |
| `contract_invalid_schema_version` | Spark cannot parse the supplied version into the expected integer field |
| `contract_unsupported_schema_version` | Version parses but is not supported |
| `contract_missing_event_id` | Required field missing/null |
| `contract_missing_event_type` | Required field missing/null |
| `contract_missing_event_timestamp` | Required field missing/null |
| `contract_missing_order_id` | Required field missing/null |
| `contract_missing_product_id` | Required field missing/null |
| `contract_missing_category` | Required field missing/null |
| `contract_missing_quantity` | Required field missing/null |
| `contract_missing_unit_price` | Required field missing/null |
| `contract_missing_currency` | Required field missing/null |

## 4. Domain data-quality rules

Canonical Python rules live in `spark/common/order_quality.py`; equivalent Spark expressions are used in `stream_orders_to_lake.py`.

| Rule | Error code | Current acceptance condition |
|---|---|---|
| Event ID required | `missing_or_invalid_event_id` | non-empty string |
| Order ID required | `missing_order_id` | non-empty string |
| Product ID required | `missing_product_id` | non-empty string |
| Event type supported | `unsupported_event_type` | `order_created` |
| Event timestamp valid | `invalid_event_timestamp` | parseable timestamp |
| Quantity positive | `invalid_quantity` | integer > 0 |
| Unit price positive | `invalid_unit_price` | numeric > 0 |
| Currency supported | `unsupported_currency` | `GBP` |
| Category supported | `unsupported_category` | one of `electronics`, `home`, `fashion`, `sports`, `books` |

The parity utility `spark/tools/check_order_quality_parity.py` checks representative inputs against both the canonical Python validator and Spark validation expression.

## 5. Validation order

```text
raw payload
  ↓
contract validation
  ├─ fail ─────────────► Quarantine (contract_error)
  │
  └─ pass
       ↓
    quality validation
       ├─ fail ────────► Quarantine (validation_error)
       │
       └─ pass ────────► Silver
```

When a contract error is present, `validation_error` is intentionally left `NULL`; the structural contract failure is the primary rejection reason.

## 6. Derived Silver fields

The contract describes the incoming event. The following are derived downstream and therefore are not producer contract fields:

| Field | Derivation |
|---|---|
| `order_value` | `round(quantity * unit_price, 2)` |
| `event_date` | date of parsed `event_timestamp` |
| `ingestion_date` | date of `ingested_at` |
| `ingestion_hour` | hour of `ingested_at` |
| Kafka lineage fields | broker metadata supplied by Spark Kafka source |

## 7. Identity rules

### `event_id`

The producer generates UUID4 strings, but the current quality validator enforces only a non-empty string. At the warehouse it is stored as text and is the primary key / exactly-once logical event boundary.

### `order_id`

Represents business order identity. It is intentionally independent from `event_id`.

A repeated delivery with the same `event_id` is allowed physically and deduplicated logically. Reusing the same `order_id` across different `event_id` values is treated as a business-key collision by dbt test `assert_unique_order_business_keys.sql`.

## 8. Quarantine contract

Rejected records are written with enough information to inspect and repair them:

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

`warehouse/tools/reprocess_quarantine.py` can repair a specific quarantined event, re-run contract/quality validation, audit the attempt and optionally republish it.

## 9. Schema-evolution note

Historical project data includes a pre-V1 period from before `schema_version` was introduced. Re-running **today's** V1 contract against all historical Bronze does not reproduce the original historical Silver exactly because events valid under the legacy rules would now fail `contract_missing_schema_version`.

Therefore full historical Bronze replay across contract changes requires version-aware transformation or an explicit migration boundary. This is documented as a known replay boundary rather than hidden by a timestamp hack.
