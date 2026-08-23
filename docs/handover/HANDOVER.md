# RetailPulse Handover

## 1. What is being handed over

RetailPulse v1 is a local, production-style streaming data-engineering reference architecture with:

```text
source adapter
→ Kafka
→ Spark contract/quality processing
→ Bronze / Silver / Quarantine
→ incremental PostgreSQL loader
→ dbt staging/fact/mart
→ Airflow orchestration
→ health / metrics / incidents / alerts / dashboard
```

The included order domain is an example implementation. The architecture is designed to be reused for other event sources.

## 2. Primary implementation ownership map

| Capability | Path |
|---|---|
| Docker topology | `docker-compose.yml` |
| Runtime config contract | `.env.example` |
| Source adapter | `producer/src/producer.py` |
| Contract | `spark/common/order_contract.py` |
| Quality rules | `spark/common/order_quality.py` |
| Canonical stream | `spark/jobs/stream_orders_to_lake.py` |
| Quality parity utility | `spark/tools/check_order_quality_parity.py` |
| Warehouse bootstrap | `warehouse/init/001_create_warehouse.sql` |
| Incremental loader | `warehouse/loader/load_orders.py` |
| dbt project | `warehouse/dbt/retailpulse/` |
| Airflow DAG | `airflow/dags/retailpulse_warehouse_pipeline.py` |
| Health | `warehouse/monitoring/check_pipeline_health.py` |
| Monitoring config | `warehouse/monitoring/config.py` |
| Email notifications | `warehouse/monitoring/notifier.py` |
| Terminal ops view | `warehouse/monitoring/operations_view.py` |
| Dashboard | `warehouse/monitoring/operations_dashboard.py` |
| Quarantine repair | `warehouse/tools/reprocess_quarantine.py` |
| Historical Silver repair | `warehouse/tools/repair_order_business_key.py` |
| CI | `.github/workflows/ci.yml` |
| Tests | `producer/tests/`, `spark/tests/`, `warehouse/tests/`, dbt `tests/` |

## 3. Normal operating model

After Docker/PC startup:

```cmd
docker compose up -d
```

Then verify/start the Spark application separately. Airflow schedules the warehouse side every 10 minutes.

Normal health:

```cmd
python -m warehouse.monitoring.check_pipeline_health
```

Release/recovery health:

```cmd
python -m warehouse.monitoring.check_pipeline_health --strict
```

Dashboard:

```cmd
python -m warehouse.monitoring.operations_dashboard
```

## 4. Persistence boundaries

| State | Location | Git tracked? | Meaning |
|---|---|---:|---|
| Code/config templates | repository | Yes | Reproducible implementation |
| Runtime secrets/config | `.env` | No | Local environment |
| Kafka retained history | Docker volume `kafka_data` | No | Upstream event history |
| Warehouse | Docker volume `postgres_data` | No | Raw/analytics/control state |
| Airflow metadata | Docker volume `airflow_db_data` | No | Airflow operational metadata |
| Lake/checkpoints | `data_lake/` | No (except `.gitkeep`) | Derived streaming state |
| dbt local profile | `warehouse/dbt/retailpulse/profiles.yml` | No | Environment-driven dbt connection |

## 5. Core invariants to preserve

Any future change should preserve or intentionally redefine these:

```text
Spark committed files are authoritative, not raw filesystem globbing.
Physical duplicate delivery is allowed.
Logical event duplication is not.
Silver unique event_id = Raw = Fact = Gold order count when stable.
Raw cannot legitimately be ahead of Silver unique.
Contract failures are distinguishable from quality failures.
Historical replay must not silently advance the live watermark.
Normal retry/recovery must be idempotent.
```

## 6. Known operational boundaries

### Spark application startup is manual

The Spark master/worker containers start with Compose, but the RetailPulse streaming application must be started separately after a reboot. This is documented explicitly rather than hidden behind an additional process supervisor.

### Schema-evolution replay

Historical Bronze contains a pre-V1 period. Blindly applying current V1 rules to every historical Bronze row will not recreate historical Silver exactly. Full historical replay across schema versions needs version-aware logic.

### Local reference topology

Kafka is a single broker and PostgreSQL is a single local instance. The project demonstrates engineering behaviour, not production HA topology.

### Alerting

SMTP/Mailtrap settings are environment-driven. Configure them before deliberately testing incident notification paths.

### Producer serializer warning

The current `kafka-python` lambda serializers can emit deprecation warnings. They are non-fatal and did not affect the 1M-event benchmark.

### `stream_orders.py`

This is a console/debug stream. New operators should use `stream_orders_to_lake.py` for the lake pipeline.

## 7. Release evidence

The v1 implementation has demonstrated:

- 72 Python tests passing at the final pre-documentation code gate;
- dbt build/test behaviour through Airflow and local validation;
- destructive Raw + analytics recovery from committed Silver;
- dbt-managed Fact index recreation after destructive rebuild;
- cold PC/Docker startup after lifecycle hardening;
- 20-event end-to-end smoke test;
- 1,000,000-event burst at 1,457.6 events/s;
- automatic two-run warehouse catch-up with a persisted DEGRADED snapshot and subsequent recovery;
- final Raw = Fact = Gold = 1,029,150;
- strict health at 1M+ in 7.88 seconds;
- responsive operations dashboard at 1M+.

## 8. Handover acceptance checklist

The receiving engineer should be able to:

- [ ] identify the canonical source, stream, loader, dbt and DAG files;
- [ ] create `.env` without committing it;
- [ ] create the ignored dbt profile;
- [ ] start Compose and the Spark application;
- [ ] produce a finite batch;
- [ ] find a successful Airflow run;
- [ ] run strict health;
- [ ] explain Bronze/Silver/Quarantine;
- [ ] explain duplicate delivery vs business-key collision;
- [ ] perform a bounded loader backfill/replay;
- [ ] dry-run a quarantine remediation;
- [ ] locate pipeline metrics/runs/incidents;
- [ ] explain the analytical DR path;
- [ ] use `NEW_DATA_SOURCE_TEMPLATE.md` to map a new domain.

## 9. Documentation hierarchy

For current behaviour, use:

```text
README.md
→ docs/architecture/
→ docs/data/
→ docs/operations/
→ docs/handover/
```

`docs/sessions/` is chronological engineering history. It is valuable for design rationale, but older runbooks may contain commands or assumptions later superseded by the stable v1 documentation.
