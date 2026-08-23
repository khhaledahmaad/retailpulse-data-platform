# RetailPulse Repository Structure

This is the implementation map used by the build, operations and handover documents. It intentionally lists operational modules and tool modules while summarizing test suites at directory level.

```text
retailpulse-data-platform/
├── .env.example                         committed config placeholder contract
├── .github/workflows/ci.yml             CI quality checks
├── .gitignore                           excludes runtime/secrets/generated data
├── docker-compose.yml                   local service topology + env wiring
├── README.md                            project front door
├── requirements-dev.txt                 local Python/dev dependencies
│
├── airflow/
│   ├── Dockerfile                       Airflow + psycopg/pyarrow/dbt image
│   └── dags/
│       └── retailpulse_warehouse_pipeline.py
│                                        10-minute warehouse DAG
│
├── producer/
│   ├── src/
│   │   └── producer.py                  synthetic source adapter / benchmark CLI
│   └── tests/                           producer tests
│
├── spark/
│   ├── common/
│   │   ├── order_contract.py            canonical event-contract validation
│   │   └── order_quality.py             canonical domain quality rules
│   ├── jobs/
│   │   ├── stream_orders.py             console/debug stream
│   │   └── stream_orders_to_lake.py     canonical Kafka→lake stream
│   ├── tools/
│   │   └── check_order_quality_parity.py Python↔Spark quality parity utility
│   └── tests/                           contract/quality tests
│
├── warehouse/
│   ├── init/
│   │   └── 001_create_warehouse.sql     raw/control schemas and tables
│   ├── loader/
│   │   └── load_orders.py               committed Silver→Raw incremental loader
│   ├── monitoring/
│   │   ├── check_pipeline_health.py     reconciliation/metrics/incidents
│   │   ├── config.py                    monitoring SLO config
│   │   ├── notifier.py                  SMTP alert/recovery notifications
│   │   ├── operations_dashboard.py      local HTML/SVG dashboard on :8084
│   │   └── operations_view.py           terminal operational view/query layer
│   ├── tools/
│   │   ├── repair_order_business_key.py targeted durable Silver repair
│   │   └── reprocess_quarantine.py      audited quarantine repair/republication
│   ├── dbt/
│   │   └── retailpulse/
│   │       ├── dbt_project.yml
│   │       ├── models/
│   │       │   ├── staging/
│   │       │   │   ├── sources.yml
│   │       │   │   ├── stg_orders.sql
│   │       │   │   └── stg_orders.yml
│   │       │   ├── facts/
│   │       │   │   ├── fct_orders.sql
│   │       │   │   └── fct_orders.yml
│   │       │   └── marts/
│   │       │       └── mart_daily_sales.sql
│   │       └── tests/                   dbt singular tests
│   └── tests/                           loader/monitoring/tool tests
│
├── data_lake/
│   ├── bronze/                          runtime data; ignored by Git
│   ├── silver/                          runtime data; ignored by Git
│   ├── quarantine/                      runtime data; ignored by Git
│   └── checkpoints/                     Spark streaming checkpoints
│
└── docs/
    ├── README.md
    ├── architecture/
    │   ├── ARCHITECTURE.md
    │   ├── DATA_FLOW.md
    │   ├── REPOSITORY_STRUCTURE.md
    │   └── diagrams/
    ├── data/
    │   ├── DATA_CONTRACT.md
    │   ├── DATA_CATALOGUE.md
    │   └── BUSINESS_GLOSSARY.md
    ├── operations/
    │   ├── BUILD_AND_START.md
    │   ├── OPERATIONS_RUNBOOK.md
    │   ├── DISASTER_RECOVERY.md
    │   └── END_TO_END_VALIDATION.md
    ├── handover/
    │   ├── HANDOVER.md
    │   └── NEW_DATA_SOURCE_TEMPLATE.md
    └── sessions/                         chronological engineering runbooks
```

## Runtime files intentionally not committed

- `.env`
- `warehouse/dbt/retailpulse/profiles.yml`
- `data_lake/**` runtime data except `.gitkeep`
- `airflow/logs/`
- `airflow/auth/`
- dbt `target/`, `logs/`, packages/cache outputs
- Python caches and virtual environments

## Canonical vs supporting modules

| Area | Canonical runtime path | Supporting/debug path |
|---|---|---|
| Producer | `producer/src/producer.py` | — |
| Spark lake stream | `spark/jobs/stream_orders_to_lake.py` | `spark/jobs/stream_orders.py` console stream |
| Contract | `spark/common/order_contract.py` | tests under `spark/tests/` |
| Quality | `spark/common/order_quality.py` + Spark expressions in lake job | `spark/tools/check_order_quality_parity.py` |
| Warehouse load | `warehouse/loader/load_orders.py` | historical/backfill/replay modes in same CLI |
| Health | `warehouse/monitoring/check_pipeline_health.py` | operations view/dashboard consume recorded state |
| Orchestration | `airflow/dags/retailpulse_warehouse_pipeline.py` | manual Airflow trigger for verification |
