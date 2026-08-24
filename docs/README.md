# RetailPulse Documentation

This directory contains the stable v1 documentation and the chronological engineering history.

## Stable documentation

### Project overview

- [Implementation Overview](architecture/IMPLEMENTATION_OVERVIEW.md) — the complete 30-session build journey, major outcomes and direct links to the historical runbooks.

### Architecture

- [Architecture](architecture/ARCHITECTURE.md) — system boundaries, components, reliability model and deployment topology.
- [Data Flow](architecture/DATA_FLOW.md) — one-event walkthrough, validation branches and warehouse flow.
- [Repository Structure](architecture/REPOSITORY_STRUCTURE.md) — implementation map from architectural capability to source path.
- `architecture/diagrams/` — editable Draw.io sources and SVG exports.

### Data

- [Data Contract](data/DATA_CONTRACT.md) — schema version, required fields, error codes and quality rules.
- [Data Catalogue](data/DATA_CATALOGUE.md) — lake datasets, warehouse models and control tables.
- [Business Glossary](data/BUSINESS_GLOSSARY.md) — shared terminology and business/engineering definitions.

### Operations

- [Build and Start](operations/BUILD_AND_START.md) — reproduce a runnable platform from a clean clone.
- [Operations Runbook](operations/OPERATIONS_RUNBOOK.md) — normal operation, health, replay, remediation and shutdown.
- [Disaster Recovery](operations/DISASTER_RECOVERY.md) — proven analytical recovery path and deeper recovery boundaries.
- [End-to-End Validation](operations/END_TO_END_VALIDATION.md) — concise validation checklist.

### Handover and reuse

- [Handover](handover/HANDOVER.md) — ownership, operational model, caveats and support checklist.
- [New Data Source Template](handover/NEW_DATA_SOURCE_TEMPLATE.md) — reusable skeleton for adapting RetailPulse to a new event domain such as decoded telemetry.

## Engineering history

`docs/sessions/` contains chronological runbooks for the build. These are historical records; stable operating instructions live in `docs/operations/`.

When a historical session conflicts with a stable v1 document, treat the stable v1 document and current source code as authoritative.
