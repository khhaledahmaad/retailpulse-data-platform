# RetailPulse Data Platform

RetailPulse is a production-style real-time data engineering portfolio project
that simulates an e-commerce event processing and analytics platform.

## Architecture

The platform will use:

- Python for synthetic event generation
- Apache Kafka for event streaming
- Apache Spark Structured Streaming for stream processing
- Parquet for Bronze and Silver data-lake storage
- PostgreSQL as the analytical warehouse
- dbt for transformations, modelling and data-quality testing
- Apache Airflow for workflow orchestration
- Docker Compose for reproducible local infrastructure

## Planned Data Flow

Python Producer
→ Kafka
→ Spark Structured Streaming
→ Bronze / Silver / Quarantine
→ Airflow
→ PostgreSQL
→ dbt
→ Analytics

## Engineering Goals

The project will demonstrate:

- Event-driven data ingestion
- Stream processing
- Schema validation
- Deduplication
- Late-arriving data handling
- Invalid-record quarantine
- Incremental loading
- Idempotent pipeline execution
- Workflow retries and failure recovery
- Dimensional data modelling
- Automated data-quality testing
- Containerised development
- CI/CD practices

## Project Status

🚧 Under active development.

Current milestone: Platform foundation and Kafka ingestion.