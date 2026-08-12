# RetailPulse — Session 1 Wrap-Up

**Date:** 8 August 2026  
**Session:** 1 of 30  
**Focus:** Project foundation and local development environment

## Session Goal

Establish a clean, reproducible foundation for the RetailPulse data engineering portfolio project before introducing Kafka, Spark, Airflow, and dbt.

## What Was Completed

### 1. Project Repository

Created the main project directory:

```text
retailpulse-data-platform/
```

Initialised Git:

```cmd
git init
```

This allows all future changes to be version-controlled through clear, incremental commits.

### 2. Python Virtual Environment

Created a project-specific Python environment:

```cmd
python -m venv .venv
.venv\Scripts\activate
```

The virtual environment isolates RetailPulse Python dependencies from other projects and system-wide packages.

### 3. Initial Project Structure

Created the main folders required for the planned data platform:

```text
retailpulse-data-platform/
├── airflow/
│   └── dags/
├── data_lake/
│   ├── bronze/
│   ├── checkpoints/
│   ├── quarantine/
│   └── silver/
├── docs/
├── producer/
│   ├── src/
│   └── tests/
├── spark/
│   ├── common/
│   ├── jobs/
│   └── tests/
└── warehouse/
    ├── dbt/
    └── init/
```

### 4. Key Project Files

Created:

```text
README.md
docker-compose.yml
.env.example
.gitignore
requirements-dev.txt
```

#### `README.md`

Provides the public-facing overview of the project, including its purpose, architecture, planned technologies, and engineering goals.

#### `.gitignore`

Prevents generated, temporary, local, or sensitive files from being committed to Git.

Examples include:

```text
.venv/
__pycache__/
.env
airflow/logs/
warehouse/dbt/target/
data_lake/bronze/*
```

#### `.gitkeep`

Added placeholder files inside empty data-lake folders so Git can preserve the intended directory structure before runtime data exists.

#### `.env.example`

Reserved for documenting environment variables required by the platform without committing real secrets or machine-specific values.

#### `requirements-dev.txt`

Started with:

```text
pytest
ruff
```

- **pytest** will be used for automated Python testing.
- **ruff** will be used for linting and Python code-quality checks.

### 5. Docker Compose Foundation

Created the first Docker Compose configuration with PostgreSQL 17 as the initial infrastructure service.

Current platform state:

```text
RetailPulse
    |
Docker Compose
    |
PostgreSQL 17
```

PostgreSQL is exposed locally through:

```text
localhost:5432
```

A Docker health check verifies that the database is actually ready to accept connections.

### 6. Docker Desktop / Docker Engine

The first `docker compose up -d` attempt failed because the Docker engine was not running.

The issue was resolved by starting Docker Desktop, which provides the Linux Docker engine used by Docker containers on the Windows development machine.

Useful commands confirmed during the session:

```cmd
docker compose up -d
docker compose ps
docker compose exec postgres psql -U retailpulse -d retailpulse
```

PostgreSQL successfully reached:

```text
healthy
```

### 7. PostgreSQL Connection Verification

Connected directly to PostgreSQL inside the running container using:

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse
```

The database and user configuration were verified successfully.

### 8. Development Tooling

Installed the current development dependencies:

```cmd
pip install -r requirements-dev.txt
```

Verified:

```cmd
pytest --version
ruff --version
```

### 9. Initial Git Commit

Created the first project checkpoint:

```cmd
git add .
git commit -m "Initialise RetailPulse data platform"
```

This establishes a clean baseline before new data-engineering components are introduced.

## Key Concepts Learned

### Docker Image

A reusable package containing the software and runtime required to create a container.

Example:

```text
postgres:17
```

### Docker Container

A running instance of an image.

In this project:

```text
postgres:17 image
        ↓
retailpulse-postgres container
```

### Docker Compose

Defines and manages multiple related services using one configuration file.

Today it manages PostgreSQL. Later it will coordinate more of the local data platform.

### Port Mapping

The Compose configuration maps:

```text
Windows localhost:5432
        ↓
PostgreSQL container:5432
```

This allows applications outside the container to connect to PostgreSQL.

### Virtual Environment

A local isolated Python environment that keeps project dependencies separate from the system Python installation.

### Git Commit

A versioned checkpoint representing a meaningful state of the project.

## Current Architecture

```text
                  RetailPulse
                      |
              Project Foundation
                      |
        +-------------+-------------+
        |             |             |
       Git          Python        Docker
        |             |             |
   Versioning       .venv      Docker Compose
                                      |
                                PostgreSQL 17
                                   healthy
```

## Current Project Status

The project infrastructure foundation is complete.

There is intentionally no active data pipeline yet.

The next layer will introduce event-driven ingestion:

```text
Python Event Producer
        ↓
Apache Kafka
        ↓
Kafka Topic
```

## Session 2 Preview

Next session will focus on:

- Adding Apache Kafka to Docker Compose
- Adding a Kafka UI for local inspection
- Creating the first Kafka topic
- Building the initial Python retail event producer
- Publishing the first synthetic order events
- Verifying that events are flowing through Kafka

## Session 1 Completion Checklist

- [x] Git repository initialised
- [x] Python virtual environment created
- [x] Project directory structure created
- [x] README added
- [x] `.gitignore` added
- [x] `.env.example` added
- [x] Development requirements added
- [x] Docker Compose configured
- [x] PostgreSQL container started
- [x] PostgreSQL health check passed
- [x] PostgreSQL connectivity verified
- [x] pytest and Ruff installed
- [x] Initial Git commit created

**Session 1 status: Complete**

---

## Later Platform Validation Note — 12 August 2026

The foundation created in Session 1 remained valid through the later platform build.

Final persistence behaviour confirmed:

```text
docker compose down
→ containers/network removed
→ named volumes retained

docker compose up -d
→ services recreated
→ persistent state restored
```

Persistent application state now includes PostgreSQL data, Kafka broker data, and Airflow metadata, while the Spark lake/checkpoint state remains host-mounted under `data_lake/`.

Important operational rule:

```text
docker compose down
→ normal restart-safe operation

docker compose down -v
→ destructive for named-volume state
```

The original Session 1 Docker/image/container/volume foundation therefore remained the correct base for the completed platform.
