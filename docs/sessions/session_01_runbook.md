# RetailPulse — Session 1 Reproduction Runbook

**Goal:** Create the repository, Python environment, project structure, PostgreSQL service and development tooling.

## 1. Create and enter the project

```cmd
mkdir retailpulse-data-platform
cd retailpulse-data-platform
git init
```

## 2. Create the Python environment

```cmd
python -m venv .venv
.venv\Scripts\activate
```

Verify:

```cmd
python --version
pip --version
```

## 3. Create the initial structure

Create these folders:

```text
airflow/dags
data_lake/bronze
data_lake/silver
data_lake/quarantine
data_lake/checkpoints
docs/sessions
producer/src
producer/tests
spark/common
spark/jobs
spark/tests
warehouse/dbt
warehouse/init
warehouse/loader
```

Core root files:

```text
README.md
.gitignore
.env.example
requirements-dev.txt
docker-compose.yml
```

Initial `requirements-dev.txt`:

```text
pytest
ruff
```

Install:

```cmd
pip install -r requirements-dev.txt
```

Verify:

```cmd
pytest --version
ruff --version
```

## 4. Add PostgreSQL to Docker Compose

Use PostgreSQL 17 with:

```text
database: retailpulse
user: retailpulse
password: retailpulse
host port: 5432
named volume: postgres_data
```

Start Docker Desktop, then:

```cmd
docker compose config
docker compose up -d
docker compose ps
```

Expected PostgreSQL state:

```text
healthy
```

## 5. Validate PostgreSQL

```cmd
docker compose exec postgres psql -U retailpulse -d retailpulse
```

Inside `psql`:

```sql
SELECT current_database();
SELECT current_user;
```

Exit:

```text
\q
```

## 6. Validate persistence

```cmd
docker compose down
docker compose up -d
docker compose ps
```

PostgreSQL should still be healthy because `postgres_data` survives normal container recreation.

## 7. Git checkpoint

```cmd
git add .
git commit -m "Initialise RetailPulse data platform"
git status
```

## Session 1 validation gate

Do not move on until all pass:

```text
[ ] .venv activates
[ ] pytest and ruff work
[ ] docker compose config succeeds
[ ] PostgreSQL is healthy
[ ] psql connection succeeds
[ ] normal down/up preserves PostgreSQL state
[ ] initial Git commit exists
```
