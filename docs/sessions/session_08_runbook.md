# RetailPulse — Session 08 Reproduction Runbook

**Session:** 08  
**Focus:** CI/CD and automated quality checks  
**Goal:** Add a GitHub Actions workflow that automatically validates Python code quality, unit tests, dbt project parsing, and Docker Compose configuration on every push or pull request to `main`.

## 1. Local baseline

Run from the project root:

```cmd
pytest -v
ruff check .
docker compose config --quiet
```

Expected:

```text
pytest → all project tests pass
ruff check . → All checks passed!
docker compose config --quiet → no output / exit code 0
```

## 2. Make `warehouse` importable

Create:

```text
warehouse/
├── __init__.py
├── loader/
│   ├── __init__.py
│   └── load_orders.py
└── tests/
    ├── __init__.py
    └── test_load_orders.py
```

Commands:

```cmd
type nul > warehouse\__init__.py
type nul > warehouse\loader\__init__.py
type nul > warehouse\tests\__init__.py
```

## 3. Add loader unit tests

Create:

```text
warehouse/tests/test_load_orders.py
```

Use:

```python
from datetime import date

from warehouse.loader import load_orders


def test_discover_partitions_returns_empty_when_root_missing(
    tmp_path,
    monkeypatch,
):
    silver_root = tmp_path / "silver" / "orders"

    monkeypatch.setattr(
        load_orders,
        "SILVER_ROOT",
        silver_root,
    )

    result = load_orders.discover_partitions(None)

    assert result == []


def test_discover_partitions_respects_watermark(
    tmp_path,
    monkeypatch,
):
    silver_root = tmp_path / "silver" / "orders"

    partitions = [
        ("2026-08-12", 19),
        ("2026-08-12", 20),
        ("2026-08-12", 21),
        ("2026-08-13", 0),
    ]

    for partition_date, partition_hour in partitions:
        (
            silver_root
            / f"ingestion_date={partition_date}"
            / f"ingestion_hour={partition_hour:02d}"
        ).mkdir(parents=True)

    monkeypatch.setattr(
        load_orders,
        "SILVER_ROOT",
        silver_root,
    )

    result = load_orders.discover_partitions(
        (date(2026, 8, 12), 20)
    )

    discovered = [
        (partition_date, partition_hour)
        for partition_date, partition_hour, _ in result
    ]

    assert discovered == [
        (date(2026, 8, 12), 20),
        (date(2026, 8, 12), 21),
        (date(2026, 8, 13), 0),
    ]


def test_discover_files_only_returns_parquet_files(
    tmp_path,
):
    partition = tmp_path / "ingestion_hour=20"
    partition.mkdir()

    (partition / "part-00002.parquet").touch()
    (partition / "part-00001.parquet").touch()
    (partition / "_SUCCESS").touch()
    (partition / "metadata.txt").touch()

    result = load_orders.discover_files_in_partition(
        partition
    )

    assert [path.name for path in result] == [
        "part-00001.parquet",
        "part-00002.parquet",
    ]
```

## 4. Validate the tests

Run:

```cmd
pytest warehouse\tests -v
```

Expected:

```text
collected 3 items
3 passed
```

Then run the whole repository suite:

```cmd
pytest -v
```

## 5. Fix Ruff issues before CI

Run:

```cmd
ruff check .
```

If import-order issues are reported:

```cmd
ruff check . --fix
```

Then re-run:

```cmd
ruff check .
```

Expected:

```text
All checks passed!
```

## 6. Validate Docker Compose

Run:

```cmd
docker compose config --quiet
```

This validates the Compose configuration without starting containers.

## 7. Create the GitHub Actions workflow

Create:

```text
.github/workflows/ci.yml
```

Commands:

```cmd
mkdir .github
mkdir .github\workflows
notepad .github\workflows\ci.yml
```

Use:

```yaml
name: CI

on:
  push:
    branches:
      - main

  pull_request:
    branches:
      - main

jobs:
  quality:
    name: Quality checks
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repository
        uses: actions/checkout@v5

      - name: Set up Python
        uses: actions/setup-python@v6
        with:
          python-version: "3.10"
          cache: pip
          cache-dependency-path: requirements-dev.txt

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements-dev.txt

      - name: Ruff
        run: ruff check .

      - name: Pytest
        run: pytest -v

      - name: Create dbt CI profile
        run: |
          mkdir -p ~/.dbt

          cat > ~/.dbt/profiles.yml <<'EOF'
          retailpulse:
            target: ci

            outputs:
              ci:
                type: postgres
                host: localhost
                user: retailpulse
                password: retailpulse
                port: 5432
                dbname: retailpulse
                schema: analytics
                threads: 4
          EOF

      - name: dbt parse
        working-directory: warehouse/dbt/retailpulse
        run: dbt parse --no-partial-parse --target ci

      - name: Validate Docker Compose
        run: docker compose config --quiet
```

## 8. What CI validates

```text
Git push / pull request
        ↓
checkout repository
        ↓
Python 3.10
        ↓
install requirements-dev.txt
        ↓
Ruff
        ↓
pytest
        ↓
dbt parse
        ↓
docker compose config
        ↓
PASS / FAIL
```

Not run in CI yet:

```text
PostgreSQL
Kafka
Spark
Airflow
warehouse loader
dbt build against a live database
```

## 9. Final local validation before push

Run:

```cmd
pytest -v
ruff check .
docker compose config --quiet
```

All must pass.

## 10. Commit and push

```cmd
git status
git add .
git commit -m "Add CI quality checks and warehouse tests"
git push origin main
```

## 11. Verify GitHub Actions

Open the repository on GitHub → **Actions**.

Expected workflow:

```text
CI
└── Quality checks
    ├── Checkout repository
    ├── Set up Python
    ├── Install dependencies
    ├── Ruff
    ├── Pytest
    ├── Create dbt CI profile
    ├── dbt parse
    └── Validate Docker Compose
```

Expected result:

```text
green tick
```

## 12. Intentional CI failure test

Temporarily add to:

```text
warehouse/tests/test_load_orders.py
```

```python
import math
```

Commit and push:

```cmd
git add warehouse\tests\test_load_orders.py
git commit -m "Test CI failure detection"
git push origin main
```

Expected:

```text
Ruff → FAIL
CI → red
```

## 13. Restore CI to green

Remove:

```python
import math
```

Run locally:

```cmd
ruff check .
pytest -v
```

Commit and push:

```cmd
git add warehouse\tests\test_load_orders.py
git commit -m "Restore CI after failure test"
git push origin main
```

Expected:

```text
CI → green
```

This proves:

```text
valid code → CI green
intentional quality defect → CI red
fixed code → CI green again
```

## 14. Session 08 architecture impact

Runtime data flow remains unchanged:

```text
Producer
→ Kafka
→ Spark
→ Bronze / Silver / Quarantine
→ incremental warehouse loader
→ PostgreSQL
→ dbt
→ Airflow
```

Session 08 adds an automated repository quality gate around it:

```text
Developer change
      ↓
Git commit / push
      ↓
GitHub Actions
      ↓
Ruff
pytest
dbt parse
Docker Compose validation
      ↓
green / red status
```

## Session 08 validation gate

```text
[x] warehouse is importable as a Python package
[x] 3 real loader tests exist and pass
[x] ruff check . passes
[x] docker compose config --quiet passes
[x] .github/workflows/ci.yml exists
[x] GitHub Actions runs automatically on push to main
[x] Ruff runs in CI
[x] pytest runs in CI
[x] dbt parse runs in CI
[x] Docker Compose validation runs in CI
[x] initial CI run is green
[x] intentional lint defect turns CI red
[x] fixing defect returns CI to green
```

**Session 08 status: Complete**
