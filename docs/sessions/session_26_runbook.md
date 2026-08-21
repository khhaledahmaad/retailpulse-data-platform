# Session 26 Runbook — Container Health Checks & Service Readiness

## Session goal

Improve RetailPulse startup reliability by distinguishing **container started** from **service ready**.

The objective was to make dependent services wait for real readiness where it materially matters, without adding extra tooling or overengineering the Compose stack.

## Baseline

Before Session 26:

- PostgreSQL warehouse already had a health check.
- Airflow metadata PostgreSQL already had a health check.
- `kafka-init` used `service_completed_successfully`.
- Kafka itself had no health check.
- Kafka UI only waited for Kafka to start.
- Spark master had no health check.
- Spark worker only waited for Spark master to start.
- Airflow API server had no health check.
- Airflow scheduler and DAG processor only waited for the API server to start.

This meant Docker Compose could start dependants before their upstream service was actually ready to accept requests.

## 26.1 — Prove real readiness probes manually

### Kafka

Validated Kafka using its own broker API utility:

```cmd
docker compose exec kafka sh -c "ls /opt/kafka/bin/kafka-broker-api-versions.sh && /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 > /dev/null && echo KAFKA_READY"
```

Observed:

```text
KAFKA_READY
```

This proved the broker was accepting Kafka protocol requests.

### Spark master

An initial loopback probe failed with:

```text
ConnectionRefusedError: [Errno 111] Connection refused
```

The issue was not Spark health. Spark was listening on its Docker network address rather than `localhost`.

Validated the correct Docker-network probe:

```cmd
docker compose exec spark-master python3 -c "import socket; print('hostname=', socket.gethostname()); print('spark-master=', socket.gethostbyname('spark-master')); s=socket.create_connection(('spark-master',7077),3); s.close(); print('SPARK_MASTER_READY')"
```

Observed:

```text
SPARK_MASTER_READY
```

Spark logs also confirmed:

```text
Successfully started service 'sparkMaster' on port 7077
Starting Spark master at spark://<docker-ip>:7077
I have been elected leader! New state: ALIVE
Registering worker ...
```

This established that the correct readiness target is `spark-master:7077` inside the Compose network.

### Airflow API server

Validated Airflow 3 health endpoint:

```cmd
docker compose exec airflow-api-server python -c "import urllib.request; r=urllib.request.urlopen('http://localhost:8080/api/v2/monitor/health', timeout=5); print(r.status); print(r.read().decode())"
```

Observed HTTP:

```text
200
```

The response reported healthy metadatabase, scheduler, and DAG processor state at the time of the check.

For Docker dependency ordering, the readiness probe intentionally treats a successful HTTP response as proof that the API server is available. It does not parse scheduler health, avoiding a circular dependency where the scheduler waits for an API server health check that itself waits for the scheduler.

## 26.2 — Kafka health check

Added to the `kafka` service:

```yaml
healthcheck:
  test:
    [
      "CMD-SHELL",
      "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 > /dev/null 2>&1"
    ]
  interval: 10s
  timeout: 5s
  retries: 5
  start_period: 10s
```

Changed Kafka UI dependency from container-start semantics to real readiness:

```yaml
depends_on:
  kafka:
    condition: service_healthy
```

Resulting chain:

```text
kafka-init completes
→ Kafka starts
→ Kafka becomes healthy
→ Kafka UI starts
```

## 26.3 — Spark master health check

Added to `spark-master`:

```yaml
healthcheck:
  test:
    [
      "CMD",
      "python3",
      "-c",
      "import socket; s=socket.create_connection(('spark-master',7077),3); s.close()"
    ]
  interval: 10s
  timeout: 5s
  retries: 5
  start_period: 10s
```

Changed Spark worker dependency to:

```yaml
depends_on:
  spark-master:
    condition: service_healthy
```

Resulting chain:

```text
Spark master starts
→ RPC endpoint accepts TCP connections on 7077
→ Spark master becomes healthy
→ Spark worker starts
```

No additional packages such as `curl` or `netcat` were introduced.

## 26.4 — Airflow API server health check

Added to `airflow-api-server`:

```yaml
healthcheck:
  test:
    [
      "CMD",
      "python",
      "-c",
      "import urllib.request; urllib.request.urlopen('http://localhost:8080/api/v2/monitor/health', timeout=5)"
    ]
  interval: 10s
  timeout: 5s
  retries: 5
  start_period: 20s
```

Both the scheduler and DAG processor now wait for API readiness:

```yaml
airflow-api-server:
  condition: service_healthy
```

The existing PostgreSQL readiness dependencies remain:

```text
airflow-db → service_healthy
postgres   → service_healthy
```

Resulting startup path:

```text
warehouse DB healthy
+
Airflow metadata DB healthy
→ Airflow API server starts
→ Airflow API health endpoint responds successfully
→ scheduler + DAG processor start
```

## 26.5 — Environment contract cleanup

During the Compose review, non-secret database identity variables were also made fail-fast because they are required configuration:

```text
POSTGRES_DB
POSTGRES_USER
AIRFLOW_DB_NAME
AIRFLOW_DB_USER
```

These now use Docker Compose required-variable syntax in the relevant configuration paths, consistent with Session 25's environment hardening.

Examples:

```yaml
"${POSTGRES_DB:?POSTGRES_DB is required}"
"${POSTGRES_USER:?POSTGRES_USER is required}"
"${AIRFLOW_DB_NAME:?AIRFLOW_DB_NAME is required}"
"${AIRFLOW_DB_USER:?AIRFLOW_DB_USER is required}"
```

Required secret variables remain fail-fast as established in Session 25.

## 26.6 — Compose validation

Validated YAML and environment interpolation using:

```cmd
docker compose config -q
```

Result:

```text
success / no output
```

`docker compose config -q` is preferred over unredirected `docker compose config` because it validates configuration without printing interpolated environment values.

## 26.7 — Cold-start readiness proof

Performed a real container cold restart while preserving named volumes:

```cmd
docker compose down
docker compose up -d
```

No `--force-recreate` was needed because `docker compose down` had already removed the old containers.

The cold startup completed successfully with the new health-based dependency ordering.

Confirmed:

```text
postgres              → healthy
airflow-db            → healthy
kafka                 → healthy
spark-master          → healthy
airflow-api-server    → healthy

kafka-ui              → running
spark-worker           → running
airflow-scheduler      → running
airflow-dag-processor  → running
```

## 26.8 — Airflow validation after cold startup

Verified DAG imports:

```cmd
docker compose exec airflow-api-server airflow dags list-import-errors
```

Result:

```text
No data found
```

Triggered a real pipeline run:

```cmd
docker compose exec airflow-api-server airflow dags trigger ^
  -r session26_readiness_proof_001 ^
  retailpulse_warehouse_pipeline
```

Result:

```text
session26_readiness_proof_001 → success
```

This proved that the stack was not merely "healthy" according to Docker; Airflow could actually execute the RetailPulse warehouse pipeline after the readiness-controlled cold startup.

## 26.9 — Final quality gate

Validated:

```cmd
pytest
ruff check .
docker compose config -q
python -m warehouse.monitoring.check_pipeline_health --strict
```

All checks were green.

The pipeline remained strictly healthy after the readiness changes.

## Operational Docker Compose rule

Use the following operational model going forward.

### Normal start

```cmd
docker compose up -d
```

### `.env` or Compose configuration changed while the stack is already running

```cmd
docker compose up -d --force-recreate
```

This forces containers to be recreated with the new environment/configuration.

### Full container restart while preserving persisted data

```cmd
docker compose down
docker compose up -d
```

Because `down` removes the containers, `--force-recreate` is unnecessary on the following `up`.

### Avoid unless intentionally deleting persisted data

```cmd
docker compose down -v
```

`-v` removes Compose-managed named volumes, including persisted PostgreSQL and Kafka data.

## Final readiness dependency model

```text
kafka-init
   ↓ completed successfully
Kafka
   ↓ healthy
Kafka UI


Spark master
   ↓ healthy
Spark worker


RetailPulse PostgreSQL ── healthy ┐
                                  ├─→ Airflow API server
Airflow PostgreSQL ───── healthy ┘       ↓ healthy
                                  ┌──────┴──────┐
                                  ↓             ↓
                             Scheduler    DAG Processor
```

## Files changed

```text
docker-compose.yml
docs/sessions/session_26_runbook.md
```

## Session 26 outcome

**COMPLETE**

RetailPulse now distinguishes process startup from real service readiness for its key infrastructure dependencies.

The stack has:

- protocol-level Kafka readiness;
- network-level Spark master readiness;
- HTTP-level Airflow API readiness;
- health-driven dependency ordering;
- fail-fast required database identities;
- successful cold-start proof;
- successful post-start Airflow pipeline execution;
- passing tests, linting, Compose validation, and strict pipeline health.

Next planned session:

**Session 27 — Performance & Scale Testing**
