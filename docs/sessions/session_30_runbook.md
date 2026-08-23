# Session 30 Runbook — Production Readiness & Project Completion

**Status:** Active checkpoint — documentation/release gate remains to be committed/tagged after review.

## Objective

Finish RetailPulse v1 by proving cold startup, end-to-end correctness, burst-scale convergence and operator usability, then replace the minimal project README with stable architecture/data/operations/handover documentation.

## 1. Cold PC/Docker startup issue

A real Windows/Docker reboot showed the first `docker compose up -d` could fail even though a second invocation succeeded.

Investigation proved PostgreSQL was not slow: both warehouse and Airflow Postgres completed crash recovery and accepted connections in under one second.

Container timestamps showed Airflow API/scheduler/DAG processor had been started normally before the databases, despite the previous Session 29 `restart: on-failure` adjustment. The final local lifecycle change therefore made dependent Airflow services explicit Compose-start services (`restart: "no"`) so dependency ordering is controlled by `docker compose up -d`.

After a full PC restart, the first startup succeeded and `docker compose ps` showed all critical services running/healthy.

**Session 29 `on-failure` advice is superseded by this real cold-reboot proof.**

## 2. Baseline after reboot

Containers were healthy/running and recent scheduled Airflow runs were `SUCCEEDED / HEALTHY`.

Strict health before new production-readiness events:

```text
Bronze rows        29137
Silver rows        29132
Silver unique      29130
Silver duplicates      2
Quarantine rows        5
Raw                29130
Fact               29130
Gold               29130
Status             HEALTHY
```

The Spark cluster containers were running but the application was not, confirming the documented operational requirement to start `stream_orders_to_lake.py` separately after a reboot.

## 3. Spark application restart

The canonical detached Spark submit command was run and verified via `ps` inside `spark-master`.

## 4. 20-event smoke test

Produced 20 valid events and allowed Airflow to process them.

Final strict health:

```text
Bronze             29157
Silver             29152
Silver unique      29150
Silver duplicates      2
Quarantine             5
Raw                29150
Fact               29150
Gold               29150
Status             HEALTHY
```

The exact +20 logical result proved end-to-end correctness after the cold reboot/startup fix.

## 5. 1,000,000-event burst test

Producer command:

```cmd
python -m producer.src.producer --count 1000000 --interval 0 --quiet
```

Producer result:

```text
Produced 1000000 events in 686.04s (1457.6 events/s)
```

Known non-fatal `kafka-python` serializer deprecation warnings were emitted.

## 6. Automatic multi-run catch-up

First scheduled catch-up run:

```text
Files discovered     123
Files skipped          3
Files loaded          120
Rows processed     730822
Rows inserted      730822
Duplicates               0
```

The run later failed strict health because Silver had completed the burst while Raw/Fact/Gold were only at 759,972. The health execution still persisted a `DEGRADED` metric snapshot.

`control.pipeline_runs` recorded:

```text
22:40 scheduled run
status                 FAILED
loader_rows_inserted   730822
```

`control.pipeline_metrics` recorded:

```text
Silver unique   1029150
Raw              759972
Fact             759972
Gold             759972
Status           DEGRADED
```

The already committed loader work remained durable.

Second scheduled run:

```text
loader_rows_inserted   269178
status                 SUCCEEDED
dbt_status             SUCCEEDED
health_status          HEALTHY
```

The two runs inserted exactly:

```text
730822 + 269178 = 1000000
```

No manual replay or repair was required.

## 7. Final 1M+ state

Strict health after catch-up:

```text
Bronze rows        1029157
Silver rows        1029152
Silver unique      1029150
Silver duplicates        2
Quarantine rows          5
Raw                 1029150
Fact                1029150
Gold                1029150
Status              HEALTHY
```

Timed strict health:

```text
7.88 seconds
```

The local operations dashboard also loaded quickly at the 1M+ state.

## 8. Documentation/handover build

Stable v1 documentation was created under:

```text
docs/architecture/
docs/data/
docs/operations/
docs/handover/
```

The project README was rewritten as a v1 front door and the stale session-era end-to-end validation file was converted to a redirect to the stable operations checklist.

The new-source handover template explicitly maps which files are reusable vs domain-specific and includes a decoded-telemetry adaptation example.

## 9. Remaining release closure after applying/reviewing this documentation

Run the final repository gate:

```cmd
ruff check .
pytest -v
python -m dotenv run -- dbt build --project-dir warehouse\dbt\retailpulse --profiles-dir warehouse\dbt\retailpulse --target dev
docker compose config --quiet
python -m warehouse.monitoring.check_pipeline_health --strict
```

Then review `git status`, commit Session 30, and tag `v1.0.0` if all gates remain green.
