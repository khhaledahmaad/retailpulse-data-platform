# Session 27 Runbook — Performance & Scale Testing

## Session goal

Measure how RetailPulse behaves under increasing event volume without changing the core architecture or adding dedicated load-testing/observability infrastructure.

The session focused on:

- producer throughput;
- Kafka/Spark burst handling;
- lake correctness under backlog;
- warehouse catch-up;
- Airflow task timing;
- resource observations;
- identifying the real scaling constraint;
- preserving exact reconciliation and existing delivery semantics.

No Prometheus, Grafana, JMeter, or additional services were introduced.

---

## Starting baseline

The stack started cleanly with the Session 26 readiness checks in place.

Initial strict pipeline state:

```text
Bronze rows:        3137
Silver rows:        3132
Silver unique:      3130
Silver duplicates:     2
Quarantine rows:       5
Raw orders:         3130
Fact orders:        3130
Gold order count:   3130

Status: HEALTHY
```

Initial Docker resource usage was modest relative to the approximately 7.7 GiB Docker memory limit.

---

# 27.1 — Producer throughput limitation identified

The original producer emitted continuously and contained:

```python
producer.send(...)
producer.flush()
time.sleep(2)
```

This was appropriate for interactive event generation but unsuitable for controlled scale testing.

At the original two-second interval:

```text
1,000 events  ≈ 33 minutes
10,000 events ≈ 5.5 hours
```

Flushing every event also prevented Kafka producer batching.

---

# 27.2 — Spark readiness probe cleanup

During Session 26 follow-up, Spark master logs contained repeated:

```text
got disassociated
```

messages approximately every ten seconds.

The cause was the TCP health probe opening and immediately closing a Spark RPC connection on port 7077.

The probe was changed to use the Spark master HTTP UI instead:

```yaml
healthcheck:
  test:
    [
      "CMD",
      "python3",
      "-c",
      "import urllib.request; urllib.request.urlopen('http://localhost:8080', timeout=5)"
    ]
  interval: 10s
  timeout: 5s
  retries: 5
  start_period: 10s
```

After recreation:

```cmd
docker compose logs --since=30s spark-master
```

showed no recurring disassociation noise.

This preserved readiness validation while avoiding artificial RPC log noise.

---

# 27.3 — Producer benchmark mode

The producer was extended with controlled CLI options while preserving its existing default behavior.

New usage:

```cmd
python -m producer.src.producer --count 1000 --interval 0 --quiet
```

Key options:

```text
--count
    finite number of events
    default remains continuous production

--interval
    seconds between events
    default remains 2 seconds

--quiet
    suppress per-event JSON output
```

The producer now flushes when the run finishes rather than after every event.

This allows KafkaProducer to batch/asynchronously send records during benchmark mode.

Default interactive behavior remains:

```text
continuous
2-second interval
prints each generated event
```

A test was added to verify finite production respects the requested count.

Validation:

```text
producer tests: 2 passed
Ruff: clean
```

The Kafka client emitted non-fatal deprecation warnings about lambda serializers during benchmark execution. These did not affect correctness or throughput and were left as a future cleanup rather than expanding Session 27 scope.

---

# 27.4 — Spark stream launch

The existing repository-aware Spark submission command was used:

```cmd
docker compose exec -d -e PYTHONPATH=/opt/retailpulse spark-master /opt/spark/bin/spark-submit ^
  --master spark://spark-master:7077 ^
  --conf spark.jars.ivy=/tmp/.ivy2 ^
  --conf spark.executorEnv.PYTHONPATH=/opt/retailpulse ^
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3 ^
  /opt/retailpulse/spark/jobs/stream_orders_to_lake.py
```

Registration was confirmed:

```text
Registering app RetailPulseOrderLakeStream
Registered app RetailPulseOrderLakeStream
```

The stream retained its checkpointed Bronze/Silver/Quarantine behavior.

---

# 27.5 — 1,000-event benchmark

## Producer

Command:

```cmd
python -m producer.src.producer --count 1000 --interval 0 --quiet
```

Result:

```text
Produced 1000 events in 0.71s
Throughput: 1410.4 events/s
```

The burst was produced before the Spark stream was launched.

This did not lose data because Kafka retained the records and Spark resumed from its existing checkpoint position.

## Spark/lake result

Pre-test logical state:

```text
Silver unique = 3130
```

After Spark consumed the backlog:

```text
Bronze rows:        4137
Silver rows:        4132
Silver unique:      4130
Silver duplicates:     2
Quarantine rows:       5
Raw orders:         3130
Fact orders:        3130
Gold order count:   3130
```

Observed backlog:

```text
Silver unique → Raw gap = 1000
Status = DEGRADED
```

This was expected and correct: the lake had caught up while the warehouse had not yet loaded the new records.

No event loss occurred.

No new quarantine records appeared.

No additional physical Silver duplicates appeared.

## Resource snapshot during catch-up

Observed approximately:

```text
Spark worker   ~274% CPU   ~775 MiB
Spark master    ~61% CPU   ~1.0 GiB
Kafka            ~7% CPU   ~512 MiB
```

This was an instantaneous Docker snapshot, not a guaranteed peak measurement.

## Warehouse catch-up

Manual run:

```text
session27_1k_catchup_001
```

Pipeline lineage:

```text
duration_seconds         81.00
loader_files_discovered  246
loader_rows_processed    1000
loader_rows_inserted     1000
loader_duplicates        0
dbt_status               SUCCEEDED
health_status            HEALTHY
raw_orders               4130
status                   SUCCEEDED
```

Important caution:

`loader_files_discovered=246` describes what the loader discovered for that run; it must not be interpreted as proof that the 1K burst itself created exactly 246 new Parquet files.

Final strict state:

```text
Silver unique = Raw = Fact = Gold = 4130
Status = HEALTHY
```

## Airflow task timing — 1K

```text
start_pipeline_run        0.34 s
run_incremental_loader    3.80 s
validate_raw_orders       0.40 s
run_dbt_build             7.92 s
check_pipeline_health    63.78 s
record_pipeline_metrics   0.34 s
complete_pipeline_run     0.33 s
```

Effective loader throughput:

```text
1000 / 3.80 ≈ 263 rows/s
```

The major runtime cost was clearly the post-load health check, not loading.

---

# 27.6 — Health-check profiling

The health checker was profiled before optimization.

Implementation observations:

```text
get_committed_files()
    parses Spark metadata logs

count_rows_from_files()
    opens every committed Parquet file
    reads Parquet row-count metadata

count_unique_values_from_files()
    opens every Silver Parquet file
    reads event_id
    calculates uniqueness in Python
```

First profile at approximately 4.1K logical events:

```text
bronze_files        1398
bronze discover      0.089 s
bronze row scan     21.277 s

silver_files        1418
silver discover      0.180 s
silver row scan     27.329 s
silver unique scan   1.206 s

quarantine_files     560
quarantine discover  0.038 s
quarantine row scan  1.064 s
```

The result corrected the initial suspicion:

```text
Silver unique calculation was NOT the bottleneck.
```

The dominant cost was opening roughly 1,400 tiny historical Parquet files to obtain row counts.

The existing Session 23 invariant was preserved:

```text
Silver row count and Silver unique count
must use the same committed-file snapshot.
```

No health-check optimization was introduced because:

- health validation runs after loading/transformation;
- the DAG runs every ten minutes;
- total run time remained around one minute to one-and-a-half minutes;
- correctness was more important than premature optimization.

The health task remains fatal when it detects a true DEGRADED state, but its runtime does not delay data already written by the loader/dbt steps.

---

# 27.7 — 5,000-event benchmark

Clean baseline:

```text
Silver unique = Raw = Fact = Gold = 4130
Status = HEALTHY
```

## Producer

```text
Produced 5000 events in 3.01s
Throughput: 1663.6 events/s
```

## Lake result

```text
Bronze rows:        9137
Silver rows:        9132
Silver unique:      9130
Silver duplicates:     2
Quarantine rows:       5
Raw/Fact/Gold:       4130
```

Expected backlog:

```text
Silver → Raw gap = 5000
Status = DEGRADED
```

Again:

```text
5000 / 5000 reached Silver
new quarantine = 0
new physical duplicates = 0
```

## Resource snapshot

Observed approximately:

```text
Kafka           ~244% CPU   ~1.0 GiB
Spark master     ~42% CPU   ~1.05 GiB
Spark worker     ~15% CPU   ~805 MiB
```

Kafka was hottest in that instantaneous sample.

The sample was taken after producer completion, so it is not treated as peak utilization.

## Scheduled catch-up discovery

The normal ten-minute Airflow run loaded the 5K backlog before the manual catch-up run began.

Real load run:

```text
scheduled__2026-08-22T12:10:00+00:00

loader_rows_processed = 5000
loader_rows_inserted  = 5000
loader_duplicates     = 0
status                = SUCCEEDED
```

The later manual run correctly processed zero additional rows.

This became an important production-style proof:

```text
the normal scheduled pipeline automatically absorbed the 5K backlog.
```

## Airflow task timing — real 5K run

```text
start_pipeline_run        0.51 s
run_incremental_loader    2.47 s
validate_raw_orders       0.33 s
run_dbt_build             7.09 s
check_pipeline_health    60.34 s
record_pipeline_metrics   0.43 s
complete_pipeline_run     0.27 s
```

Effective loader throughput:

```text
5000 / 2.47 ≈ 2024 rows/s
```

Final:

```text
Silver unique = Raw = Fact = Gold = 9130
Status = HEALTHY
```

---

# 27.8 — File-count scaling verification

After the 5K burst, the health profiler was repeated.

Before 5K:

```text
Bronze files = 1398
Silver files = 1418
```

After 5K:

```text
Bronze files = 1404
Silver files = 1424
```

Only six additional Bronze and six additional Silver files were present even though row volume increased by 5,000.

Profiler after approximately 9.1K logical events:

```text
bronze_files        1404
bronze row scan     19.382 s

silver_files        1424
silver row scan     19.165 s
silver unique scan   1.151 s

quarantine_files     562
quarantine row scan  0.937 s
```

This strongly supports:

```text
health-check runtime is driven much more by Parquet file count
than by row count at the current scale.
```

The lower second-run scan timings are treated as normal filesystem/cache/runtime variation, not a true performance improvement.

---

# 27.9 — 20,000-event benchmark

A final larger burst was used as the scale ceiling for this portfolio session.

No 50K/100K test was required because 20K was sufficient to demonstrate the scaling behavior and identify the limiting factor.

## Producer

```text
Produced 20000 events in 14.04s
Throughput: 1424.5 events/s
```

## Lake result

Before:

```text
Silver unique = 9130
```

After:

```text
Bronze rows:        29137
Silver rows:        29132
Silver unique:      29130
Silver duplicates:      2
Quarantine rows:        5
Raw/Fact/Gold:        9130
```

Increase:

```text
Silver unique +20000 exactly
```

No new quarantine rows.

No new physical duplicates.

Expected pre-warehouse state:

```text
Silver → Raw gap = 20000
Status = DEGRADED
```

## Resource snapshot

Observed approximately:

```text
Spark worker   ~949 MiB
Spark master   ~1.05 GiB
Kafka          ~978 MiB
```

No memory-exhaustion behavior was observed.

Again, `docker stats --no-stream` is an instantaneous snapshot and not a peak-resource profiler.

---

# 27.10 — Scheduled 20K warehouse catch-up

No manual catch-up was triggered.

The normal scheduled DAG handled the backlog:

```text
scheduled__2026-08-22T12:30:00+00:00
```

Pipeline lineage:

```text
loader_files_discovered = 15
loader_rows_processed   = 20000
loader_rows_inserted    = 20000
loader_duplicates       = 0
status                  = SUCCEEDED
```

Final strict health:

```text
Bronze rows:        29137
Silver rows:        29132
Silver unique:      29130
Silver duplicates:      2
Quarantine rows:        5
Raw orders:         29130
Fact orders:        29130
Gold order count:   29130

Status: HEALTHY
```

This proved:

```text
20,000-event burst
→ Kafka accepted all events
→ Spark processed all events
→ no additional quarantine
→ no additional physical duplicates
→ scheduled Airflow run picked up the backlog
→ all 20,000 rows inserted
→ dbt reconciled Fact and Gold
→ strict health returned HEALTHY
```

---

# 27.11 — Airflow task timing — 20K

```text
start_pipeline_run        0.71 s
run_incremental_loader    6.75 s
validate_raw_orders       0.71 s
run_dbt_build            12.77 s
check_pipeline_health    53.49 s
record_pipeline_metrics   0.31 s
complete_pipeline_run     0.28 s
```

Effective loader throughput:

```text
20000 / 6.75 ≈ 2963 rows/s
```

The entire scheduled pipeline run remained around 77 seconds.

---

# Benchmark summary

```text
Metric                    1K              5K              20K
----------------------------------------------------------------
Producer time             0.71 s          3.01 s          14.04 s
Producer throughput       1410/s          1664/s          1425/s

Events reaching Silver    1000/1000       5000/5000       20000/20000
Rows lost                 0               0               0
New quarantine rows       0               0               0
New physical duplicates   0               0               0

Loader time               3.80 s          2.47 s          6.75 s
Loader throughput         ~263/s          ~2024/s         ~2963/s

dbt build                 7.92 s          7.09 s          12.77 s
Health check              63.78 s         60.34 s         53.49 s

Final reconciliation      exact           exact           exact
Final pipeline health     HEALTHY         HEALTHY         HEALTHY
```

The 1K loader rate should not be interpreted as a fundamental low-volume throughput ceiling because that run encountered a very different discovered-file shape and fixed orchestration/file-open overhead.

---

# Main performance conclusions

## 1. Producer/Kafka path is stable at portfolio scale

Observed producer throughput remained approximately:

```text
1.4K–1.7K events/second
```

across 1K, 5K, and 20K bursts.

No producer-side event loss was detected.

---

## 2. Spark handled burst backlog correctly

Every valid generated event reached Silver.

Across all three benchmarks:

```text
new quarantine = 0
new physical duplicates = 0
```

Checkpoint-based recovery also correctly consumed the first 1K backlog after Spark started.

---

## 3. Warehouse loading scales well

The loader processed:

```text
1K   → 3.80 s
5K   → 2.47 s
20K  → 6.75 s
```

The 20K run achieved approximately:

```text
2963 inserted rows/second
```

The 20K backlog was absorbed in a single normal scheduled Airflow run.

---

## 4. dbt is not currently a bottleneck

Observed:

```text
1K   → 7.92 s
5K   → 7.09 s
20K  → 12.77 s
```

This remains small relative to the ten-minute scheduling interval.

---

## 5. Health validation is the dominant fixed runtime cost

Observed:

```text
1K   → 63.78 s
5K   → 60.34 s
20K  → 53.49 s
```

Profiling showed the main cost is not event uniqueness.

It is repeated opening of roughly 1,400 historical Bronze/Silver Parquet files to obtain row counts.

At the current scale:

```text
file fragmentation matters more than row volume.
```

---

## 6. No health optimization is currently necessary

The health check runs after data loading and dbt transformation.

Although it dominates DAG runtime, the full pipeline remains comfortably below the ten-minute schedule interval.

Therefore the correct engineering decision for now is:

```text
measure and document
rather than prematurely optimize.
```

Potential future optimization triggers:

- DAG runtime begins approaching the next scheduled run;
- historical Parquet file count grows substantially;
- health checks take several minutes;
- local filesystem scan becomes operationally disruptive.

Potential future remedies could include compaction or maintaining incremental metrics, but neither is justified yet.

---

# Final quality gate

Final validation:

```text
pytest
→ 71 passed

ruff check .
→ All checks passed

docker compose config -q
→ valid

python -m warehouse.monitoring.check_pipeline_health --strict
→ HEALTHY
```

Final logical state:

```text
Silver unique = 29130
Raw orders    = 29130
Fact orders   = 29130
Gold count    = 29130
```

Existing intentional physical state:

```text
Silver duplicates = 2
Quarantine rows   = 5
```

---

# Files changed

Before adding this runbook, the Session 27 working tree contained:

```text
M docker-compose.yml
M producer/src/producer.py
M producer/tests/test_producer.py
```

Session wrap-up adds:

```text
docs/sessions/session_27_runbook.md
```

---

# Session 27 outcome

**COMPLETE**

RetailPulse has now been validated under controlled 1K, 5K, and 20K event bursts.

The project demonstrated:

- approximately 1.4K–1.7K events/s producer throughput;
- lossless Kafka/Spark backlog handling;
- zero new quarantine records from valid benchmark traffic;
- zero new duplicate business insertions;
- scheduled automatic recovery of 5K and 20K warehouse backlogs;
- approximately 3K rows/s loader throughput in the 20K test;
- exact Silver → Raw → Fact → Gold reconciliation;
- strict HEALTHY state after every completed catch-up;
- no observed memory pressure at the tested scale;
- identification of historical tiny-Parquet-file scanning as the main current fixed-cost bottleneck.

The important architectural conclusion is that RetailPulse scales comfortably to the tested portfolio workload without requiring additional infrastructure or premature performance tuning.
