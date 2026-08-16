import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote, urlparse

import psycopg
import pyarrow.parquet as pq
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]

load_dotenv(
    dotenv_path=PROJECT_ROOT / ".env",
    override=False,
)

BRONZE_ROOT = Path("data_lake/bronze/orders")
SILVER_ROOT = Path("data_lake/silver/orders")
QUARANTINE_ROOT = Path("data_lake/quarantine/orders")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "retailpulse")
POSTGRES_USER = os.getenv("POSTGRES_USER", "retailpulse")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "retailpulse")

MAX_LOAD_AGE_MINUTES = int(os.getenv("MAX_LOAD_AGE_MINUTES", "2880"))
MAX_LAG_ROWS = int(os.getenv("MAX_LAG_ROWS", "60"))


def get_connection():
    return psycopg.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def parquet_row_count(path: Path) -> int:
    return pq.ParquetFile(path).metadata.num_rows


def metadata_log_number(path: Path) -> int:
    name = path.name.removesuffix(".compact")
    return int(name)


def list_metadata_logs(metadata_root: Path):
    logs = []

    for path in metadata_root.iterdir():
        if path.name.startswith("."):
            continue

        name = path.name.removesuffix(".compact")

        if name.isdigit():
            logs.append(path)

    return logs


def read_metadata_entries(path: Path):
    entries = []

    with path.open(
        "r",
        encoding="utf-8",
        errors="replace",
    ) as handle:
        for line in handle:
            line = line.strip()

            if not line or line.startswith("v"):
                continue

            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue

            if isinstance(entry, dict):
                entries.append(entry)

    return entries


def spark_path_to_local_path(
    spark_path: str,
    root: Path,
) -> Path:
    parsed = urlparse(spark_path)
    decoded_path = unquote(parsed.path)

    marker = "/data_lake/"

    if marker in decoded_path:
        relative_path = decoded_path.split(
            marker,
            1,
        )[1]

        return Path("data_lake") / relative_path

    filename = Path(decoded_path).name
    matches = list(root.rglob(filename))

    if len(matches) == 1:
        return matches[0]

    raise RuntimeError("Unable to resolve Spark metadata path: " f"{spark_path}")


def get_committed_files(root: Path):
    metadata_root = root / "_spark_metadata"

    if not root.exists():
        return []

    if not metadata_root.exists():
        raise RuntimeError("Spark metadata directory is missing for " f"{root}")

    logs = list_metadata_logs(metadata_root)

    if not logs:
        return []

    compact_logs = [path for path in logs if path.name.endswith(".compact")]

    committed_files = set()
    last_processed_batch = -1

    if compact_logs:
        latest_compact = max(
            compact_logs,
            key=metadata_log_number,
        )

        last_processed_batch = metadata_log_number(latest_compact)

        for entry in read_metadata_entries(latest_compact):
            spark_path = entry.get("path")

            if not spark_path:
                continue

            local_path = spark_path_to_local_path(
                spark_path,
                root,
            )

            action = entry.get("action", "add")

            if action == "add":
                committed_files.add(local_path)

            elif action == "delete":
                committed_files.discard(local_path)

    later_logs = sorted(
        (
            path
            for path in logs
            if (
                not path.name.endswith(".compact")
                and metadata_log_number(path) > last_processed_batch
            )
        ),
        key=metadata_log_number,
    )

    for log_path in later_logs:
        for entry in read_metadata_entries(log_path):
            spark_path = entry.get("path")

            if not spark_path:
                continue

            local_path = spark_path_to_local_path(
                spark_path,
                root,
            )

            action = entry.get("action", "add")

            if action == "add":
                committed_files.add(local_path)

            elif action == "delete":
                committed_files.discard(local_path)

    return sorted(committed_files)


def count_spark_rows(root: Path) -> int:
    committed_files = get_committed_files(root)

    total_rows = 0

    for path in committed_files:
        if not path.exists():
            raise RuntimeError("Spark-committed Parquet file is " f"missing: {path}")

        total_rows += parquet_row_count(path)

    return total_rows


def count_spark_unique_values(
    root: Path,
    column: str,
) -> int:
    committed_files = get_committed_files(root)

    unique_values = set()

    for path in committed_files:
        if not path.exists():
            raise RuntimeError("Spark-committed Parquet file is " f"missing: {path}")

        table = pq.ParquetFile(path).read(columns=[column])

        for value in table.column(column).to_pylist():
            if value is not None:
                unique_values.add(value)

    return len(unique_values)


def collect_lake_metrics():
    bronze_rows = count_spark_rows(BRONZE_ROOT)

    silver_rows = count_spark_rows(SILVER_ROOT)

    silver_unique_events = count_spark_unique_values(
        SILVER_ROOT,
        "event_id",
    )

    quarantine_rows = count_spark_rows(QUARANTINE_ROOT)

    return {
        "bronze_rows": bronze_rows,
        "silver_rows": silver_rows,
        "silver_unique_events": (silver_unique_events),
        "silver_duplicate_deliveries": (silver_rows - silver_unique_events),
        "quarantine_rows": quarantine_rows,
    }


def collect_database_metrics(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                COUNT(*),
                MAX(loaded_at)
            FROM raw.orders
            """)

        raw_orders, latest_loaded_at = cur.fetchone()

        cur.execute("""
            SELECT COUNT(*)
            FROM analytics.fct_orders
            """)

        fact_orders = cur.fetchone()[0]

        cur.execute("""
            SELECT COALESCE(
                SUM(order_count),
                0
            )
            FROM analytics.mart_daily_sales
            """)

        gold_order_count = cur.fetchone()[0]

    return {
        "raw_orders": raw_orders,
        "fact_orders": fact_orders,
        "gold_order_count": gold_order_count,
        "latest_loaded_at": latest_loaded_at,
    }


def calculate_load_age_minutes(
    latest_loaded_at,
):
    if latest_loaded_at is None:
        return None

    now = datetime.now(timezone.utc)

    return int((now - latest_loaded_at).total_seconds() / 60)


def evaluate_health(
    bronze_rows,
    silver_rows,
    quarantine_rows,
    raw_orders,
    fact_orders,
    gold_order_count,
    latest_loaded_at,
    max_lag_rows=MAX_LAG_ROWS,
    strict=False,
    silver_unique_events=None,
):
    issues = []
    degraded = False

    if silver_unique_events is None:
        silver_unique_events = silver_rows

    bronze_expected = silver_rows + quarantine_rows
    bronze_gap = bronze_rows - bronze_expected

    if bronze_gap != 0:
        issues.append(
            "Bronze does not reconcile with "
            "Silver + Quarantine: "
            f"{bronze_rows} != "
            f"{silver_rows} + "
            f"{quarantine_rows} "
            f"(gap={bronze_gap})"
        )

        if strict or abs(bronze_gap) > max_lag_rows:
            degraded = True

    silver_raw_gap = silver_unique_events - raw_orders

    if silver_raw_gap != 0:
        issues.append(
            "Silver unique events do not reconcile with "
            "raw.orders: "
            f"{silver_unique_events} != "
            f"{raw_orders} "
            f"(gap={silver_raw_gap})"
        )

        if strict or silver_raw_gap < 0 or silver_raw_gap > max_lag_rows:
            degraded = True

    if raw_orders != fact_orders:
        issues.append(
            "raw.orders does not reconcile "
            "with fct_orders: "
            f"{raw_orders} != "
            f"{fact_orders}"
        )
        degraded = True

    if fact_orders != gold_order_count:
        issues.append(
            "fct_orders does not reconcile "
            "with Gold order count: "
            f"{fact_orders} != "
            f"{gold_order_count}"
        )
        degraded = True

    load_age_minutes = calculate_load_age_minutes(latest_loaded_at)

    if latest_loaded_at is None:
        issues.append("No warehouse load timestamp available")
        degraded = True

    elif load_age_minutes > MAX_LOAD_AGE_MINUTES:
        issues.append(
            "Warehouse data is stale: "
            f"{load_age_minutes} minutes "
            "since latest load"
        )
        degraded = True

    if degraded:
        status = "DEGRADED"
    elif issues:
        status = "WARNING"
    else:
        status = "HEALTHY"

    return {
        "status": status,
        "issues": issues,
        "load_age_minutes": load_age_minutes,
    }


def record_metrics(
    conn,
    lake,
    db,
    health,
):
    details = (
        "; ".join(health["issues"])
        if health["issues"]
        else ("All reconciliation and " "freshness checks passed")
    )

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO control.pipeline_metrics (
                bronze_rows,
                silver_rows,
                quarantine_rows,
                raw_orders,
                fact_orders,
                gold_order_count,
                latest_loaded_at,
                status,
                details
            )
            VALUES (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
            )
            """,
            (
                lake["bronze_rows"],
                lake["silver_rows"],
                lake["quarantine_rows"],
                db["raw_orders"],
                db["fact_orders"],
                db["gold_order_count"],
                db["latest_loaded_at"],
                health["status"],
                details,
            ),
        )

    conn.commit()


def print_report(
    lake,
    db,
    health,
):
    print()
    print("RetailPulse Pipeline Health")
    print("---------------------------")

    print(f"Bronze rows:       " f"{lake['bronze_rows']}")
    print(f"Silver rows:       " f"{lake['silver_rows']}")
    print(f"Silver unique:     " f"{lake['silver_unique_events']}")
    print(f"Silver duplicates: " f"{lake['silver_duplicate_deliveries']}")
    print(f"Quarantine rows:   " f"{lake['quarantine_rows']}")
    print(f"Raw orders:        " f"{db['raw_orders']}")
    print(f"Fact orders:       " f"{db['fact_orders']}")
    print(f"Gold order count:  " f"{db['gold_order_count']}")

    print(f"Latest load:       " f"{db['latest_loaded_at']}")

    if health["load_age_minutes"] is not None:
        print(f"Load age:          " f"{health['load_age_minutes']} " "minutes")

    print()
    print(f"Status: {health['status']}")

    if health["issues"]:
        print()
        print("Issues:")

        for issue in health["issues"]:
            print(f"- {issue}")

    print()


def parse_args():
    parser = argparse.ArgumentParser(description="Check RetailPulse pipeline health")

    parser.add_argument(
        "--strict",
        action="store_true",
        help=("Require exact cross-layer reconciliation " "with no live lag tolerance"),
    )

    parser.add_argument(
        "--max-lag-rows",
        type=int,
        default=MAX_LAG_ROWS,
        help=("Maximum tolerated live row lag " f"(default: {MAX_LAG_ROWS})"),
    )

    return parser.parse_args()


def main():
    args = parse_args()

    if args.max_lag_rows < 0:
        raise ValueError(
            "--max-lag-rows must be zero or greater"
        )

    lake = collect_lake_metrics()

    with get_connection() as conn:
        db = collect_database_metrics(conn)

        health = evaluate_health(
            bronze_rows=lake["bronze_rows"],
            silver_rows=lake["silver_rows"],
            silver_unique_events=lake["silver_unique_events"],
            quarantine_rows=lake["quarantine_rows"],
            raw_orders=db["raw_orders"],
            fact_orders=db["fact_orders"],
            gold_order_count=db["gold_order_count"],
            latest_loaded_at=db["latest_loaded_at"],
            max_lag_rows=args.max_lag_rows,
            strict=args.strict,
        )

        record_metrics(
            conn=conn,
            lake=lake,
            db=db,
            health=health,
        )

        print_report(
            lake=lake,
            db=db,
            health=health,
        )

        if health["status"] == "DEGRADED":
            raise SystemExit(1)


if __name__ == "__main__":
    main()
