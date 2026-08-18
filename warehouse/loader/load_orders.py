import argparse
import os
from datetime import date
from pathlib import Path

import psycopg
import pyarrow.parquet as pq

SILVER_ROOT = Path("data_lake/silver/orders")

DATASET_NAME = "silver_orders"

DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
DB_NAME = os.getenv("POSTGRES_DB", "retailpulse")
DB_USER = os.getenv("POSTGRES_USER", "retailpulse")
DB_PASSWORD = os.getenv(
    "POSTGRES_PASSWORD",
    "retailpulse",
)


INSERT_ORDER_SQL = """
INSERT INTO raw.orders (
    event_id,
    event_type,
    event_timestamp,
    event_date,
    order_id,
    customer_id,
    product_id,
    category,
    quantity,
    unit_price,
    order_value,
    currency,
    kafka_key,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    kafka_timestamp,
    ingested_at
)
VALUES (
    %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s
)
ON CONFLICT (event_id) DO NOTHING
"""


def get_watermark(
    conn: psycopg.Connection,
) -> tuple[date, int] | None:
    row = conn.execute(
        """
        SELECT
            watermark_date,
            watermark_hour
        FROM control.loader_watermarks
        WHERE dataset_name = %s
        """,
        (DATASET_NAME,),
    ).fetchone()

    if row is None:
        return None

    return row[0], row[1]


def discover_partitions(
    watermark: tuple[date, int] | None,
    start_partition: tuple[date, int] | None = None,
    end_partition: tuple[date, int] | None = None,
) -> list[tuple[date, int, Path]]:
    partitions = []

    if not SILVER_ROOT.exists():
        return partitions

    historical_mode = start_partition is not None or end_partition is not None

    for date_path in SILVER_ROOT.glob("ingestion_date=*"):
        if not date_path.is_dir():
            continue

        partition_date = date.fromisoformat(date_path.name.split("=", 1)[1])

        for hour_path in date_path.glob("ingestion_hour=*"):
            if not hour_path.is_dir():
                continue

            partition_hour = int(hour_path.name.split("=", 1)[1])

            partition = (
                partition_date,
                partition_hour,
            )

            if historical_mode:
                if start_partition is not None and partition < start_partition:
                    continue

                if end_partition is not None and partition > end_partition:
                    continue

            elif watermark is not None and partition < watermark:
                continue

            partitions.append(
                (
                    partition_date,
                    partition_hour,
                    hour_path,
                )
            )

    return sorted(
        partitions,
        key=lambda item: (
            item[0],
            item[1],
        ),
    )


def discover_files_in_partition(
    partition_path: Path,
) -> list[Path]:
    return sorted(partition_path.glob("*.parquet"))


def already_loaded(
    conn: psycopg.Connection,
    file_path: str,
) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM control.loaded_files
        WHERE file_path = %s
        """,
        (file_path,),
    ).fetchone()

    return row is not None


def should_skip_file(
    is_loaded: bool,
    replay: bool,
) -> bool:
    return is_loaded and not replay


def load_file(
    conn: psycopg.Connection,
    path: Path,
) -> tuple[int, int]:
    table = pq.ParquetFile(path).read()

    rows = table.to_pylist()

    values = [
        (
            row["event_id"],
            row["event_type"],
            row["event_timestamp"],
            row["event_date"],
            row["order_id"],
            row["customer_id"],
            row["product_id"],
            row["category"],
            row["quantity"],
            row["unit_price"],
            row["order_value"],
            row["currency"],
            row["kafka_key"],
            row["topic"],
            row["partition"],
            row["offset"],
            row["kafka_timestamp"],
            row["ingested_at"],
        )
        for row in rows
    ]

    processed_rows = len(values)
    inserted_rows = 0

    if values:
        with conn.cursor() as cur:
            cur.executemany(
                INSERT_ORDER_SQL,
                values,
                returning=False,
            )
            inserted_rows = cur.rowcount

    return processed_rows, inserted_rows


def register_file(
    conn: psycopg.Connection,
    file_path: str,
    row_count: int,
) -> None:
    conn.execute(
        """
        INSERT INTO control.loaded_files (
            file_path,
            dataset_name,
            row_count
        )
        VALUES (%s, %s, %s)
        ON CONFLICT (file_path) DO NOTHING
        """,
        (
            file_path,
            DATASET_NAME,
            row_count,
        ),
    )


def update_watermark(
    conn: psycopg.Connection,
    watermark_date: date,
    watermark_hour: int,
) -> None:
    conn.execute(
        """
        INSERT INTO control.loader_watermarks (
            dataset_name,
            watermark_date,
            watermark_hour,
            updated_at
        )
        VALUES (
            %s,
            %s,
            %s,
            NOW()
        )
        ON CONFLICT (dataset_name)
        DO UPDATE SET
            watermark_date = EXCLUDED.watermark_date,
            watermark_hour = EXCLUDED.watermark_hour,
            updated_at = NOW()
        """,
        (
            DATASET_NAME,
            watermark_date,
            watermark_hour,
        ),
    )


def should_advance_watermark(
    historical_mode: bool,
) -> bool:
    return not historical_mode


def parse_partition(
    value: str,
) -> tuple[date, int]:
    try:
        date_part, hour_part = value.split("T", 1)

        partition_date = date.fromisoformat(date_part)

        if len(hour_part) != 2 or not hour_part.isdigit():
            raise ValueError

        partition_hour = int(hour_part)

        if not 0 <= partition_hour <= 23:
            raise ValueError

    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "partition must use YYYY-MM-DDTHH format"
        ) from exc

    return partition_date, partition_hour


def parse_args():
    parser = argparse.ArgumentParser(
        description=("Load RetailPulse Silver orders " "into PostgreSQL")
    )

    parser.add_argument(
        "--from",
        dest="start_partition",
        type=parse_partition,
        help=("Historical range start " "in YYYY-MM-DDTHH format"),
    )

    parser.add_argument(
        "--to",
        dest="end_partition",
        type=parse_partition,
        help=("Historical range end " "in YYYY-MM-DDTHH format"),
    )

    parser.add_argument(
        "--replay",
        action="store_true",
        help=("Reread already-loaded files inside " "the historical range"),
    )

    parser.add_argument(
        "--airflow-run-id",
        help="Airflow DAG run_id used for pipeline-run lineage",
    )

    args = parser.parse_args()

    historical_mode = args.start_partition is not None or args.end_partition is not None

    if historical_mode and (args.start_partition is None or args.end_partition is None):
        parser.error("--from and --to must be supplied together")

    if (
        args.start_partition is not None
        and args.end_partition is not None
        and args.start_partition > args.end_partition
    ):
        parser.error("--from must be earlier than or equal to --to")

    if args.replay and not historical_mode:
        parser.error("--replay requires --from and --to")

    return args


def update_pipeline_run_loader_metrics(
    conn: psycopg.Connection,
    *,
    airflow_run_id: str,
    discovered_files: int,
    skipped_files: int,
    loaded_files: int,
    processed_rows: int,
    inserted_rows: int,
    duplicate_rows: int,
) -> None:
    conn.execute(
        """
        UPDATE control.pipeline_runs
        SET
            loader_files_discovered = %s,
            loader_files_skipped = %s,
            loader_files_loaded = %s,
            loader_rows_processed = %s,
            loader_rows_inserted = %s,
            loader_duplicates = %s
        WHERE airflow_run_id = %s
        """,
        (
            discovered_files,
            skipped_files,
            loaded_files,
            processed_rows,
            inserted_rows,
            duplicate_rows,
            airflow_run_id,
        ),
    )


def main() -> None:
    args = parse_args()

    historical_mode = (
        args.start_partition is not None
    )

    discovered_files = 0
    skipped_files = 0
    loaded_files = 0
    processed_rows = 0
    inserted_rows = 0
    duplicate_rows = 0

    with psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    ) as conn:
        watermark = get_watermark(conn)

        print(f"Current watermark: {watermark}")

        if args.replay:
            mode = "REPLAY"
        elif historical_mode:
            mode = "BACKFILL"
        else:
            mode = "NORMAL"

        print(f"Mode: {mode}")

        if historical_mode:
            print(
                "Historical range: "
                f"{args.start_partition} "
                "to "
                f"{args.end_partition}"
            )

        partitions = discover_partitions(
            watermark=watermark,
            start_partition=args.start_partition,
            end_partition=args.end_partition,
        )

        print(f"Eligible partitions: {len(partitions)}")

        if not partitions:
            print("No eligible Silver partitions.")

            if args.airflow_run_id is not None:
                update_pipeline_run_loader_metrics(
                    conn,
                    airflow_run_id=args.airflow_run_id,
                    discovered_files=0,
                    skipped_files=0,
                    loaded_files=0,
                    processed_rows=0,
                    inserted_rows=0,
                    duplicate_rows=0,
                )

            return

        for (
            partition_date,
            partition_hour,
            partition_path,
        ) in partitions:
            files = discover_files_in_partition(partition_path)

            print()
            print(
                "Scanning "
                f"{partition_date} "
                f"hour {partition_hour:02d}: "
                f"{len(files)} files"
            )

            discovered_files += len(files)

            for path in files:
                file_id = path.as_posix()

                is_loaded = already_loaded(
                    conn,
                    file_id,
                )

                if should_skip_file(
                    is_loaded=is_loaded,
                    replay=args.replay,
                ):
                    skipped_files += 1
                    print(
                        f"SKIPPED: {file_id} "
                        "(already loaded)"
                    )
                    continue

                with conn.transaction():
                    file_processed_rows, file_inserted_rows = load_file(
                        conn,
                        path,
                    )

                    register_file(
                        conn,
                        file_id,
                        file_processed_rows,
                    )

                file_duplicate_rows = (
                    file_processed_rows - file_inserted_rows
                )

                loaded_files += 1
                processed_rows += file_processed_rows
                inserted_rows += file_inserted_rows

                print(
                    f"LOADED: {file_id} "
                    f"(processed={file_processed_rows}, "
                    f"inserted={file_inserted_rows}, "
                    f"duplicates={file_duplicate_rows})"
                )

            if should_advance_watermark(
                historical_mode=historical_mode,
            ):
                with conn.transaction():
                    update_watermark(
                        conn,
                        partition_date,
                        partition_hour,
                    )

        duplicate_rows = processed_rows - inserted_rows

        if args.airflow_run_id is not None:
            update_pipeline_run_loader_metrics(
                conn,
                airflow_run_id=args.airflow_run_id,
                discovered_files=discovered_files,
                skipped_files=skipped_files,
                loaded_files=loaded_files,
                processed_rows=processed_rows,
                inserted_rows=inserted_rows,
                duplicate_rows=duplicate_rows,
            )

    print()
    print("Load complete.")
    print(f"Files discovered: {discovered_files}")
    print(f"Files skipped: {skipped_files}")
    print(f"Files loaded: {loaded_files}")
    print(f"Rows processed: {processed_rows}")
    print(f"Rows inserted: {inserted_rows}")
    print(f"Duplicate rows ignored: {duplicate_rows}")


if __name__ == "__main__":
    main()
