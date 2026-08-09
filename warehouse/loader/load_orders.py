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
) -> list[tuple[date, int, Path]]:
    partitions = []

    if not SILVER_ROOT.exists():
        return partitions

    for date_path in SILVER_ROOT.glob("ingestion_date=*"):
        if not date_path.is_dir():
            continue

        partition_date = date.fromisoformat(date_path.name.split("=", 1)[1])

        for hour_path in date_path.glob("ingestion_hour=*"):
            if not hour_path.is_dir():
                continue

            partition_hour = int(hour_path.name.split("=", 1)[1])

            if (
                watermark is not None
                and (
                    partition_date,
                    partition_hour,
                )
                < watermark
            ):
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


def load_file(
    conn: psycopg.Connection,
    path: Path,
) -> int:
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

    if values:
        with conn.cursor() as cur:
            cur.executemany(
                INSERT_ORDER_SQL,
                values,
            )

    return len(values)


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


def main() -> None:
    loaded_files = 0
    loaded_rows = 0

    with psycopg.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    ) as conn:
        watermark = get_watermark(conn)

        print(f"Current watermark: {watermark}")

        partitions = discover_partitions(watermark)

        print(f"Eligible partitions: {len(partitions)}")

        if not partitions:
            print("No eligible Silver partitions.")
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

            for path in files:
                file_id = path.as_posix()

                if already_loaded(
                    conn,
                    file_id,
                ):
                    continue

                with conn.transaction():
                    row_count = load_file(
                        conn,
                        path,
                    )

                    register_file(
                        conn,
                        file_id,
                        row_count,
                    )

                loaded_files += 1
                loaded_rows += row_count

                print(f"LOADED: {file_id} ({row_count} rows)")

            with conn.transaction():
                update_watermark(
                    conn,
                    partition_date,
                    partition_hour,
                )

    print()
    print("Load complete.")
    print(f"Files loaded: {loaded_files}")
    print(f"Rows processed: {loaded_rows}")


if __name__ == "__main__":
    main()
