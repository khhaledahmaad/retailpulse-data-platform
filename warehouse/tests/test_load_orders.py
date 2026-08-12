from datetime import date
import math

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

    result = load_orders.discover_partitions((date(2026, 8, 12), 20))

    discovered = [
        (partition_date, partition_hour) for partition_date, partition_hour, _ in result
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

    result = load_orders.discover_files_in_partition(partition)

    assert [path.name for path in result] == [
        "part-00001.parquet",
        "part-00002.parquet",
    ]
