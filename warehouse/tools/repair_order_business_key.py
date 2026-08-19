import argparse
import os
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

SILVER_ROOT = Path("data_lake/silver/orders")


def parse_args():
    parser = argparse.ArgumentParser(
        description=("Repair one order_id in durable " "Silver by event_id.")
    )

    parser.add_argument(
        "--event-id",
        required=True,
    )

    parser.add_argument(
        "--old-order-id",
        required=True,
    )

    parser.add_argument(
        "--new-order-id",
        required=True,
    )

    parser.add_argument(
        "--apply",
        action="store_true",
        help=("Apply the repair. Without this " "flag the command is dry-run only."),
    )

    return parser.parse_args()


def matching_rows(
    table,
    event_id,
):
    mask = pc.equal(
        table["event_id"],
        pa.scalar(event_id),
    )

    count = pc.sum(
        pc.cast(
            mask,
            pa.int64(),
        )
    ).as_py()

    return mask, count or 0


def main():
    args = parse_args()

    matches = []

    for path in sorted(SILVER_ROOT.rglob("*.parquet")):
        table = pq.ParquetFile(path).read()

        mask, count = matching_rows(
            table,
            args.event_id,
        )

        if count == 0:
            continue

        order_ids = pc.unique(
            pc.filter(
                table["order_id"],
                mask,
            )
        ).to_pylist()

        matches.append(
            (
                path,
                table,
                mask,
                count,
                order_ids,
            )
        )

    total_matches = sum(item[3] for item in matches)

    print(f"Event ID:       " f"{args.event_id}")

    print(f"Old order ID:   " f"{args.old_order_id}")

    print(f"New order ID:   " f"{args.new_order_id}")

    print(f"Silver matches: " f"{total_matches}")

    if total_matches == 0:
        raise SystemExit("No matching Silver rows found.")

    for (
        path,
        _,
        _,
        count,
        order_ids,
    ) in matches:
        print(f"- {path}: " f"{count} row(s), " f"order_id={order_ids}")

        if order_ids != [args.old_order_id]:
            raise SystemExit("Unexpected existing " "order_id; repair aborted.")

    if not args.apply:
        print()
        print("DRY RUN ONLY — " "no files changed.")
        return

    for (
        path,
        table,
        mask,
        count,
        _,
    ) in matches:
        order_id_index = table.schema.get_field_index("order_id")

        updated_order_ids = pc.if_else(
            mask,
            pa.scalar(
                args.new_order_id,
                type=table["order_id"].type,
            ),
            table["order_id"],
        )

        updated_table = table.set_column(
            order_id_index,
            "order_id",
            updated_order_ids,
        )

        temp_path = path.with_name(path.name + ".session22.tmp")

        pq.write_table(
            updated_table,
            temp_path,
        )

        verify_table = pq.ParquetFile(temp_path).read()

        (
            verify_mask,
            verify_count,
        ) = matching_rows(
            verify_table,
            args.event_id,
        )

        verify_order_ids = pc.unique(
            pc.filter(
                verify_table["order_id"],
                verify_mask,
            )
        ).to_pylist()

        if verify_count != count or verify_order_ids != [args.new_order_id]:
            temp_path.unlink(missing_ok=True)

            raise SystemExit("Verification failed; " "original file left " "unchanged.")

        os.replace(
            temp_path,
            path,
        )

        print(f"Updated {count} row(s): " f"{path}")

    print()

    print(f"APPLIED: {total_matches} " "Silver row(s) repaired.")


if __name__ == "__main__":
    main()
