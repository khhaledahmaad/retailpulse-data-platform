from datetime import timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task
from pendulum import datetime

import psycopg

WAREHOUSE_ROOT = "/opt/retailpulse/warehouse"
DBT_ROOT = f"{WAREHOUSE_ROOT}/dbt/retailpulse"


@dag(
    dag_id="retailpulse_warehouse_pipeline",
    schedule="*/10 * * * *",
    start_date=datetime(
        2026,
        8,
        1,
        tz="UTC",
    ),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=[
        "retailpulse",
        "warehouse",
        "dbt",
    ],
)
def retailpulse_warehouse_pipeline():

    run_incremental_loader = BashOperator(
        task_id="run_incremental_loader",
        bash_command=(
            "cd /opt/retailpulse && " "python warehouse/loader/load_orders.py"
        ),
    )

    @task
    def validate_raw_orders() -> dict:
        with psycopg.connect(
            host="postgres",
            port=5432,
            dbname="retailpulse",
            user="retailpulse",
            password="retailpulse",
        ) as conn:
            row = conn.execute("""
                SELECT
                    COUNT(*) AS row_count,
                    MAX(loaded_at) AS latest_load
                FROM raw.orders
                """).fetchone()

        row_count = row[0]
        latest_load = row[1]

        if row_count == 0:
            raise ValueError("raw.orders contains no rows")

        return {
            "row_count": row_count,
            "latest_load": str(latest_load),
        }

    run_dbt_build = BashOperator(
        task_id="run_dbt_build",
        bash_command=(
            f"cd {DBT_ROOT} && "
            "dbt build "
            "--profiles-dir /opt/retailpulse/warehouse/dbt/retailpulse"
        ),
    )

    @task
    def record_pipeline_metrics(
        validation: dict,
    ) -> None:
        print("RetailPulse warehouse pipeline complete")
        print(f"raw.orders rows: " f"{validation['row_count']}")
        print(f"latest load: " f"{validation['latest_load']}")

    validation = validate_raw_orders()

    run_incremental_loader >> validation
    validation >> run_dbt_build

    metrics = record_pipeline_metrics(validation)

    run_dbt_build >> metrics


retailpulse_warehouse_pipeline()
