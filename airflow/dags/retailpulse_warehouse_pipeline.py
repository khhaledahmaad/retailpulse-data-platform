import os
from datetime import timedelta

import psycopg
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import (
    dag,
    get_current_context,
    task,
)
from pendulum import datetime

WAREHOUSE_ROOT = "/opt/retailpulse/warehouse"
DBT_ROOT = f"{WAREHOUSE_ROOT}/dbt/retailpulse"


def get_warehouse_connection():
    return psycopg.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


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
    tags=[
        "retailpulse",
        "warehouse",
        "dbt",
    ],
)


def retailpulse_warehouse_pipeline():

    @task(
        retries=2,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=2),
    )
    def start_pipeline_run() -> str:
        context = get_current_context()
        airflow_run_id = context["ti"].run_id

        with get_warehouse_connection() as conn:
            conn.execute(
                """
                INSERT INTO control.pipeline_runs (
                    airflow_run_id,
                    status
                )
                VALUES (%s, 'RUNNING')
                ON CONFLICT (airflow_run_id)
                DO UPDATE SET
                    started_at = NOW(),
                    finished_at = NULL,
                    loader_files_discovered = NULL,
                    loader_files_skipped = NULL,
                    loader_files_loaded = NULL,
                    loader_rows_processed = NULL,
                    loader_rows_inserted = NULL,
                    loader_duplicates = NULL,
                    dbt_status = NULL,
                    health_status = NULL,
                    raw_orders = NULL,
                    latest_load = NULL,
                    status = 'RUNNING',
                    error_message = NULL
                """,
                (airflow_run_id,),
            )

        return airflow_run_id

    run_incremental_loader = BashOperator(
        task_id="run_incremental_loader",
        bash_command=(
            "cd /opt/retailpulse && "
            "python -m warehouse.loader.load_orders "
            '--airflow-run-id "{{ run_id }}"'
        ),
        retries=2,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=5),
    )

    @task(
        retries=1,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=2),
    )
    def validate_raw_orders() -> dict:
        with get_warehouse_connection() as conn:
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
            "--target airflow "
            "--profiles-dir /opt/retailpulse/warehouse/dbt/retailpulse"
        ),
        retries=1,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=10),
    )

    check_pipeline_health = BashOperator(
        task_id="check_pipeline_health",
        bash_command=(
            "cd /opt/retailpulse && "
            "python -m warehouse.monitoring.check_pipeline_health "
        ),
        retries=0,
        execution_timeout=timedelta(minutes=5),
    )

    @task(
        retries=1,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=2),
    )
    def record_pipeline_metrics(
        validation: dict,
    ) -> None:
        print("RetailPulse warehouse pipeline complete")
        print(f"raw.orders rows: " f"{validation['row_count']}")
        print(f"latest load: " f"{validation['latest_load']}")

    @task(
        retries=1,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=2),
    )
    def complete_pipeline_run(
        validation: dict,
    ) -> None:
        context = get_current_context()
        airflow_run_id = context["ti"].run_id

        with get_warehouse_connection() as conn:
            conn.execute(
                """
                UPDATE control.pipeline_runs
                SET
                    finished_at = NOW(),
                    dbt_status = 'SUCCEEDED',
                    health_status = 'HEALTHY',
                    raw_orders = %s,
                    latest_load = %s,
                    status = 'SUCCEEDED'
                WHERE airflow_run_id = %s
                """,
                (
                    validation["row_count"],
                    validation["latest_load"],
                    airflow_run_id,
                ),
            )

        print(
            "RetailPulse pipeline run complete: "
            f"{airflow_run_id}"
        )

    @task(
        trigger_rule="one_failed",
        retries=0,
        execution_timeout=timedelta(minutes=2),
    )
    def record_pipeline_failure() -> None:
        context = get_current_context()

        ti = context["ti"]
        airflow_run_id = ti.run_id

        task_breadcrumbs = ti.get_task_breadcrumbs(
            dag_id=ti.dag_id,
            run_id=airflow_run_id,
        )

        failed_tasks = [
            breadcrumb["task_id"]
            for breadcrumb in task_breadcrumbs
            if breadcrumb.get("state") == "failed"
            and breadcrumb["task_id"] != "record_pipeline_failure"
        ]

        error_message = (
            "Pipeline failed. Failed tasks: "
            + ", ".join(sorted(failed_tasks))
        )

        with get_warehouse_connection() as conn:
            conn.execute(
                """
                UPDATE control.pipeline_runs
                SET
                    finished_at = NOW(),
                    status = 'FAILED',
                    error_message = %s
                WHERE airflow_run_id = %s
                """,
                (
                    error_message,
                    airflow_run_id,
                ),
            )

        print(error_message)

        raise RuntimeError(error_message)

    pipeline_run = start_pipeline_run()

    validation = validate_raw_orders()

    metrics = record_pipeline_metrics(validation)

    completion = complete_pipeline_run(validation)

    (
        pipeline_run
        >> run_incremental_loader
        >> validation
        >> run_dbt_build
        >> check_pipeline_health
        >> metrics
        >> completion
    )

    failure = record_pipeline_failure()

    [
        run_incremental_loader,
        validation,
        run_dbt_build,
        check_pipeline_health,
        metrics,
        completion,
    ] >> failure


retailpulse_warehouse_pipeline()
