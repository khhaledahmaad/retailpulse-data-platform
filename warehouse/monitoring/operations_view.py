from psycopg.rows import dict_row

from warehouse.monitoring.check_pipeline_health import get_connection


def fetch_latest_metrics(conn):
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            SELECT
                metric_id,
                recorded_at,
                bronze_rows,
                silver_rows,
                silver_unique_events,
                silver_rows - silver_unique_events
                    AS silver_duplicate_deliveries,
                quarantine_rows,
                raw_orders,
                fact_orders,
                gold_order_count,
                silver_unique_events - raw_orders
                    AS silver_raw_lag,
                latest_loaded_at,
                CASE
                    WHEN latest_loaded_at IS NULL
                    THEN NULL
                    ELSE ROUND(
                        EXTRACT(
                            EPOCH FROM (
                                recorded_at - latest_loaded_at
                            )
                        ) / 60
                    )::BIGINT
                END AS freshness_minutes,
                status,
                details
            FROM control.pipeline_metrics
            ORDER BY recorded_at DESC
            LIMIT 1
            """)

        return cur.fetchone()


def fetch_active_incidents(conn):
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""
            SELECT
                incident_id,
                incident_type,
                severity,
                details,
                opened_at,
                alert_sent_at
            FROM control.pipeline_incidents
            WHERE resolved_at IS NULL
            ORDER BY opened_at DESC
            """)

        return cur.fetchall()


def fetch_recent_runs(conn, limit=5):
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT
                pipeline_run_id,
                airflow_run_id,
                started_at,
                finished_at,
                status,
                dbt_status,
                health_status,
                loader_files_loaded,
                loader_rows_inserted,
                loader_duplicates,
                raw_orders,
                latest_load,
                error_message
            FROM control.pipeline_runs
            ORDER BY started_at DESC
            LIMIT %s
            """,
            (limit,),
        )

        return cur.fetchall()


def fetch_metric_history(conn, limit=24):
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT
                recorded_at,
                status,
                silver_rows,
                silver_unique_events,
                silver_rows - silver_unique_events
                    AS silver_duplicate_deliveries,
                raw_orders,
                silver_unique_events - raw_orders
                    AS silver_raw_lag,
                CASE
                    WHEN latest_loaded_at IS NULL
                    THEN NULL
                    ELSE ROUND(
                        EXTRACT(
                            EPOCH FROM (
                                recorded_at - latest_loaded_at
                            )
                        ) / 60
                    )::BIGINT
                END AS freshness_minutes
            FROM control.pipeline_metrics
            WHERE silver_unique_events IS NOT NULL
            ORDER BY recorded_at DESC
            LIMIT %s
            """,
            (limit,),
        )

        return cur.fetchall()


def print_operations_view(
    metrics,
    incidents,
    runs,
):
    print()
    print("RetailPulse Operations View")
    print("---------------------------")

    if metrics is None:
        print("No pipeline metrics available.")
        return

    print(f"Health:            {metrics['status']}")
    print(f"Bronze rows:       {metrics['bronze_rows']}")
    print(f"Silver rows:       {metrics['silver_rows']}")
    print("Silver unique:     " f"{metrics['silver_unique_events']}")
    print("Silver duplicates: " f"{metrics['silver_duplicate_deliveries']}")
    print(f"Quarantine rows:   {metrics['quarantine_rows']}")
    print(f"Raw orders:        {metrics['raw_orders']}")
    print(f"Fact orders:       {metrics['fact_orders']}")
    print(f"Gold order count:  {metrics['gold_order_count']}")
    print(f"Silver→Raw lag:    {metrics['silver_raw_lag']}")
    print("Freshness:         " f"{metrics['freshness_minutes']} minutes")

    print()
    print(f"Active incidents:  {len(incidents)}")

    for incident in incidents:
        print(
            f"- [{incident['severity']}] "
            f"{incident['incident_type']} "
            f"(opened {incident['opened_at']})"
        )

    print()
    print("Recent pipeline runs:")

    if not runs:
        print("- No pipeline runs recorded.")
    else:
        for run in runs:
            print(
                f"- {run['status']} | "
                f"{run['airflow_run_id']} | "
                f"{run['started_at']} | "
                f"health={run['health_status']}"
            )

    print()


def main():
    with get_connection() as conn:
        metrics = fetch_latest_metrics(conn)
        incidents = fetch_active_incidents(conn)
        runs = fetch_recent_runs(conn)

    print_operations_view(
        metrics=metrics,
        incidents=incidents,
        runs=runs,
    )


if __name__ == "__main__":
    main()
