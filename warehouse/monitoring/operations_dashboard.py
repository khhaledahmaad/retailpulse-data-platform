import os
from html import escape
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from psycopg import Error as PsycopgError

from warehouse.monitoring.check_pipeline_health import get_connection
from warehouse.monitoring.operations_view import (
    fetch_active_incidents,
    fetch_latest_metrics,
    fetch_metric_history,
    fetch_recent_runs,
)

HOST = os.getenv("OPERATIONS_DASHBOARD_HOST", "127.0.0.1")
PORT = int(os.getenv("OPERATIONS_DASHBOARD_PORT", "8084"))


def fmt(value):
    return "—" if value is None else escape(str(value))


def fmt_ts(value):
    if value is None:
        return "—"

    return escape(value.strftime("%Y-%m-%d %H:%M:%S %Z"))


def badge(value):
    status = "UNKNOWN" if value is None else str(value)

    return f'<span class="badge {escape(status.lower())}">' f"{escape(status)}</span>"


def table(headers, rows):
    if not rows:
        return '<p class="muted">No records.</p>'

    header_html = "".join(f"<th>{escape(header)}</th>" for header in headers)

    row_html = "".join(
        "<tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>" for row in rows
    )

    return (
        '<div class="table-wrap"><table>'
        f"<thead><tr>{header_html}</tr></thead>"
        f"<tbody>{row_html}</tbody>"
        "</table></div>"
    )


def render_trend_chart(
    history,
    title,
    value_key,
    unit="",
):
    observations = [
        (
            item["recorded_at"],
            float(item[value_key]),
        )
        for item in history
        if item[value_key] is not None
    ]

    if not observations:
        return f"""
        <div class="trend-card">
            <div class="label">
                {escape(title)}
            </div>
            <p class="muted">
                No historical values available.
            </p>
        </div>
        """

    width = 600
    height = 190

    left = 48
    right = 18
    top = 18
    bottom = 36

    plot_width = width - left - right
    plot_height = height - top - bottom

    values = [value for _, value in observations]

    minimum = min(values)
    maximum = max(values)

    if minimum == maximum:
        padding = max(
            abs(maximum) * 0.1,
            1,
        )
    else:
        padding = (maximum - minimum) * 0.1

    axis_minimum = minimum - padding
    axis_maximum = maximum + padding

    if minimum >= 0:
        axis_minimum = max(
            0,
            axis_minimum,
        )

    value_range = axis_maximum - axis_minimum

    point_count = len(observations)

    points = []

    for index, (timestamp, value) in enumerate(observations):
        if point_count == 1:
            x = left + (plot_width / 2)
        else:
            x = left + index * plot_width / (point_count - 1)

        y = top + (axis_maximum - value) / value_range * plot_height

        points.append(
            (
                x,
                y,
                timestamp,
                value,
            )
        )

    grid_lines = []

    grid_count = 4

    for index in range(grid_count):
        ratio = index / (grid_count - 1)

        y = top + ratio * plot_height

        grid_value = axis_maximum - ratio * value_range

        grid_lines.append(f"""
            <line
                class="trend-grid-line"
                x1="{left}"
                y1="{y:.1f}"
                x2="{width - right}"
                y2="{y:.1f}"
            />

            <text
                class="trend-axis-label"
                x="{left - 8}"
                y="{y + 4:.1f}"
                text-anchor="end"
            >
                {grid_value:g}
            </text>
            """)

    if point_count == 1:
        line_shape = ""
    else:
        point_string = " ".join(f"{x:.1f},{y:.1f}" for x, y, _, _ in points)

        line_shape = "<polyline " 'class="trend-line" ' f'points="{point_string}" />'

    point_shapes = []

    for x, y, timestamp, value in points:
        tooltip = f"{timestamp.strftime('%Y-%m-%d %H:%M:%S %Z')}" f": {value:g}{unit}"

        point_shapes.append(f"""
            <circle
                class="trend-point"
                cx="{x:.1f}"
                cy="{y:.1f}"
                r="4"
                tabindex="0"
                aria-label="{escape(tooltip)}"
            >
                <title>
                    {escape(tooltip)}
                </title>
            </circle>
            """)

    latest = values[-1]

    first_timestamp = observations[0][0].strftime("%H:%M")

    last_timestamp = observations[-1][0].strftime("%H:%M")

    return f"""
    <div class="trend-card">

        <div class="trend-header">
            <div>
                <div class="label">
                    {escape(title)}
                </div>

                <div class="trend-latest">
                    {latest:g}{escape(unit)}
                </div>
            </div>

            <div class="trend-observations">
                {point_count} snapshots
            </div>
        </div>

        <svg
            class="trend-chart"
            viewBox="0 0 {width} {height}"
            role="img"
            aria-label="{escape(title)} trend"
        >
            {''.join(grid_lines)}

            {line_shape}

            {''.join(point_shapes)}

            <text
                class="trend-time-label"
                x="{left}"
                y="{height - 8}"
                text-anchor="start"
            >
                {escape(first_timestamp)}
            </text>

            <text
                class="trend-time-label"
                x="{width - right}"
                y="{height - 8}"
                text-anchor="end"
            >
                {escape(last_timestamp)}
            </text>

        </svg>

        <div class="trend-meta">
            <span>
                min {minimum:g}{escape(unit)}
            </span>

            <span>
                Hover points for details
            </span>

            <span>
                max {maximum:g}{escape(unit)}
            </span>
        </div>

    </div>
    """


def render_dashboard(
    metrics,
    incidents,
    runs,
    history,
):
    if metrics is None:
        return "<h1>RetailPulse Operations</h1>" "<p>No metrics available.</p>"

    incident_rows = [
        (
            fmt(item["incident_type"]),
            badge(item["severity"]),
            fmt_ts(item["opened_at"]),
            fmt_ts(item["alert_sent_at"]),
            fmt(item["details"]),
        )
        for item in incidents
    ]

    run_rows = []

    for run in runs:
        duration = "—"

        if run["started_at"] and run["finished_at"]:
            seconds = (run["finished_at"] - run["started_at"]).total_seconds()

            duration = f"{seconds:.1f}s"

        run_rows.append(
            (
                badge(run["status"]),
                fmt(run["airflow_run_id"]),
                fmt_ts(run["started_at"]),
                duration,
                badge(run["dbt_status"]),
                badge(run["health_status"]),
                fmt(run["loader_files_loaded"]),
                fmt(run["loader_rows_inserted"]),
            )
        )

    history_rows = [
        (
            fmt_ts(item["recorded_at"]),
            badge(item["status"]),
            fmt(item["silver_raw_lag"]),
            fmt(item["silver_duplicate_deliveries"]),
            fmt(item["freshness_minutes"]),
        )
        for item in history
    ]

    chronological_history = list(
        reversed(history)
    )

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta
    name="viewport"
    content="width=device-width, initial-scale=1"
>
<meta http-equiv="refresh" content="60">
<title>RetailPulse Operations</title>

<style>
:root {{
    font-family: Arial, sans-serif;
    color: #18212f;
    background: #f4f6f8;
}}

body {{
    margin: 0;
}}

main {{
    max-width: 1400px;
    margin: auto;
    padding: 24px;
}}

h1,
h2 {{
    margin-bottom: 8px;
}}

.muted {{
    color: #667085;
}}

.cards {{
    display: grid;
    grid-template-columns:
        repeat(
            auto-fit,
            minmax(180px, 1fr)
        );
    gap: 12px;
    margin: 20px 0;
}}

.card,
section {{
    background: white;
    border: 1px solid #e4e7ec;
    border-radius: 10px;
}}

.card {{
    padding: 16px;
}}

.label {{
    color: #667085;
    font-size: 13px;
}}

.value {{
    font-size: 24px;
    font-weight: 700;
    margin-top: 6px;
}}

section {{
    margin-top: 16px;
    padding: 18px;
}}

.badge {{
    display: inline-block;
    padding: 3px 8px;
    border-radius: 999px;
    font-size: 12px;
    font-weight: 700;
    background: #eef2f6;
}}

.healthy,
.succeeded {{
    background: #dcfae6;
    color: #067647;
}}

.warning,
.running {{
    background: #fef0c7;
    color: #b54708;
}}

.degraded,
.failed {{
    background: #fee4e2;
    color: #b42318;
}}

.table-wrap {{
    overflow-x: auto;
}}

table {{
    border-collapse: collapse;
    width: 100%;
    margin-top: 12px;
    font-size: 13px;
}}

th,
td {{
    border-bottom: 1px solid #eaecf0;
    padding: 10px 12px;
    text-align: left;
    vertical-align: top;
    white-space: nowrap;
}}

th {{
    color: #475467;
    background: #f9fafb;
}}

.trend-grid {{
    display: grid;
    grid-template-columns:
        repeat(
            auto-fit,
            minmax(320px, 1fr)
        );
    gap: 16px;
    margin-top: 16px;
}}

.trend-card {{
    border: 1px solid #eaecf0;
    border-radius: 10px;
    padding: 18px;
}}

.trend-header {{
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
}}

.trend-latest {{
    font-size: 24px;
    font-weight: 700;
    margin-top: 6px;
}}

.trend-observations {{
    color: #667085;
    font-size: 12px;
}}

.trend-chart {{
    width: 100%;
    height: auto;
    margin-top: 12px;
    overflow: visible;
}}

.trend-grid-line {{
    stroke: #eaecf0;
    stroke-width: 1;
}}

.trend-line {{
    fill: none;
    stroke: #475467;
    stroke-width: 2.5;
    stroke-linejoin: round;
    stroke-linecap: round;
}}

.trend-point {{
    fill: white;
    stroke: #475467;
    stroke-width: 2.5;
    cursor: pointer;
    transition:
        r 0.15s ease,
        stroke-width 0.15s ease;
}}

.trend-point:hover,
.trend-point:focus {{
    r: 6;
    stroke-width: 3;
    outline: none;
}}

.trend-axis-label,
.trend-time-label {{
    fill: #667085;
    font-size: 11px;
}}

.trend-meta {{
    display: flex;
    justify-content: space-between;
    gap: 12px;
    color: #667085;
    font-size: 12px;
    margin-top: 4px;
}}
</style>
</head>

<body>
<main>

<h1>RetailPulse Operations</h1>

<p class="muted">
Latest snapshot:
{fmt_ts(metrics["recorded_at"])}
· refreshes every 60 seconds
</p>

<div class="cards">

<div class="card">
<div class="label">
Current health
</div>
<div class="value">
{badge(metrics["status"])}
</div>
</div>

<div class="card">
<div class="label">
Active incidents
</div>
<div class="value">
{len(incidents)}
</div>
</div>

<div class="card">
<div class="label">
Silver→Raw lag
</div>
<div class="value">
{fmt(metrics["silver_raw_lag"])}
</div>
</div>

<div class="card">
<div class="label">
Silver duplicates
</div>
<div class="value">
{fmt(
    metrics[
        "silver_duplicate_deliveries"
    ]
)}
</div>
</div>

<div class="card">
<div class="label">
Freshness
</div>
<div class="value">
{fmt(metrics["freshness_minutes"])}
min
</div>
</div>

</div>

<section>

<h2>Layer reconciliation</h2>

{table(
    [
        "Bronze",
        "Silver physical",
        "Silver unique",
        "Quarantine",
        "Raw",
        "Fact",
        "Gold",
    ],
    [
        (
            fmt(metrics["bronze_rows"]),
            fmt(metrics["silver_rows"]),
            fmt(
                metrics[
                    "silver_unique_events"
                ]
            ),
            fmt(metrics["quarantine_rows"]),
            fmt(metrics["raw_orders"]),
            fmt(metrics["fact_orders"]),
            fmt(
                metrics[
                    "gold_order_count"
                ]
            ),
        )
    ],
)}

</section>

<section>

<h2>Active incidents</h2>

{table(
    [
        "Incident",
        "Severity",
        "Opened",
        "Alert sent",
        "Details",
    ],
    incident_rows,
)}

</section>

<section>

<h2>Recent pipeline runs</h2>

{table(
    [
        "Run",
        "Airflow run ID",
        "Started",
        "Duration",
        "dbt",
        "Health",
        "Files",
        "Inserted",
    ],
    run_rows,
)}

</section>

<section>

<h2>Trends</h2>

<p class="muted">
Latest duplicate-aware monitoring snapshots,
oldest to newest.
</p>

<div class="trend-grid">

{render_trend_chart(
    chronological_history,
    "Silver→Raw logical lag",
    "silver_raw_lag",
)}

{render_trend_chart(
    chronological_history,
    "Warehouse freshness",
    "freshness_minutes",
    " min",
)}

</div>

</section>

<section>

<h2>Metric history</h2>

<p class="muted">
Latest 24 duplicate-aware snapshots.
</p>

{table(
    [
        "Recorded",
        "Health",
        "Silver→Raw lag",
        "Duplicates",
        "Freshness min",
    ],
    history_rows,
)}

</section>

</main>
</body>
</html>"""


def load_dashboard_data():
    with get_connection() as conn:
        return (
            fetch_latest_metrics(conn),
            fetch_active_incidents(conn),
            fetch_recent_runs(
                conn,
                limit=10,
            ),
            fetch_metric_history(
                conn,
                limit=24,
            ),
        )


class DashboardHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        request_path = self.path.split(
            "?",
            1,
        )[0]

        if request_path not in (
            "/",
            "/index.html",
        ):
            self.send_error(404)
            return

        try:
            data = load_dashboard_data()

        except PsycopgError:
            self.send_error(
                500,
                "Dashboard database query failed",
            )
            return

        body = render_dashboard(
            *data
        ).encode("utf-8")

        self.send_response(200)

        self.send_header(
            "Content-Type",
            "text/html; charset=utf-8",
        )

        self.send_header(
            "Cache-Control",
            "no-store",
        )

        self.send_header(
            "Content-Length",
            str(len(body)),
        )

        self.end_headers()
        self.wfile.write(body)


def main():
    server = ThreadingHTTPServer(
        (HOST, PORT),
        DashboardHandler,
    )

    print("RetailPulse Operations Dashboard: " f"http://{HOST}:{PORT}")

    print("Press Ctrl+C to stop.")

    try:
        server.serve_forever()

    except KeyboardInterrupt:
        print("\nStopping dashboard.")

    finally:
        server.server_close()


if __name__ == "__main__":
    main()
