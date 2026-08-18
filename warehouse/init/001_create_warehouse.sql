CREATE SCHEMA IF NOT EXISTS raw;

CREATE SCHEMA IF NOT EXISTS control;


CREATE TABLE IF NOT EXISTS raw.orders (
    event_id           TEXT PRIMARY KEY,
    event_type         TEXT NOT NULL,
    event_timestamp    TIMESTAMPTZ NOT NULL,
    event_date         DATE NOT NULL,

    order_id           TEXT NOT NULL,
    customer_id        TEXT,
    product_id         TEXT NOT NULL,
    category           TEXT NOT NULL,

    quantity           INTEGER NOT NULL,
    unit_price         NUMERIC(12, 2) NOT NULL,
    order_value        NUMERIC(14, 2) NOT NULL,
    currency           CHAR(3) NOT NULL,

    kafka_key          TEXT,
    kafka_topic        TEXT NOT NULL,
    kafka_partition    INTEGER NOT NULL,
    kafka_offset       BIGINT NOT NULL,
    kafka_timestamp    TIMESTAMPTZ,
    ingested_at        TIMESTAMPTZ,

    loaded_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS control.loaded_files (
    file_path          TEXT PRIMARY KEY,
    dataset_name       TEXT NOT NULL,
    row_count          BIGINT NOT NULL,
    loaded_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS control.loader_watermarks (
    dataset_name       TEXT PRIMARY KEY,
    watermark_date     DATE NOT NULL,
    watermark_hour     INTEGER NOT NULL
        CHECK (
            watermark_hour >= 0
            AND watermark_hour <= 23
        ),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS control.pipeline_metrics (
    metric_id BIGSERIAL PRIMARY KEY,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    bronze_rows BIGINT,
    silver_rows BIGINT,
    quarantine_rows BIGINT,

    raw_orders BIGINT,
    fact_orders BIGINT,
    gold_order_count BIGINT,

    latest_loaded_at TIMESTAMPTZ,

    status TEXT NOT NULL,
    details TEXT
);

CREATE TABLE IF NOT EXISTS control.event_reprocessing_log (
    reprocessing_id BIGSERIAL PRIMARY KEY,

    event_id UUID NOT NULL,
    order_id TEXT,

    original_contract_error TEXT,
    original_validation_error TEXT,
    original_kafka_timestamp TIMESTAMPTZ,

    corrections JSONB NOT NULL,

    action TEXT NOT NULL,
    status TEXT NOT NULL,

    republished_topic TEXT,
    republished_partition INTEGER,
    republished_offset BIGINT,

    error_message TEXT,

    created_at TIMESTAMPTZ NOT NULL
        DEFAULT NOW(),

    CONSTRAINT event_reprocessing_action_check
        CHECK (action IN ('DRY_RUN', 'PUBLISH')),

    CONSTRAINT event_reprocessing_status_check
        CHECK (
            status IN (
                'DRY_RUN',
                'PUBLISHED',
                'PUBLISH_FAILED'
            )
        )
);

CREATE INDEX IF NOT EXISTS
    idx_event_reprocessing_log_event_id
ON control.event_reprocessing_log (event_id);

CREATE TABLE IF NOT EXISTS control.pipeline_runs (
    pipeline_run_id BIGSERIAL PRIMARY KEY,

    airflow_run_id TEXT NOT NULL UNIQUE,

    started_at TIMESTAMPTZ NOT NULL
        DEFAULT NOW(),
    finished_at TIMESTAMPTZ,

    loader_files_discovered INTEGER,
    loader_files_skipped INTEGER,
    loader_files_loaded INTEGER,

    loader_rows_processed INTEGER,
    loader_rows_inserted INTEGER,
    loader_duplicates INTEGER,

    dbt_status TEXT,
    health_status TEXT,

    raw_orders BIGINT,
    latest_load TIMESTAMPTZ,

    status TEXT NOT NULL
        DEFAULT 'RUNNING',

    error_message TEXT,

    CONSTRAINT pipeline_runs_status_check
        CHECK (
            status IN (
                'RUNNING',
                'SUCCEEDED',
                'FAILED'
            )
        ),

    CONSTRAINT pipeline_runs_dbt_status_check
        CHECK (
            dbt_status IS NULL
            OR dbt_status IN (
                'SUCCEEDED',
                'FAILED'
            )
        ),

    CONSTRAINT pipeline_runs_health_status_check
        CHECK (
            health_status IS NULL
            OR health_status IN (
                'HEALTHY',
                'WARNING',
                'DEGRADED'
            )
        )
);

CREATE INDEX IF NOT EXISTS
    idx_pipeline_runs_started_at
ON control.pipeline_runs (started_at DESC);