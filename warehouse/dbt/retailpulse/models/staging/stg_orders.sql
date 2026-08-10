select
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

    ingested_at,
    loaded_at

from {{ source('raw', 'orders') }}