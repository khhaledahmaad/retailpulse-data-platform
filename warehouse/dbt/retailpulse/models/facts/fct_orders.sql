{{
    config(
        materialized='incremental',
        unique_key='event_id',
        indexes=[
            {'columns': ['event_id'], 'unique': true}
        ]
    )
}}

select
    event_id,
    order_id,
    customer_id,
    product_id,

    event_timestamp as ordered_at,
    event_date,

    category,
    quantity,
    unit_price,
    order_value,
    currency,

    loaded_at

from {{ ref('stg_orders') }}

{% if is_incremental() %}

where loaded_at > (
    select coalesce(
        max(loaded_at),
        '1900-01-01'::timestamptz
    )
    from {{ this }}
)

{% endif %}