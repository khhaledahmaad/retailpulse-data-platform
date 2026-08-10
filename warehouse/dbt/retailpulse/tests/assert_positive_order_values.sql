select
    event_id,
    order_value

from {{ ref('fct_orders') }}

where order_value <= 0