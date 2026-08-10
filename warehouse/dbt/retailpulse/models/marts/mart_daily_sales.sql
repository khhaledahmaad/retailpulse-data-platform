select
    event_date,

    count(*) as order_count,

    sum(quantity) as units_sold,

    round(
        sum(order_value),
        2
    ) as gross_revenue,

    round(
        avg(order_value),
        2
    ) as average_order_value

from {{ ref('fct_orders') }}

group by event_date