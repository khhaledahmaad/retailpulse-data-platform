select
    order_id,
    count(distinct event_id) as distinct_event_count

from {{ ref('stg_orders') }}

group by order_id

having count(distinct event_id) > 1