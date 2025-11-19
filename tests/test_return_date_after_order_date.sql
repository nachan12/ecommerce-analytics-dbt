-- Singular test: Validates that return dates are not before the corresponding order date
-- This ensures data quality and logical consistency in return processing

select
    r.return_id,
    r.return_date,
    o.order_date,
    oi.order_id
from {{ ref('stg_returns') }} r
join {{ ref('stg_order_items') }} oi
    on r.order_item_id = oi.order_item_id
join {{ ref('stg_orders') }} o
    on oi.order_id = o.order_id
where r.return_date < o.order_date

