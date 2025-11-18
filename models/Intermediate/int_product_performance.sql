{{ config(materialized='table') }}

with products as (

    select *
    from {{ ref('stg_products') }}

),
completed_order_items as (

    select
        oi.order_item_id,
        oi.order_id,
        oi.product_id,
        oi.quantity,
        oi.unit_price,
        oi.gross_item_sales
    from {{ ref('stg_order_items') }} oi
    join {{ ref('int_order_metrics') }} om
        on oi.order_id = om.order_id
    where om.is_completed

),
product_sales as (

    select
        product_id,
        count(distinct order_id) as orders_sold,
        sum(quantity) as total_units_sold,
        sum(gross_item_sales) as gross_sales
    from completed_order_items
    group by 1

),
product_returns as (

    select
        oi.product_id,
        count(r.return_id) as return_count,
        sum(oi.quantity) as units_returned,
        sum(r.refund_amount) as refund_amount
    from {{ ref('stg_returns') }} r
    join {{ ref('stg_order_items') }} oi
        on r.order_item_id = oi.order_item_id
    group by 1

),
final as (

    select
        p.product_id,
        p.product_name,
        p.category,
        p.supplier_id,
        p.unit_cost,
        coalesce(ps.orders_sold, 0) as orders_sold,
        coalesce(ps.total_units_sold, 0) as total_units_sold,
        coalesce(ps.gross_sales, 0) as gross_sales,
        {{ safe_divide('coalesce(ps.gross_sales, 0)', 'ps.total_units_sold', default_value='null') }} as avg_unit_price_realized,
        coalesce(pr.return_count, 0) as return_count,
        coalesce(pr.units_returned, 0) as units_returned,
        coalesce(pr.refund_amount, 0) as total_refund_amount,
        coalesce(ps.gross_sales, 0) - coalesce(pr.refund_amount, 0) as net_sales_after_returns,
        p.unit_cost * coalesce(ps.total_units_sold, 0) as estimated_cogs,
        coalesce(ps.gross_sales, 0) - (p.unit_cost * coalesce(ps.total_units_sold, 0)) as gross_profit_before_returns,
        coalesce(ps.gross_sales, 0) - coalesce(pr.refund_amount, 0) - (p.unit_cost * coalesce(ps.total_units_sold, 0)) as gross_profit_after_returns
    from products p
    left join product_sales ps using (product_id)
    left join product_returns pr using (product_id)

)

select *
from final;

