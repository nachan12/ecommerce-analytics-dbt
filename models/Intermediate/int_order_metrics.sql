{#
    Performance Optimization Strategy:
    - Materialization: Table to cache expensive aggregations (line items + returns)
    - Partitioning: Recommended to partition by order_date for incremental downstream processing
    - Clustering: Recommended to cluster by user_id for customer-level queries
    
    This intermediate table is heavily used by fct_orders and other marts, so materializing
    as a table avoids recomputing aggregations on every query.
#}
{{ config(
    materialized='table',
    # Uncomment and adjust for your warehouse:
    # BigQuery: partition_by={'field': 'order_date', 'data_type': 'date'}, cluster_by=['user_id']
    # Snowflake: cluster_by=['order_date', 'user_id']
    # Redshift: diststyle KEY distkey user_id, sortkey (order_date, user_id)
) }}

with orders as (

    select *
    from {{ ref('stg_orders') }}

),
order_line_metrics as (

    select
        order_id,
        sum(quantity) as total_item_quantity,
        count(*) as line_item_count,
        count(distinct product_id) as distinct_products,
        sum(gross_item_sales) as gross_item_sales
    from {{ ref('stg_order_items') }}
    group by 1

),
returns_by_order as (

    select
        oi.order_id,
        count(r.return_id) as return_count,
        sum(r.refund_amount) as total_refund_amount
    from {{ ref('stg_returns') }} r
    join {{ ref('stg_order_items') }} oi
        on r.order_item_id = oi.order_item_id
    group by 1

),
final as (

    select
        o.order_id,
        o.user_id,
        o.order_date,
        o.status,
        o.is_completed,
        o.total_amount as order_total_amount,
        coalesce(olm.total_item_quantity, 0) as total_item_quantity,
        coalesce(olm.line_item_count, 0) as line_item_count,
        coalesce(olm.distinct_products, 0) as distinct_products,
        coalesce(olm.gross_item_sales, 0) as gross_item_sales,
        coalesce(rbo.total_refund_amount, 0) as total_refund_amount,
        coalesce(olm.gross_item_sales, 0) - coalesce(rbo.total_refund_amount, 0) as net_sales,
        coalesce(rbo.return_count, 0) as return_count,
        -- Use dbt_utils.generate_surrogate_key for composite key
        {{ dbt_utils.generate_surrogate_key(['o.order_id', 'o.user_id', 'o.order_date']) }} as order_surrogate_key
    from orders o
    left join order_line_metrics olm using (order_id)
    left join returns_by_order rbo using (order_id)

)

select *
from final;

