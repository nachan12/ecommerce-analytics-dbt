{#
    Performance Optimization Strategy:
    - Materialization: Table to cache expensive customer-level aggregations
    - Clustering: Recommended to cluster by customer_segment for segmentation queries
    
    This intermediate table aggregates all completed orders per customer, which is computationally
    expensive. Materializing as a table avoids recomputing lifetime metrics on every query.
#}
{{ config(
    materialized='table',
    Snowflake_cluster_by=['customer_segment']  
) }}

with customers as (

    select *
    from {{ ref('stg_users') }}

),
completed_order_metrics as (

    select *
    from {{ ref('int_order_metrics') }}
    where is_completed

),
customer_stats as (

    select
        user_id,
        count(*) as completed_order_count,
        sum(net_sales) as lifetime_revenue,
        sum(total_item_quantity) as lifetime_units,
        avg(net_sales) as avg_order_value,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        sum(return_count) as total_return_count,
        sum(total_refund_amount) as total_refunded_amount
    from completed_order_metrics
    group by 1

),
final as (

    select
        c.user_id,
        c.email,
        c.signup_date,
        c.country,
        c.customer_segment,
        coalesce(cs.completed_order_count, 0) as completed_order_count,
        coalesce(cs.lifetime_revenue, 0) as lifetime_revenue,
        coalesce(cs.lifetime_units, 0) as lifetime_units,
        coalesce(cs.avg_order_value, 0) as avg_order_value,
        cs.first_order_date,
        cs.most_recent_order_date,
        coalesce(cs.total_return_count, 0) as total_return_count,
        coalesce(cs.total_refunded_amount, 0) as total_refunded_amount
    from customers c
    left join customer_stats cs using (user_id)

)

select *
from final

