{{ config(materialized='table') }}

with dim_customers as (

    select *
    from {{ ref('dim_customers') }}

),
segment_rollup as (

    select
        customer_segment,
        loyalty_tier,
        count(*) as customer_count,
        sum(case when is_active_customer then 1 else 0 end) as active_customer_count,
        avg(lifetime_revenue) as avg_lifetime_revenue,
        sum(lifetime_revenue) as total_segment_revenue,
        avg(completed_order_count) as avg_completed_orders,
        sum(total_refunded_amount) as total_segment_refunds,
        avg(total_return_count) as avg_returns_per_customer
    from dim_customers
    group by 1, 2

),
final as (

    select
        customer_segment,
        loyalty_tier,
        customer_count,
        active_customer_count,
        {{ safe_divide('active_customer_count', 'customer_count', default_value=0) }} as active_customer_ratio,
        avg_lifetime_revenue,
        total_segment_revenue,
        avg_completed_orders,
        total_segment_refunds,
        avg_returns_per_customer,
        {{ safe_divide('total_segment_refunds', 'total_segment_revenue', default_value=0) }} as refund_to_revenue_ratio
    from segment_rollup

)

select *
from final;

