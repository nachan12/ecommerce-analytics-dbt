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
        case
            when customer_count = 0 then 0
            else cast(active_customer_count as {{ dbt.type_float() }}) / customer_count
        end as active_customer_ratio,
        avg_lifetime_revenue,
        total_segment_revenue,
        avg_completed_orders,
        total_segment_refunds,
        avg_returns_per_customer,
        case
            when total_segment_revenue = 0 then 0
            else total_segment_refunds / total_segment_revenue
        end as refund_to_revenue_ratio
    from segment_rollup

)

select *
from final;

