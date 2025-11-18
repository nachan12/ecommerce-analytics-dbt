{{ config(materialized='table') }}

with customer_lifetime as (

    select *
    from {{ ref('int_customer_lifetime') }}

),
loyalty_attributes as (

    select
        cl.*,
        case
            when cl.lifetime_revenue >= 500 then 'VIP'
            when cl.lifetime_revenue >= 200 then 'GROWTH'
            when cl.lifetime_revenue > 0 then 'NEW'
            else 'PROSPECT'
        end as loyalty_tier,
        case
            when cl.completed_order_count >= 5 then 'LOYAL'
            when cl.completed_order_count between 2 and 4 then 'ACTIVE'
            when cl.completed_order_count = 1 then 'NEW'
            else 'INACTIVE'
        end as engagement_band,
        case
            when cl.most_recent_order_date is not null then true
            else false
        end as is_active_customer
    from customer_lifetime cl

),
final as (

    select
        user_id as customer_id,
        email,
        signup_date,
        country,
        customer_segment,
        completed_order_count,
        lifetime_revenue,
        lifetime_units,
        avg_order_value,
        first_order_date,
        most_recent_order_date,
        total_return_count,
        total_refunded_amount,
        loyalty_tier,
        engagement_band,
        is_active_customer
    from loyalty_attributes

)

select *
from final;

