{{ config(materialized='table') }}

with product_performance as (

    select *
    from {{ ref('int_product_performance') }}

),
final as (

    select
        product_id,
        product_name,
        category,
        supplier_id,
        unit_cost,
        orders_sold,
        total_units_sold,
        gross_sales,
        avg_unit_price_realized,
        return_count,
        units_returned,
        total_refund_amount,
        net_sales_after_returns,
        estimated_cogs,
        gross_profit_before_returns,
        gross_profit_after_returns,
        {{ safe_divide('units_returned', 'total_units_sold', default_value=0) }} as return_rate,
        {{ safe_divide('gross_profit_after_returns', 'gross_sales', default_value=0) }} as profit_margin_after_returns,
        {{ safe_divide('gross_profit_after_returns', 'estimated_cogs', default_value='null') }} as roi_after_returns
    from product_performance

)

select *
from final;

