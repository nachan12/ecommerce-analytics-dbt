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
        case
            when total_units_sold = 0 then 0
            else cast(units_returned as {{ dbt.type_float() }}) / total_units_sold
        end as return_rate,
        case
            when gross_sales = 0 then 0
            else gross_profit_after_returns / gross_sales
        end as profit_margin_after_returns,
        case
            when estimated_cogs = 0 then null
            else gross_profit_after_returns / estimated_cogs
        end as roi_after_returns
    from product_performance

)

select *
from final;

