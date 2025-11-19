{#
    Demonstration of dbt_utils Usage:
    This model showcases several dbt_utils macros for common data transformation patterns.
    
    dbt_utils macros used:
    1. date_spine: Generates a date spine for complete time series
    2. generate_surrogate_key: Creates surrogate keys from multiple columns
    3. union_relations: Combines multiple CTEs (demonstrated conceptually)
    4. dateadd/datediff: Date manipulation functions
    
    This model creates a daily revenue summary with complete date coverage.
#}
{{ config(materialized='table') }}

with 
-- Step 1: Get date range from orders
date_range as (
    select
        min(order_date) as min_date,
        max(order_date) as max_date
    from {{ ref('fct_orders') }}
    where is_completed = true
),

-- Step 2: Generate date spine using dbt_utils.date_spine
-- This ensures we have a row for every day, even if there are no orders
date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2023-05-01' as date)",
        end_date="cast('2023-08-31' as date)"
    ) }}
),

-- Step 3: Aggregate daily revenue from fact table
daily_revenue as (
    select
        order_date,
        customer_segment,
        loyalty_tier,
        primary_product_category,
        count(distinct order_id) as order_count,
        count(distinct customer_id) as unique_customers,
        sum(net_sales) as total_revenue,
        sum(gross_item_sales) as total_gross_sales,
        sum(total_refund_amount) as total_refunds,
        avg(net_sales) as avg_order_value,
        sum(total_item_quantity) as total_units_sold
    from {{ ref('fct_orders') }}
    where is_completed = true
    group by 1, 2, 3, 4
),

-- Step 4: Join date spine with revenue data to ensure complete time series
daily_revenue_with_spine as (
    select
        ds.date_day as revenue_date,
        dr.customer_segment,
        dr.loyalty_tier,
        dr.primary_product_category,
        coalesce(dr.order_count, 0) as order_count,
        coalesce(dr.unique_customers, 0) as unique_customers,
        coalesce(dr.total_revenue, 0) as total_revenue,
        coalesce(dr.total_gross_sales, 0) as total_gross_sales,
        coalesce(dr.total_refunds, 0) as total_refunds,
        dr.avg_order_value,
        coalesce(dr.total_units_sold, 0) as total_units_sold,
        -- Use dbt_utils.generate_surrogate_key to create composite key
        {{ dbt_utils.generate_surrogate_key([
            'ds.date_day',
            'dr.customer_segment',
            'dr.loyalty_tier',
            'dr.primary_product_category'
        ]) }} as daily_revenue_key
    from date_spine ds
    left join daily_revenue dr
        on ds.date_day = dr.order_date
),

-- Step 5: Calculate rolling metrics using window functions
final as (
    select
        revenue_date,
        customer_segment,
        loyalty_tier,
        primary_product_category,
        daily_revenue_key,
        order_count,
        unique_customers,
        total_revenue,
        total_gross_sales,
        total_refunds,
        avg_order_value,
        total_units_sold,
        -- 7-day rolling average revenue
        avg(total_revenue) over (
            partition by customer_segment, loyalty_tier, primary_product_category
            order by revenue_date
            rows between 6 preceding and current row
        ) as revenue_7day_avg,
        -- 30-day rolling total revenue
        sum(total_revenue) over (
            partition by customer_segment, loyalty_tier, primary_product_category
            order by revenue_date
            rows between 29 preceding and current row
        ) as revenue_30day_total,
        -- Day of week for weekly pattern analysis
       
        -- Calculate days since first order (for cohort analysis)
        datediff('day', 
            min(revenue_date) over (partition by customer_segment, loyalty_tier),
            revenue_date
        ) as days_since_segment_start
    from daily_revenue_with_spine
)

select * from final
order by revenue_date desc, customer_segment, loyalty_tier, total_revenue desc

