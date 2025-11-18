{#
    Complex Analytical Query: Customer Segment Product Performance Analysis
    
    This analysis demonstrates advanced SQL patterns including:
    - Multiple CTEs for readability and modularity
    - Window functions for ranking and running calculations
    - Complex joins across fact and dimension tables
    - Conditional aggregations and case statements
    - Cohort analysis and trend calculations
    - Subqueries and correlated aggregations
    
    Business Questions Answered:
    1. Which products perform best within each customer segment?
    2. What is the average order value trend by segment over time?
    3. Which customer segments have the highest return rates by product category?
    4. What is the customer lifetime value distribution by segment and product category?
    5. Which products drive the most revenue from VIP customers?
    
    Usage:
    Run this query directly in your SQL client or use: dbt compile --select analyses/customer_segment_product_performance_analysis
#}

with 
-- Step 1: Enrich orders with customer and product dimensions
enriched_orders as (
    select
        fo.order_id,
        fo.order_date,
        fo.customer_id,
        fo.customer_segment,
        fo.loyalty_tier,
        fo.customer_country,
        fo.primary_product_id,
        fo.primary_product_name,
        fo.primary_product_category,
        fo.net_sales,
        fo.gross_item_sales,
        fo.total_refund_amount,
        fo.return_count,
        fo.total_item_quantity,
        fo.distinct_products,
        dc.lifetime_revenue as customer_lifetime_revenue,
        dc.completed_order_count as customer_total_orders,
        dc.avg_order_value as customer_avg_order_value,
        dc.engagement_band
    from {{ ref('fct_orders') }} fo
    inner join {{ ref('dim_customers') }} dc
        on fo.customer_id = dc.customer_id
    where fo.is_completed = true
),

-- Step 2: Calculate monthly segment-level metrics with window functions
monthly_segment_metrics as (
    select
        date_trunc('month', order_date) as order_month,
        customer_segment,
        loyalty_tier,
        primary_product_category,
        count(distinct order_id) as order_count,
        count(distinct customer_id) as unique_customers,
        sum(net_sales) as total_net_sales,
        sum(gross_item_sales) as total_gross_sales,
        sum(total_refund_amount) as total_refunds,
        sum(return_count) as total_returns,
        sum(total_item_quantity) as total_units_sold,
        avg(net_sales) as avg_order_value,
        -- Window function: Calculate running total by segment
        sum(sum(net_sales)) over (
            partition by customer_segment 
            order by date_trunc('month', order_date)
            rows between unbounded preceding and current row
        ) as segment_running_total_revenue,
        -- Window function: Calculate month-over-month growth
        lag(sum(net_sales)) over (
            partition by customer_segment 
            order by date_trunc('month', order_date)
        ) as previous_month_revenue
    from enriched_orders
    group by 1, 2, 3, 4
),

-- Step 3: Calculate growth rates and trends
segment_trends as (
    select
        *,
        -- Calculate month-over-month growth rate
        case 
            when previous_month_revenue > 0 
            then ((total_net_sales - previous_month_revenue) / previous_month_revenue) * 100
            else null
        end as mom_growth_rate_pct,
        -- Calculate return rate
        case 
            when total_units_sold > 0 
            then (total_returns::float / total_units_sold) * 100
            else 0
        end as return_rate_pct,
        -- Calculate refund rate
        case 
            when total_gross_sales > 0 
            then (total_refunds / total_gross_sales) * 100
            else 0
        end as refund_rate_pct
    from monthly_segment_metrics
),

-- Step 4: Product performance by segment with rankings
product_segment_performance as (
    select
        customer_segment,
        loyalty_tier,
        primary_product_category,
        primary_product_id,
        primary_product_name,
        count(distinct order_id) as order_count,
        count(distinct customer_id) as unique_customers,
        sum(net_sales) as total_revenue,
        sum(total_item_quantity) as total_units,
        avg(net_sales) as avg_order_value,
        sum(return_count) as total_returns,
        -- Window function: Rank products by revenue within each segment
        rank() over (
            partition by customer_segment, primary_product_category
            order by sum(net_sales) desc
        ) as revenue_rank_in_category,
        -- Window function: Calculate percentage of segment revenue
        (sum(net_sales) * 100.0 / sum(sum(net_sales)) over (partition by customer_segment)) as pct_of_segment_revenue,
        -- Window function: Calculate cumulative revenue percentage (Pareto analysis)
        sum(sum(net_sales)) over (
            partition by customer_segment
            order by sum(net_sales) desc
            rows between unbounded preceding and current row
        ) * 100.0 / sum(sum(net_sales)) over (partition by customer_segment) as cumulative_revenue_pct
    from enriched_orders
    where primary_product_id is not null
    group by 1, 2, 3, 4, 5
),

-- Step 5: Customer lifetime value analysis by product category
customer_ltv_by_category as (
    select
        customer_id,
        customer_segment,
        loyalty_tier,
        primary_product_category,
        count(distinct order_id) as orders_in_category,
        sum(net_sales) as category_lifetime_value,
        max(order_date) as last_order_date,
        min(order_date) as first_order_date,
        -- Calculate days between first and last order
        -- Note: Warehouse-specific syntax:
        -- BigQuery: date_diff(max(order_date), min(order_date), day)
        -- Postgres: (max(order_date) - min(order_date))::integer
        -- Snowflake/Redshift: datediff('day', min(order_date), max(order_date))
        datediff('day', min(order_date), max(order_date)) as customer_tenure_days,
        -- Window function: Rank customers by LTV within segment and category
        rank() over (
            partition by customer_segment, primary_product_category
            order by sum(net_sales) desc
        ) as ltv_rank_in_segment_category
    from enriched_orders
    where primary_product_category is not null
    group by 1, 2, 3, 4
),

-- Step 6: Aggregate LTV statistics by segment and category
ltv_statistics as (
    select
        customer_segment,
        primary_product_category,
        count(distinct customer_id) as total_customers,
        avg(category_lifetime_value) as avg_category_ltv,
        -- Percentile calculations (warehouse-specific syntax):
        -- Postgres/Redshift: percentile_cont(0.5) within group (order by ...)
        -- BigQuery: percentile_cont(category_lifetime_value, 0.5) over ()
        -- Snowflake: percentile_cont(0.5) within group (order by ...)
        percentile_cont(0.5) within group (order by category_lifetime_value) as median_category_ltv,
        percentile_cont(0.75) within group (order by category_lifetime_value) as p75_category_ltv,
        percentile_cont(0.90) within group (order by category_lifetime_value) as p90_category_ltv,
        percentile_cont(0.95) within group (order by category_lifetime_value) as p95_category_ltv,
        max(category_lifetime_value) as max_category_ltv,
        avg(orders_in_category) as avg_orders_per_customer,
        avg(customer_tenure_days) as avg_tenure_days
    from customer_ltv_by_category
    group by 1, 2
),

-- Step 7: Identify high-value opportunities (VIP customers, high-revenue products)
high_value_opportunities as (
    select
        customer_segment,
        loyalty_tier,
        primary_product_category,
        primary_product_name,
        count(distinct customer_id) as vip_customer_count,
        sum(net_sales) as revenue_from_vip,
        avg(net_sales) as avg_vip_order_value,
        -- Identify products with high VIP engagement
        case 
            when count(distinct customer_id) >= 3 and avg(net_sales) >= 100
            then 'High Opportunity'
            when count(distinct customer_id) >= 2 and avg(net_sales) >= 50
            then 'Medium Opportunity'
            else 'Low Opportunity'
        end as opportunity_tier
    from enriched_orders
    where loyalty_tier = 'VIP'
        and primary_product_id is not null
    group by 1, 2, 3, 4
)

-- Final output: Comprehensive analysis combining all insights
select
    -- Segment and product identifiers
    st.order_month,
    st.customer_segment,
    st.loyalty_tier,
    st.primary_product_category,
    
    -- Revenue metrics
    st.total_net_sales,
    st.total_gross_sales,
    st.segment_running_total_revenue,
    st.mom_growth_rate_pct,
    
    -- Order metrics
    st.order_count,
    st.unique_customers,
    st.avg_order_value,
    
    -- Return metrics
    st.total_returns,
    st.return_rate_pct,
    st.refund_rate_pct,
    
    -- Product performance metrics
    psp.primary_product_id,
    psp.primary_product_name,
    psp.revenue_rank_in_category,
    psp.pct_of_segment_revenue,
    psp.cumulative_revenue_pct,
    
    -- LTV statistics
    ltv.avg_category_ltv,
    ltv.median_category_ltv,
    ltv.p90_category_ltv,
    ltv.avg_orders_per_customer,
    
    -- High-value opportunities
    hvo.opportunity_tier,
    hvo.revenue_from_vip,
    hvo.vip_customer_count,
    
    -- Business insights flags
    case 
        when st.mom_growth_rate_pct > 10 then 'Rapid Growth'
        when st.mom_growth_rate_pct > 0 then 'Growing'
        when st.mom_growth_rate_pct < -10 then 'Declining'
        else 'Stable'
    end as growth_status,
    
    case 
        when psp.cumulative_revenue_pct <= 80 then 'Top 80% Product'
        else 'Long Tail Product'
    end as pareto_classification,
    
    case 
        when st.return_rate_pct > 15 then 'High Return Risk'
        when st.return_rate_pct > 5 then 'Moderate Return Risk'
        else 'Low Return Risk'
    end as return_risk_level

from segment_trends st
left join product_segment_performance psp
    on st.customer_segment = psp.customer_segment
    and st.loyalty_tier = psp.loyalty_tier
    and st.primary_product_category = psp.primary_product_category
    and psp.revenue_rank_in_category <= 5  -- Top 5 products per category
left join ltv_statistics ltv
    on st.customer_segment = ltv.customer_segment
    and st.primary_product_category = ltv.primary_product_category
left join high_value_opportunities hvo
    on st.customer_segment = hvo.customer_segment
    and st.loyalty_tier = hvo.loyalty_tier
    and st.primary_product_category = hvo.primary_product_category
    and psp.primary_product_id = hvo.primary_product_id

where st.order_month >= date_trunc('month', current_date - interval '6 months')  -- Last 6 months
    -- Note: Warehouse-specific date arithmetic:
    -- BigQuery: date_sub(current_date, interval 6 month)
    -- Postgres: current_date - interval '6 months'
    -- Snowflake/Redshift: dateadd(month, -6, current_date)

order by 
    st.order_month desc,
    st.customer_segment,
    st.total_net_sales desc,
    psp.revenue_rank_in_category

