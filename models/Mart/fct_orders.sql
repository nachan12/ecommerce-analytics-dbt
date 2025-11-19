{#
    Performance Optimization Strategy:
    - Materialization: Incremental (merge strategy) to only process new/changed orders
    - Partitioning: Recommended to partition by order_date for time-based queries
    - Clustering: Recommended to cluster by customer_id and order_date for join performance
    - Indexes: Primary key on order_id, indexes on customer_id and order_date
    
    Warehouse-specific notes:
    - BigQuery: Use partition_by={'field': 'order_date', 'data_type': 'date'} and cluster_by=['customer_id', 'order_date']
    - Snowflake: Use cluster_by=['order_date', 'customer_id'] in config
    - Redshift: Use diststyle KEY distkey customer_id and sortkey (order_date, customer_id)
    - Postgres: Create indexes on (order_id), (customer_id), (order_date)
#}
{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge',
    Snowflake_cluster_by=['order_date', 'customer_id']
   ) }}

{% set reprocess_days = var('fct_orders_reprocess_days', 14) %}

with orders as (

    select *
    from {{ ref('int_order_metrics') }}
    {% if is_incremental() %}
    where order_date >= (
        select {{ subtract_days('max(order_date)', reprocess_days) }}
        from {{ this }}
    )
    {% endif %}
),
customer_dim as (

    select
         customer_id,
        email,
        country,
        customer_segment,
        loyalty_tier,
        lifetime_revenue,
        completed_order_count,
        first_order_date,
        most_recent_order_date
    from {{ ref('dim_customers') }}

),
order_products as (

    select
        oi.order_id,
        p.product_id,
        p.product_name,
        p.category,
        sum(oi.quantity) as product_quantity,
        sum(oi.gross_item_sales) as product_sales
    from {{ ref('stg_order_items') }} oi
    join {{ ref('stg_products') }} p
        on oi.product_id = p.product_id
    group by 1, 2, 3, 4

),
primary_product as (

    select *
    from (
        select
            order_id,
            product_id,
            product_name,
            category,
            product_quantity,
            product_sales,
            row_number() over (
                partition by order_id
                order by product_quantity desc, product_sales desc, product_id
            ) as product_rank
        from order_products
    )
    where product_rank = 1

),
category_mix as (

    select
        oi.order_id,
        count(distinct p.category) as distinct_categories
    from {{ ref('stg_order_items') }} oi
    join {{ ref('stg_products') }} p
        on oi.product_id = p.product_id
    group by 1

),
final as (

    select
        o.order_id,
        o.user_id as customer_id,
        cd.email as customer_email,
        cd.country as customer_country,
        cd.customer_segment,
        cd.loyalty_tier,
        o.order_date,
        o.status,
        o.is_completed,
        o.order_total_amount,
        o.total_item_quantity,
        o.line_item_count,
        o.distinct_products,
        cm.distinct_categories,
        o.gross_item_sales,
        o.total_refund_amount,
        o.net_sales,
        o.return_count,
        pp.product_id as primary_product_id,
        pp.product_name as primary_product_name,
        pp.category as primary_product_category,
        pp.product_quantity as primary_product_quantity,
        pp.product_sales as primary_product_sales,
        cd.lifetime_revenue as customer_lifetime_revenue,
        cd.completed_order_count as customer_completed_order_count,
        cd.first_order_date as customer_first_order_date,
        cd.most_recent_order_date as customer_most_recent_order_date
    from orders o
    left join customer_dim cd
        on o.user_id = cd.customer_id
    left join primary_product pp
        on o.order_id = pp.order_id
    left join category_mix cm
        on o.order_id = cm.order_id

)

select *
from final

