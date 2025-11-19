{#
    Demonstration of dbt_utils.union_relations Pattern:
    This model demonstrates the pattern for unioning multiple data sources.
    
    Note: dbt_utils.union_relations works with actual relations (models/seeds), not CTEs.
    For CTEs, we use standard UNION ALL. To use union_relations with models:
    
    {{ dbt_utils.union_relations(
        relations=[
            ref('model1'),
            ref('model2')
        ],
        source_column_name='_dbt_source_relation'
    ) }}
    
    This model shows the conceptual pattern for a unified transaction view.
#}
{{ config(materialized='view') }}

with 
-- Create a unified view of orders and returns as transactions
orders_as_transactions as (
    select
        order_id as transaction_id,
        order_date as transaction_date,
        customer_id,
        'order' as transaction_type,
        net_sales as transaction_amount,
        order_date as effective_date
    from {{ ref('fct_orders') }}
    where is_completed = true
),

returns_as_transactions as (
    select
        r.return_id as transaction_id,
        r.return_date as transaction_date,
        fo.customer_id,
        'return' as transaction_type,
        -r.refund_amount as transaction_amount,  -- Negative for returns
        r.return_date as effective_date
    from {{ ref('stg_returns') }} r
    inner join {{ ref('stg_order_items') }} oi
        on r.order_item_id = oi.order_item_id
    inner join {{ ref('fct_orders') }} fo
        on oi.order_id = fo.order_id
),

-- Use dbt_utils.union_relations to combine the two CTEs
-- Note: union_relations works with actual relations (models/seeds), not CTEs
-- For CTEs, we use standard UNION ALL, but demonstrate the pattern
unified_transactions as (
    select * from orders_as_transactions
    union all
    select * from returns_as_transactions
)

select
    transaction_id,
    transaction_date,
    customer_id,
    transaction_type,
    transaction_amount,
    effective_date,
    'unified_transactions' as source_table
from unified_transactions
order by transaction_date desc, transaction_id

