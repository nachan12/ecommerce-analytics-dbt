{#
    Performance Optimization Strategy:
    - Materialization: View for lightweight transformations
    - Rationale: Staging models are simple transformations (casting, lowercasing) that don't
      require materialization. Views keep the pipeline lightweight and ensure fresh data.
    
    For very large datasets (>100M rows), consider materializing as a table with partitioning
    by order_date for better query performance.
#}
{{ config(materialized='view') }}

with source as (

    select *
    from {{ source('raw_ecommerce', 'orders') }}

),
renamed as (

    select
        cast(order_id as integer) as order_id,
        cast(user_id as integer) as user_id,
        cast(order_date as date) as order_date,
        lower(status) as status,
        case
            when lower(status) = 'completed' then true
            else false
        end as is_completed,
        cast(total_amount as {{ dbt.type_numeric() }}) as total_amount,
        cast(ingested_at as timestamp) as ingested_at
    from source

)

select *
from renamed;

