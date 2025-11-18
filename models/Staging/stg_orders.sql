{{ config(materialized='view') }}

with source as (

    select *
    from {{ ref('raw_orders') }}

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
        cast(total_amount as {{ dbt.type_numeric() }}) as total_amount
    from source

)

select *
from renamed;

