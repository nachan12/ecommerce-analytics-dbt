{{ config(materialized='view') }}

with source as (

    select *
    from {{ ref('raw_order_items') }}

),
renamed as (

    select
        cast(order_item_id as integer) as order_item_id,
        cast(order_id as integer) as order_id,
        cast(product_id as integer) as product_id,
        cast(quantity as integer) as quantity,
        cast(price as {{ dbt.type_numeric() }}) as unit_price,
        cast(quantity as integer) * cast(price as {{ dbt.type_numeric() }}) as gross_item_sales
    from source

)

select *
from renamed;

