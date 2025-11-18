{{ config(materialized='view') }}

with source as (

    select *
    from {{ ref('raw_products') }}

),
renamed as (

    select
        cast(product_id as integer) as product_id,
        product_name,
        upper(category) as category,
        cast(supplier_id as integer) as supplier_id,
        cast(cost as {{ dbt.type_numeric() }}) as unit_cost
    from source

)

select *
from renamed;

