{{ config(materialized='view') }}

with source as (

    select *
    from {{ source('raw_ecommerce', 'returns') }}

),
renamed as (

    select
        cast(return_id as integer) as return_id,
        cast(order_item_id as integer) as order_item_id,
        cast(return_date as date) as return_date,
        lower(return_reason) as return_reason,
        cast(refund_amount as {{ dbt.type_numeric() }}) as refund_amount,
        cast(ingested_at as timestamp) as ingested_at
    from source

)

select *
from renamed;

