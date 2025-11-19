{{ config(materialized='view') }}

with source as (

    select *
    from {{ source('DEV', 'users') }}

),
renamed as (

    select
        cast(user_id as integer) as user_id,
        lower(email) as email,
        cast(signup_date as date) as signup_date,
        upper(country) as country,
        upper(customer_segment) as customer_segment,
        cast(ingested_at as timestamp) as ingested_at
    from source

)

select *
from renamed

