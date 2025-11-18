{{ config(materialized='view') }}

with source as (

    select *
    from {{ ref('raw_users') }}

),
renamed as (

    select
        cast(user_id as integer) as user_id,
        lower(email) as email,
        cast(signup_date as date) as signup_date,
        upper(country) as country,
        upper(customer_segment) as customer_segment
    from source

)

select *
from renamed;

