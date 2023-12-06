with 

source as (

    select * from {{ source('jaffle_shop', 'jaffle_shop_customers') }}

),

staged as (

    select
        id as customer_id,
        first_name,
        last_name
    from source

)

select * from staged