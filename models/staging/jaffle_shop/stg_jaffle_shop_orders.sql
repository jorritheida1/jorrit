with 

source as (

    select * from {{ source("jaffle_shop_2", "jaffle_shop_orders")}}

),

transformed as (  -- ranks each order by its orders date for each user, including new column (user_order_seq) that represents the order old to new 

     select
        id as order_id,
        user_id as customer_id,
        order_date,
        status as order_status,
        row_number() over (
            partition by user_id 
            order by order_date, id
        ) as user_order_seq

    from source

)

select * from transformed