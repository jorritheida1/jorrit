with 

source as (

    select * from {{ source("jaffle_shop_2", "jaffle_shop_customers")}}

),

transformed as (   -- creates a new column/table that concattenates first and last name into a new column called from base_customers
    
    select 

        id as customer_id,
        last_name as surname,
        first_name as givenname,
        first_name || ' ' || last_name as name, 
        * 
    from source
    
)

select * from transformed