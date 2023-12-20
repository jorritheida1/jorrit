with

orders as (

    select * from {{ ref('int_orders') }}

),

customers as (

    select * from {{ ref('stg_jaffle_shop_customers') }}

),

-- Add a new CTE to aggregate order_ids per customer_id (indicates that the code tries to use a window function with the DISTINCT keyword, which is currently not supported in Databricks. That's why we need to work around it and create a CTE first)
-- (we created a separate CTE called order_ids_per_customer to aggregate the order_ids for each customer. This CTE groups the orders by customer_id and collects the distinct order_ids using the collect_list function. 
-- Then, in the final CTE, we join this order_ids_per_customer CTE with the previous CTE using the customer_id column.)
order_ids_per_customer as (
    select
        customer_id,
        collect_list(distinct order_id) as customer_order_ids
    from orders
    group by customer_id
),

------
customer_orders as (

    select

        orders.*,
        customers.name,
        customers.surname,
        customers.givenname,

        --- customer level aggregations
        min(orders.order_date) over(
            partition by orders.customer_id
        ) as customer_first_order_date,

        min(orders.valid_order_date) over(
            partition by orders.customer_id
        ) as customer_first_non_returned_order_date,

        max(orders.valid_order_date) over(
            partition by orders.customer_id
        ) as customer_most_recent_non_returned_order_date,

        count(*) over(
            partition by orders.customer_id
        ) as customer_order_count,

        sum(nvl2(orders.valid_order_date, 1, 0)) over(
            partition by orders.customer_id
        ) as customer_non_returned_order_count,

        sum(nvl2(orders.valid_order_date, orders.order_value_dollars, 0)) over(
            partition by orders.customer_id
        ) as customer_total_lifetime_value
    
    from orders
    inner join customers
        on orders.customer_id = customers.customer_id

),

add_avg_order_values as (

    select
        *,
        customer_total_lifetime_value / customer_non_returned_order_count 
        as customer_avg_non_returned_order_value

    from customer_orders
),

    -- Final CTEs 
final as (
    select
        add_avg_order_values.order_id,
        add_avg_order_values.customer_id,
        add_avg_order_values.surname,
        add_avg_order_values.givenname,
        add_avg_order_values.customer_first_order_date as first_order_date,
        add_avg_order_values.customer_order_count as order_count,
        add_avg_order_values.customer_total_lifetime_value as total_lifetime_value,
        add_avg_order_values.order_value_dollars,
        add_avg_order_values.order_status,
        add_avg_order_values.payment_status,
        order_ids_per_customer.customer_order_ids as customer_order_ids
    from add_avg_order_values
    -- Join order_ids_per_customer to get customer_order_ids
    inner join order_ids_per_customer
        on add_avg_order_values.customer_id = order_ids_per_customer.customer_id
)

-- Simple Select Statement
select * from final
