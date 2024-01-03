WITH paid_orders AS (
    SELECT Orders.ID as order_id,
        Orders.USER_ID AS customer_id,
        Orders.ORDER_DATE AS order_placed_at,
        Orders.STATUS AS order_status,
        p.total_amount_paid,
        p.payment_finalized_date,
        C.FIRST_NAME as customer_first_name,
        C.LAST_NAME as customer_last_name
    FROM {{ source('jaffle_shop_2', 'jaffle_shop_orders') }} AS Orders
    LEFT JOIN (
        SELECT ORDERID as order_id,
            MAX(CREATED) as payment_finalized_date,
            SUM(AMOUNT) / 100.0 as total_amount_paid
        FROM {{ source('stripe_2', 'stripe_payments') }}
        WHERE STATUS <> 'fail'
        GROUP BY 1
    ) p ON orders.ID = p.order_id
    LEFT JOIN {{ source('jaffle_shop_2', 'jaffle_shop_customers') }} C ON orders.USER_ID = C.ID
),

customer_orders AS (
    SELECT C.ID as customer_id,
        MIN(ORDER_DATE) as first_order_date,
        MAX(ORDER_DATE) as most_recent_order_date,
        COUNT(ORDERS.ID) AS number_of_orders
    FROM {{ source('jaffle_shop_2', 'jaffle_shop_customers') }} C
    LEFT JOIN {{ source('jaffle_shop_2', 'jaffle_shop_orders') }} AS Orders ON orders.USER_ID = C.ID
    GROUP BY 1
)

SELECT
    p.*,
    ROW_NUMBER() OVER (ORDER BY p.order_id) as transaction_seq,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY p.order_id) as customer_sales_seq,
    CASE
        WHEN c.first_order_date = p.order_placed_at THEN 'new'
        ELSE 'return'
    END as nvsr,
    x.clv_bad as customer_lifetime_value,
    c.first_order_date as fdos
FROM paid_orders p
LEFT JOIN customer_orders as c USING (customer_id)
LEFT OUTER JOIN (
    SELECT
        p.order_id,
        SUM(t2.total_amount_paid) as clv_bad
    FROM paid_orders p
    LEFT JOIN paid_orders t2 ON p.customer_id = t2.customer_id AND p.order_id >= t2.order_id
    GROUP BY 1
    ORDER BY p.order_id
) x ON x.order_id = p.order_id
ORDER BY order_id
