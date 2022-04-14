with customers as (

    select * from {{ ref('stg_customers')}}

),

orders as (

    select * from {{ ref('stg_orders') }}

),

customer_orders as (

    select
        customer_id,

        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders

    from orders

    group by 1

),

customer_ltv as
(
    select 
      orders.customer_id,
      sum(payments.amount / 100.0) AS lifetime_value
    from {{ref('stg_payments')}} AS payments
    inner join orders
    on payments.orderid   = orders.order_id
    where payments.status = 'success'
    group by 1
)
,
final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders,
        coalesce(customer_ltv.lifetime_value, 0)      as lifetime_value,
        CURRENT_TIMESTAMP()                           as created_ts
    from customers

    left join customer_orders using (customer_id)
    left join customer_ltv using (customer_id)

)

select * from final