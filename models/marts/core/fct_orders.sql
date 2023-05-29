with payments as (
    select * from {{ ref('stg_payments') }}
),
orders as (
    select * from {{ ref('stg_orders') }}
),
order_payments as (
    select
        order_id,
        sum(case when status='success' then amount end) as amount
    from payments
    group by 1
),

final as (
    select 
        p.order_id,
        o.customer_id,
        o.order_date,
        p.amount
    from 
        order_payments p join orders o using(order_id)
)

select * from final

