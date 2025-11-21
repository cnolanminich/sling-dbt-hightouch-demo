{{
    config(
        materialized='table'
    )
}}

-- Customer lifetime value calculation
-- Aggregates order data by customer

with customer_orders as (
    select
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        count(o.order_id) as total_orders,
        sum(o.total_amount) as lifetime_value,
        max(o.order_date) as last_order_date,
        min(o.order_date) as first_order_date
    from {{ ref('stg_customers') }} c
    left join {{ ref('stg_orders') }} o on c.customer_id = o.customer_id
    group by 1, 2, 3, 4
)

select
    *,
    case
        when lifetime_value >= 10000 then 'VIP'
        when lifetime_value >= 5000 then 'Premium'
        when lifetime_value >= 1000 then 'Standard'
        else 'New'
    end as customer_tier
from customer_orders
