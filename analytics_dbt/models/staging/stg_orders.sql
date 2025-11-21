{{
    config(
        materialized='view'
    )
}}

-- Staging model for order data
-- Depends on raw.orders from Sling ingestion

select
    order_id,
    customer_id,
    order_date,
    total_amount,
    status,
    created_at,
    updated_at
from {{ source('raw', 'orders') }}
where order_id is not null
