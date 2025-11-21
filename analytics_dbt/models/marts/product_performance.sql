{{
    config(
        materialized='table'
    )
}}

-- Product performance metrics
-- This would typically join with order_items table, simplified for demo

select
    product_id,
    product_name,
    category,
    price,
    -- In a real scenario, we'd join with order_items to get actual sales
    -- For demo, we're showing the structure
    created_at,
    updated_at
from {{ ref('stg_products') }}
