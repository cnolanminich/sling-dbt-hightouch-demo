{{
    config(
        materialized='view'
    )
}}

-- Staging model for product data
-- Depends on raw.products from Sling ingestion

select
    product_id,
    product_name,
    category,
    price,
    created_at,
    updated_at
from {{ source('raw', 'products') }}
where product_id is not null
