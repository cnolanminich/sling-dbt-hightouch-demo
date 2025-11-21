{{
    config(
        materialized='view'
    )
}}

-- Staging model for customer data
-- Depends on raw.customers from Sling ingestion

select
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    created_at,
    updated_at
from {{ source('raw', 'customers') }}
where customer_id is not null
