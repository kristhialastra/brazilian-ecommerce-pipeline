-- stg_customers.sql
-- Customer registry. Note: customer_id is per-order, customer_unique_id is per person.

select
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state

from {{ source('olist_raw', 'customers') }}
