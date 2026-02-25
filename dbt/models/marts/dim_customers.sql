-- dim_customers.sql
-- One row per unique customer (person), not per order.
--
-- Design note: In the raw data, customer_id is generated fresh per order — the same
-- person placing two orders gets two different customer_ids. customer_unique_id is the
-- stable identifier for a real person. We deduplicate on it here so that
-- COUNT(DISTINCT customer_key) gives accurate unique-customer counts in analysis.

with source as (

    select * from {{ ref('stg_customers') }}

),

deduplicated as (

    -- DISTINCT ON picks one deterministic row per person.
    -- Address (city/state) is order-specific in this dataset, so we just take any row.
    select distinct on (customer_unique_id)
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state
    from source
    order by customer_unique_id

)

select
    row_number() over (order by customer_unique_id)     as customer_key,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
from deduplicated
