-- stg_sellers.sql
-- Seller registry with location information.

select
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state

from {{ source('olist_raw', 'sellers') }}
