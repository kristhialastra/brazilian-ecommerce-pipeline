-- stg_products.sql
-- Product catalog. Fixes two typos present in the raw source data:
--   product_name_lenght        -> product_name_length
--   product_description_lenght -> product_description_length

select
    product_id,
    product_category_name,
    product_name_lenght         as product_name_length,
    product_description_lenght  as product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm

from {{ source('olist_raw', 'products') }}
