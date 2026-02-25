-- dim_products.sql
-- One row per product. Joins the Portuguese category name to its English translation.
--
-- Design note: The translation table covers 71 categories. Products with a category
-- not present in the translation table fall back to the Portuguese name via COALESCE
-- so no product is ever left with a NULL category label.

with products as (

    select * from {{ ref('stg_products') }}

),

translations as (

    select * from {{ ref('stg_product_category_translation') }}

)

select
    row_number() over (order by p.product_id)                               as product_key,
    p.product_id,
    coalesce(t.product_category_name_english, p.product_category_name)      as product_category_name_english,
    p.product_name_length,
    p.product_description_length,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm
from products p
left join translations t
    on p.product_category_name = t.product_category_name
