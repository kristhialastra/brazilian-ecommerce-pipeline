-- stg_product_category_translation.sql
-- Portuguese to English category name mapping.
-- Note: the source CSV has a UTF-8 BOM character which pandas may preserve in the
-- first column name. If this model errors, inspect the raw column name with:
--   SELECT column_name FROM information_schema.columns
--   WHERE table_schema = 'olist_raw' AND table_name = 'product_category_name_translation';

select
    product_category_name,
    product_category_name_english

from {{ source('olist_raw', 'product_category_name_translation') }}
