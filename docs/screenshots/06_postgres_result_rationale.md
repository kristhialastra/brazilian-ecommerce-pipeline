Based on what we know about the Olist dataset:
- 99,441 orders
- 112,650 order items
- 3,095 sellers
- 32,951 products

For each table:
- fact_order_items: 112,650 rows (one row per order item - order_id + order_item_id grain)
- dim_customers: ~96,096 rows (deduplicated on customer_unique_id from 99,441 customers)
- dim_products: 32,951 rows (one per product)
- dim_sellers: 3,095 rows (one per seller)
- dim_date: a date spine spanning 2016 through 2018, which gives us roughly 1,096 days total (366 days in the leap year 2016, plus 365 each for 2017 and 2018)
- dim_customers: the raw table has 99,441 rows, but after deduplicating on customer_unique_id I'm getting around 96,096 unique customers
