# Staging Models

The staging layer is the first transformation layer in the dbt pipeline. Each model maps 1:1 to a raw source table in `olist_raw`. No joins happen here.

All models are materialized as **views** in the `olist_staging` schema.

---

## Purpose

Staging models do three things only:

1. **Cast types** — raw tables land as text; staging casts to `timestamp`, `numeric`, etc.
2. **Rename columns** — removes noise (e.g., strips the `geolocation_` prefix), fixes typos
3. **Surface the raw source** — every model references its source via `{{ source('olist_raw', '<table>') }}`

No business logic, no deduplication, no joins. That happens in `marts/`.

---

## Models

| Model | Source Table | Key Transformations |
|-------|-------------|---------------------|
| `stg_orders` | `orders` | Casts 5 timestamp columns from text |
| `stg_order_items` | `order_items` | Casts `price`, `freight_value` to `numeric(10,2)`; casts `shipping_limit_date` to timestamp |
| `stg_order_payments` | `order_payments` | Casts `payment_value` to `numeric(10,2)` |
| `stg_order_reviews` | `order_reviews` | Casts `review_creation_date`, `review_answer_timestamp` to timestamp |
| `stg_customers` | `customers` | No casts needed — passes through all 5 columns as-is |
| `stg_sellers` | `sellers` | No casts needed — passes through all 4 columns as-is |
| `stg_products` | `products` | Fixes two raw typos: `product_name_lenght` → `product_name_length`, `product_description_lenght` → `product_description_length` |
| `stg_geolocation` | `geolocation` | Strips redundant `geolocation_` prefix from all 5 column names |
| `stg_product_category_translation` | `product_category_name_translation` | No transforms — maps Portuguese category names to English |

---

## Source Reference

All models use the `{{ source() }}` macro, not `{{ ref() }}`. The source contract is declared in [`../sources.yml`](../sources.yml).

```sql
-- Example from stg_orders.sql
from {{ source('olist_raw', 'orders') }}
```

`{{ ref() }}` is used in `marts/` to reference these staging models:

```sql
-- Example from marts/dim_sellers.sql
from {{ ref('stg_sellers') }}
```

---

## Notes

- **`stg_customers`**: `customer_id` is per-order (a new ID is created for each order). `customer_unique_id` is per person. `dim_customers` deduplicates on `customer_unique_id`.
- **`stg_order_payments`**: An order can have multiple payment rows (split payments). Aggregation to order level happens in `fact_order_items`, not here.
- **`stg_order_reviews`**: Multiple reviews can exist per order. Deduplication to one review per order happens in `fact_order_items`.
- **`stg_geolocation`**: Not used in any mart model. The raw table has 1M+ rows with multiple coordinates per zip code — not suitable as a clean dimension without further aggregation.
- **`stg_product_category_translation`**: The source CSV has a UTF-8 BOM on the first column. If you see a column named `\ufeffproduct_category_name` in the raw table, that is the cause.
