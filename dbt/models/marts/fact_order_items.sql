-- fact_order_items.sql
-- Grain: one row per order item (order_id + order_item_id).
--
-- Measures:
--   item_price, freight_value        — item-level, fully additive
--   order_payment_value              — order-level total; use at order grain only
--   days_to_deliver, delay_days      — order-level; do not SUM across items
--
-- Foreign keys to dim_date use YYYYMMDD integer format.
-- order_delivered_date_key is NULL for undelivered orders (~3% of rows).

with items as (

    select * from {{ ref('stg_order_items') }}

),

orders as (

    select * from {{ ref('stg_orders') }}

),

customers as (

    select * from {{ ref('stg_customers') }}

),

payments_agg as (

    -- Collapse multiple payment rows per order into a single total + primary type.
    -- primary_payment_type = the payment method used for the first sequential charge.
    select
        order_id,
        sum(payment_value)                                              as order_payment_value,
        max(case when payment_sequential = 1 then payment_type end)    as primary_payment_type
    from {{ ref('stg_order_payments') }}
    group by order_id

),

reviews_deduped as (

    -- A small number of orders have more than one review row.
    -- We keep only the most recently created review per order.
    select distinct on (order_id)
        order_id,
        review_score
    from {{ ref('stg_order_reviews') }}
    order by order_id, review_creation_date desc

),

customer_keys as (

    select * from {{ ref('dim_customers') }}

),

product_keys as (

    select * from {{ ref('dim_products') }}

),

seller_keys as (

    select * from {{ ref('dim_sellers') }}

)

select

    -- -------------------------------------------------------------------------
    -- Foreign keys
    -- -------------------------------------------------------------------------
    ck.customer_key,
    pk.product_key,
    sk.seller_key,

    to_char(o.order_purchase_timestamp::date,       'YYYYMMDD')::int    as order_purchase_date_key,
    to_char(o.order_estimated_delivery_date::date,  'YYYYMMDD')::int    as order_estimated_delivery_date_key,

    -- NULL when the order has not been delivered (canceled, processing, etc.)
    to_char(o.order_delivered_customer_date::date,  'YYYYMMDD')::int    as order_delivered_date_key,

    -- -------------------------------------------------------------------------
    -- Degenerate dimensions (attributes that live in the fact, no separate dim)
    -- -------------------------------------------------------------------------
    i.order_id,
    i.order_item_id,
    o.order_status,
    pmt.primary_payment_type,

    -- -------------------------------------------------------------------------
    -- Order-level attribute (repeated for every item in the same order)
    -- -------------------------------------------------------------------------
    rev.review_score,

    -- -------------------------------------------------------------------------
    -- Item-level measures — safe to SUM across any grain
    -- -------------------------------------------------------------------------
    i.price                                                             as item_price,
    i.freight_value,

    -- -------------------------------------------------------------------------
    -- Order-level measures — aggregate at ORDER grain, not item grain
    -- -------------------------------------------------------------------------
    pmt.order_payment_value,

    -- -------------------------------------------------------------------------
    -- Delivery metrics (order-level; NULL when order is not yet delivered)
    -- -------------------------------------------------------------------------
    case
        when o.order_delivered_customer_date is not null
        then extract(day from
                 date_trunc('day', o.order_delivered_customer_date)
               - date_trunc('day', o.order_purchase_timestamp)
             )::int
    end                                                                 as days_to_deliver,

    case
        when o.order_delivered_customer_date is not null
        then extract(day from
                 date_trunc('day', o.order_delivered_customer_date)
               - date_trunc('day', o.order_estimated_delivery_date)
             )::int
    end                                                                 as delivery_delay_days,

    case
        when o.order_delivered_customer_date is not null
        then date_trunc('day', o.order_delivered_customer_date)
             <= date_trunc('day', o.order_estimated_delivery_date)
    end                                                                 as is_delivered_on_time

from items i

inner join orders o
    on i.order_id = o.order_id

-- Resolve customer_id → customer_unique_id → surrogate key
inner join customers c
    on o.customer_id = c.customer_id
inner join customer_keys ck
    on c.customer_unique_id = ck.customer_unique_id

inner join product_keys pk
    on i.product_id = pk.product_id

inner join seller_keys sk
    on i.seller_id = sk.seller_id

left join payments_agg pmt
    on i.order_id = pmt.order_id

left join reviews_deduped rev
    on i.order_id = rev.order_id
