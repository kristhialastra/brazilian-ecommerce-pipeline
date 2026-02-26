-- stg_orders.sql
-- One row per order. Casts all timestamp columns from text to proper timestamp type.

select
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp::timestamp         as order_purchase_timestamp,
    order_approved_at::timestamp                as order_approved_at,
    order_delivered_carrier_date::timestamp     as order_delivered_carrier_date,
    order_delivered_customer_date::timestamp    as order_delivered_customer_date,
    order_estimated_delivery_date::timestamp    as order_estimated_delivery_date

from {{ source('olist_raw', 'orders') }}
