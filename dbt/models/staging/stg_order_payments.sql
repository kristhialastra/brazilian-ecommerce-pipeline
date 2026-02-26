-- stg_order_payments.sql
-- One row per payment attempt. An order can be split across multiple payment methods.

select
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value::numeric(10, 2)   as payment_value

from {{ source('olist_raw', 'order_payments') }}
