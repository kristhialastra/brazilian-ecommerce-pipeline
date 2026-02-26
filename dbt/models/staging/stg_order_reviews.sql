-- stg_order_reviews.sql
-- Customer reviews submitted after order delivery. Casts date columns to timestamp.

select
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date::timestamp     as review_creation_date,
    review_answer_timestamp::timestamp  as review_answer_timestamp

from {{ source('olist_raw', 'order_reviews') }}
