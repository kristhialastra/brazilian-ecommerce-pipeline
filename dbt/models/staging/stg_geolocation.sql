-- stg_geolocation.sql
-- Zip code to coordinates mapping. Removes the repetitive 'geolocation_' prefix
-- from every column name for cleaner downstream references.

select
    geolocation_zip_code_prefix as zip_code_prefix,
    geolocation_lat             as latitude,
    geolocation_lng             as longitude,
    geolocation_city            as city,
    geolocation_state           as state

from {{ source('olist_raw', 'geolocation') }}
