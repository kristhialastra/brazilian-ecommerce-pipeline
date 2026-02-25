-- dim_date.sql
-- One row per calendar day from 2016-01-01 to 2018-12-31 (inclusive).
-- Covers the full Olist dataset range (Sep 2016 – Oct 2018) with buffer.
-- Generated via dbt_utils.date_spine — no source table required.

with date_spine as (

    {{
        dbt_utils.date_spine(
            datepart = "day",
            start_date = "cast('2016-01-01' as date)",
            end_date   = "cast('2019-01-01' as date)"
        )
    }}

)

select
    to_char(date_day, 'YYYYMMDD')::int              as date_key,
    date_day                                         as date,
    extract(year    from date_day)::int              as year,
    extract(quarter from date_day)::int              as quarter,
    extract(month   from date_day)::int              as month,
    trim(to_char(date_day, 'Month'))                 as month_name,
    extract(week    from date_day)::int              as week_of_year,
    extract(isodow  from date_day)::int              as day_of_week,
    trim(to_char(date_day, 'Day'))                   as day_name,
    extract(isodow  from date_day) in (6, 7)         as is_weekend
from date_spine
