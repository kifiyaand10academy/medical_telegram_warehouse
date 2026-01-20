-- models/marts/dim_dates.sql
with date_range as (
    select 
        min(message_date)::date as min_date,
        max(message_date)::date as max_date
    from "neondb"."public"."stg_telegram_messages"
),
date_series as (
    select generate_series(
        (select min_date - 30 from date_range),   -- 30 days before earliest message
        (select max_date + 30 from date_range),   -- 30 days after latest message
        '1 day'::interval
    )::date as full_date
)
select
    to_char(full_date, 'YYYYMMDD')::int as date_key,
    full_date,
    extract(isodow from full_date)::int as day_of_week,
    trim(to_char(full_date, 'Day')) as day_name,
    extract(week from full_date)::int as week_of_year,
    extract(month from full_date)::int as month,
    trim(to_char(full_date, 'Month')) as month_name,
    extract(quarter from full_date)::int as quarter,
    extract(year from full_date)::int as year,
    case when extract(isodow from full_date) in (6,7) then true else false end as is_weekend
from date_series