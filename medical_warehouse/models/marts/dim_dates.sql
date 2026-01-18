-- Generate date dimension for last 2 years
with dates as (
    select generate_series(
        current_date - interval '2 years',
        current_date + interval '1 year',
        '1 day'::interval
    )::date as full_date
)
select
    to_char(full_date, 'YYYYMMDD')::int as date_key,
    full_date,
    extract(isodow from full_date)::int as day_of_week,
    to_char(full_date, 'Day') as day_name,
    extract(week from full_date)::int as week_of_year,
    extract(month from full_date)::int as month,
    to_char(full_date, 'Month') as month_name,
    extract(quarter from full_date)::int as quarter,
    extract(year from full_date)::int as year,
    case when extract(isodow from full_date) in (6,7) then true else false end as is_weekend
from dates