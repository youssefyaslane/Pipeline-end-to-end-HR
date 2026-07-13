with date_bounds as (
    select
        min(least(date_of_birth, date_of_hire, date_of_termination, last_performance_review_date)) as min_date,
        max(greatest(date_of_birth, date_of_hire, date_of_termination, last_performance_review_date)) as max_date
    from {{ ref('stg_employees') }}
),

date_spine as (
    select generate_series(
        (select min_date from date_bounds),
        (select max_date from date_bounds),
        interval '1 day'
    )::date as date_day
)

select
    to_char(date_day, 'YYYYMMDD')::int  as date_sk,
    date_day,
    extract(year from date_day)::int    as year,
    extract(month from date_day)::int   as month,
    extract(day from date_day)::int     as day,
    extract(quarter from date_day)::int as quarter,
    trim(to_char(date_day, 'Day'))      as day_name,
    trim(to_char(date_day, 'Month'))    as month_name
from date_spine
