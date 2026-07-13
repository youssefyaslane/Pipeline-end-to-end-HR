with status_counts as (
    select
        emp_status_id,
        employment_status,
        count(*) as employee_count
    from {{ ref('stg_employees') }}
    group by emp_status_id, employment_status
),

ranked as (
    select
        emp_status_id,
        employment_status,
        row_number() over (partition by emp_status_id order by employee_count desc) as rn
    from status_counts
)

select
    emp_status_id,
    employment_status
from ranked
where rn = 1
