with department_counts as (
    select
        dept_id,
        department,
        count(*) as employee_count
    from {{ ref('stg_employees') }}
    group by dept_id, department
),

ranked as (
    select
        dept_id,
        department,
        row_number() over (partition by dept_id order by employee_count desc) as rn
    from department_counts
)

select
    dept_id,
    department
from ranked
where rn = 1
