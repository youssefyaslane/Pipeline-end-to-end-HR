select
    emp_id as employee_sk,
    employee_name,
    sex,
    marital_desc,
    citizen_desc,
    hispanic_latino,
    race_desc,
    state,
    zip
from {{ ref('stg_employees') }}
