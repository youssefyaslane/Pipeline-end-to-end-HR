select
    e.emp_id                                                as employee_sk,
    e.dept_id                                               as department_fk,
    e.position_id                                           as position_fk,
    e.manager_id                                            as manager_fk,
    e.perf_score_id                                         as performance_fk,
    e.emp_status_id                                         as employment_status_fk,
    e.marital_status_id                                     as marital_status_fk,
    rs.source_sk                                            as recruitment_source_fk,
    to_char(e.date_of_birth, 'YYYYMMDD')::int               as date_of_birth_fk,
    to_char(e.date_of_hire, 'YYYYMMDD')::int                as date_of_hire_fk,
    to_char(e.date_of_termination, 'YYYYMMDD')::int         as date_of_termination_fk,
    to_char(e.last_performance_review_date, 'YYYYMMDD')::int as last_performance_review_date_fk,
    e.salary,
    e.engagement_survey,
    e.emp_satisfaction,
    e.special_projects_count,
    e.days_late_last_30,
    e.absences,
    e.is_terminated
from {{ ref('stg_employees') }} e
left join {{ ref('dim_recruitment_source') }} rs
    on e.recruitment_source = rs.recruitment_source
