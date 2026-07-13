with performance_counts as (
    select
        perf_score_id,
        performance_score,
        count(*) as employee_count
    from {{ ref('stg_employees') }}
    group by perf_score_id, performance_score
),

ranked as (
    select
        perf_score_id,
        performance_score,
        row_number() over (partition by perf_score_id order by employee_count desc) as rn
    from performance_counts
)

select
    perf_score_id,
    performance_score
from ranked
where rn = 1
