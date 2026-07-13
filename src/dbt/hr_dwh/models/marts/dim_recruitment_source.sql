select
    row_number() over (order by recruitment_source) as source_sk,
    recruitment_source
from (
    select distinct recruitment_source
    from {{ ref('stg_employees') }}
) as distinct_sources
