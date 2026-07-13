select distinct
    marital_status_id,
    marital_desc
from {{ ref('stg_employees') }}
