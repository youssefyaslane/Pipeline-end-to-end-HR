-- position_id 13 et 23 couvrent chacun plusieurs intitulés de poste distincts
-- dans le CSV source, sans faute de frappe évidente ni majorité claire
-- (ex: id=13 -> "IT Manager - DB"/"Support"/"Infra"). On retient le libellé
-- le plus fréquent par id ; les quelques employés minoritaires sur ces ids
-- affichent donc un intitulé légèrement approximatif. Limitation connue,
-- acceptée pour ce projet plutôt que d'inventer une règle de résolution.

with position_counts as (
    select
        position_id,
        position,
        count(*) as employee_count
    from {{ ref('stg_employees') }}
    group by position_id, position
),

ranked as (
    select
        position_id,
        position,
        row_number() over (partition by position_id order by employee_count desc) as rn
    from position_counts
)

select
    position_id,
    position
from ranked
where rn = 1
