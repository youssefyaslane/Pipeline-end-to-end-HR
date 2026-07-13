-- Certains noms de manager (ex: "Brandon R. LeBlanc", "Michael Albert")
-- apparaissent sous deux manager_id différents dans le CSV source. On ne
-- fusionne pas ces doublons (pas de règle fiable pour choisir le "bon" id) :
-- limitation connue, acceptée pour ce projet.
-- 8 employés ont un manager_id manquant (NULL) : ils resteront sans
-- correspondance dans fact_employee plutôt que de leur assigner un id deviné.

select distinct
    manager_id,
    manager_name
from {{ ref('stg_employees') }}
where manager_id is not null
