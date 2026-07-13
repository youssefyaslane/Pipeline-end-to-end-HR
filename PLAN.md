# Plan — Pipeline end-to-end HR : CSV → Airflow → dbt → PostgreSQL (Docker)
### Version pédagogique — avancer étape par étape

## Context
L'utilisateur veut construire un projet data end-to-end **à but d'apprentissage / portfolio**, avec la
stack **Airflow (orchestration) + dbt (transformation) + PostgreSQL (entrepôt) dans Docker**, en partant
d'un fichier CSV source : `dataset/HRDataset_v14.csv` (311 employés, 36 colonnes — HR Dataset v14 de Kaggle).

Le CSV a été validé comme suffisant pour un schéma en étoile : il contient des **mesures**
(Salary, EngagementSurvey, EmpSatisfaction, SpecialProjectsCount, DaysLateLast30, Absences, Termd) et
plusieurs **axes descriptifs** déjà fournis en couples ID/libellé (DeptID/Department, PositionID/Position,
ManagerID/ManagerName, PerfScoreID/PerformanceScore, EmpStatusID/EmploymentStatus…).

**Contrainte pédagogique explicite : l'utilisateur veut avancer doucement, comprendre chaque brique avant
la suivante — pas tout générer d'un coup.** Le plan est donc découpé en **6 phases séquentielles**, chacune
avec ses sous-parties, son objectif d'apprentissage, les fichiers concernés, et un point de vérification
avant de passer à la phase suivante. Chaque phase doit être validée (par l'utilisateur, en testant) avant
de démarrer la suivante.

📐 **Schémas visuels de référence** (architecture pipeline + modèle en étoile) :
https://claude.ai/code/artifact/420a7adf-e911-4f42-bdf9-53245aa03dda

## Structure du projet (état actuel)

```
Pipeline_end_to_end/
├── PLAN.md
├── dataset/
│   └── HRDataset_v14.csv
└── src/
    ├── docker/       (docker-compose.yml, Dockerfile, sql/ — Phase 1 & 5)
    ├── ingestion/     (load_csv.py — Phase 2)
    ├── dags/          (hr_pipeline_dag.py — Phase 5)
    └── dbt/           (projet dbt hr_dwh/ — Phase 3 & 4)
```

## Architecture cible (vue d'ensemble, à garder en tête)

```
CSV  ──ingestion (Python)──►  raw.hr_employees   (Phase 2)
                                    │
                         dbt staging│ nettoyage/typage         (Phase 3)
                                    ▼
                              stg_employees
                                    │
                         dbt marts  │ schéma en étoile         (Phase 4)
                                    ▼
   dim_department, dim_position, dim_manager, dim_performance,
   dim_employment_status, dim_marital_status, dim_recruitment_source,
   dim_date, dim_employee   +   fact_employee

Orchestration Airflow (relie tout)                              (Phase 5)
Tests qualité dbt + vérifs finales                               (Phase 6)
```

---

## Phase 0 — Prérequis & mise à niveau environnement ✅
**Objectif d'apprentissage** : avoir un environnement Docker fonctionnel, comprendre ce que chaque outil fait avant de l'utiliser.

### 0.1 Vérifier Docker Desktop
- Vérifier `docker --version` et `docker compose version`.
- Vérifier que Docker Desktop tourne (WSL2 backend sous Windows).

### 0.2 Comprendre le rôle de chaque brique (discussion, pas de code)
- **PostgreSQL** : la base de données qui stockera à la fois les métadonnées Airflow et l'entrepôt de données HR (schémas `raw`, `staging`, `marts`).
- **Airflow** : l'orchestrateur — décide *quand* et *dans quel ordre* les étapes s'exécutent.
- **dbt** : l'outil de transformation SQL — définit *comment* les données brutes deviennent le modèle en étoile.
- ELT (Extract-Load-Transform) vs ETL : on charge le brut avant de transformer, pour garder les données brutes toujours disponibles.

### 0.3 Créer la structure de dossiers
```
Pipeline_end_to_end/
├── dataset/            (déjà présent, contient le CSV)
└── src/
    ├── docker/
    ├── ingestion/
    ├── dags/
    └── dbt/
```
**Checkpoint** : dossiers créés, Docker fonctionne (`docker run hello-world`).

---

## Phase 1 — PostgreSQL seul dans Docker (sans Airflow, sans dbt)
**Objectif d'apprentissage** : comprendre docker-compose, les volumes, les variables d'environnement, et se connecter à une base Postgres conteneurisée — sans la complexité d'Airflow par-dessus.

### 1.1 Fichier `.env`
- `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB=hr`.
- Expliquer pourquoi on isole les secrets dans `.env` plutôt qu'en dur dans le compose.

### 1.2 `src/docker/docker-compose.yml` — service Postgres uniquement
- Image `postgres:16`.
- Volume nommé pour la persistance des données (`pgdata`).
- Port mappé `5432:5432`.
- Healthcheck simple.

### 1.3 Script d'initialisation des schémas
- `src/docker/sql/00_init_schemas.sql` monté dans `/docker-entrypoint-initdb.d/`.
- `CREATE SCHEMA raw; CREATE SCHEMA staging; CREATE SCHEMA marts;`
- Expliquer le rôle de chaque schéma dans l'architecture medallion simplifiée.

### 1.4 Test de connexion
- `docker compose up -d` (depuis `src/docker/`).
- Se connecter via `psql` (ou DBeaver/pgAdmin si l'utilisateur préfère un client graphique) et vérifier que les 3 schémas existent.

**Checkpoint** : Postgres tourne, on peut s'y connecter, les schémas `raw/staging/marts` existent. **Pas encore de données dedans.**

---

## Phase 2 — Ingestion du CSV vers `raw.hr_employees`
**Objectif d'apprentissage** : comprendre comment un script Python charge un fichier plat dans une base, et repérer les problèmes de qualité de données à la source (avant même dbt).

### 2.1 Explorer le CSV "à la main"
- Ouvrir le CSV avec l'utilisateur (ou un script d'exploration rapide) pour repérer :
  - le BOM UTF-8 sur `Employee_Name`,
  - les espaces parasites (`Department`, `Sex`),
  - les formats de dates hétérogènes (`DOB` en MM/DD/YY vs `DateofHire` en M/D/YYYY),
  - les valeurs vides (`DateofTermination` pour les employés actifs).
- Ce constat guidera le nettoyage en Phase 3 (dbt) — **on ne nettoie pas encore ici**, on charge le brut tel quel.

### 2.2 Script `src/ingestion/load_csv.py`
- `pandas.read_csv(..., encoding="utf-8-sig")`.
- Connexion via SQLAlchemy à Postgres (variables d'env réutilisées).
- `to_sql("hr_employees", schema="raw", if_exists="replace")`.
- Exécuté manuellement d'abord (`python load_csv.py`), pas encore via Airflow.

### 2.3 `requirements.txt` (racine du projet)
- `pandas`, `sqlalchemy`, `psycopg2-binary`.
- Placé à la racine (pas dans `src/ingestion/`) pour regrouper aussi les futures dépendances (dbt, etc.) au même endroit.
- Environnement virtuel local ou petit conteneur dédié — au choix selon préférence de l'utilisateur.

### 2.4 Vérification
- Requête `SELECT COUNT(*) FROM raw.hr_employees;` → doit renvoyer 311.
- `SELECT * FROM raw.hr_employees LIMIT 5;` pour visualiser le résultat brut.

**Checkpoint** : les 311 lignes sont dans `raw.hr_employees`, données brutes non nettoyées (volontairement).

---

## Phase 3 — dbt : mise en place du projet + couche staging
**Objectif d'apprentissage** : comprendre l'anatomie d'un projet dbt (models, sources, profiles, tests) et écrire le premier modèle de nettoyage.

### 3.1 Installation et initialisation dbt
- `pip install dbt-postgres`.
- `dbt init hr_dwh` (dans `src/dbt/`) → explorer la structure générée (`dbt_project.yml`, `models/`).
- `profiles.yml` : configurer la connexion vers le Postgres Docker (host, port, user, password, dbname, schema target).
- `dbt debug` pour valider la connexion.

### 3.2 Déclarer la source
- `src/dbt/hr_dwh/models/staging/_sources.yml` : déclarer `source('raw', 'hr_employees')`.
- Premiers tests dbt simples sur la source : `not_null` et `unique` sur `EmpID`.
- Expliquer la différence entre une *source* (données externes à dbt) et un *model* (transformé par dbt).

### 3.3 Premier modèle : `stg_employees.sql`
- Materialization `view` (léger, recalculé à chaque requête).
- Contenu, introduit **progressivement** (pas tout d'un coup) :
  1. D'abord un simple `SELECT *` depuis la source pour valider la chaîne bout en bout.
  2. Ensuite ajouter le `TRIM()` sur les colonnes texte à espaces (`Department`, `Sex`).
  3. Ensuite le parsing/cast des dates hétérogènes en `DATE`.
  4. Enfin la normalisation des flags 0/1 en booléens (`Termd`, `MarriedID`), et renommage en snake_case.
- À chaque sous-étape : `dbt run --select stg_employees` puis vérifier le résultat en base.

### 3.4 Vérification
- `dbt run` passe.
- `dbt test` passe sur les tests de source.
- Requête manuelle sur `staging.stg_employees` pour confirmer que le nettoyage est correct (plus d'espaces, dates valides).

**Checkpoint** : `stg_employees` propre et fiable — c'est la base sur laquelle tout le modèle en étoile va s'appuyer.

---

## Phase 4 — dbt : couche marts (le schéma en étoile)
**Objectif d'apprentissage** : construire concrètement les dimensions et la table de faits, comprendre les clés de substitution (surrogate keys) et le grain d'une table de faits.

Construire **une dimension à la fois**, dans cet ordre suggéré (du plus simple au plus impliqué) :

### 4.1 Première dimension simple : `dim_department`
- `SELECT DISTINCT DeptID, Department` depuis `stg_employees`.
- Introduire ici le concept de clé de dimension (utiliser l'ID naturel si propre, sinon `dbt_utils.generate_surrogate_key`).
- Vérifier en base après `dbt run --select dim_department`.

### 4.2 Répéter le pattern pour les dimensions suivantes (une par une, avec vérification à chaque fois)
- `dim_position` (PositionID/Position)
- `dim_manager` (ManagerID/ManagerName)
- `dim_performance` (PerfScoreID/PerformanceScore)
- `dim_employment_status` (EmpStatusID/EmploymentStatus)
- `dim_marital_status`, `dim_recruitment_source` (dimensions secondaires, plus rapides une fois le pattern acquis)

### 4.3 Dimension date (la plus technique)
- `dim_date.sql` : générer un calendrier couvrant la plage DOB / DateofHire / DateofTermination.
- Introduire `dbt_utils.date_spine` (ou génération SQL manuelle si on veut éviter une dépendance externe au début).
- Discuter du rôle multiple de cette dimension (plusieurs FKs dans le fait pointent vers elle : date de naissance, date d'embauche, date de départ).

### 4.4 Dimension employé (attributs démographiques restants)
- `dim_employee.sql` : Sex, MaritalDesc, CitizenDesc, HispanicLatino, RaceDesc, State, Zip.
- Discuter brièvement SCD Type 1 vs Type 2 (on reste en Type 1 — écrasement — pour ce projet simple).

### 4.5 La table de faits : `fact_employee.sql`
- Grain explicite : **une ligne = un employé**.
- Colonnes : FKs vers chaque dimension + mesures (Salary, EngagementSurvey, EmpSatisfaction, SpecialProjectsCount, DaysLateLast30, Absences, Termd).
- Construite en dernier, une fois toutes les dimensions validées.

### 4.6 Tests de qualité sur les marts
- `_marts.yml` : `unique` + `not_null` sur les clés de chaque dimension.
- Tests `relationships` : chaque FK de `fact_employee` doit exister dans sa dimension.
- `dbt test` doit passer intégralement.

**Checkpoint** : schéma en étoile complet et testé, requêtable directement en SQL dans Postgres.

---

## Phase 5 — Orchestration Airflow
**Objectif d'apprentissage** : comprendre un DAG Airflow, les opérateurs, et comment Airflow appelle dbt — après avoir déjà tout fait tourner manuellement en Phase 2-4.

### 5.1 Ajouter Airflow au docker-compose
- Étendre `src/docker/docker-compose.yml` (Phase 1) avec les services `airflow-init`, `webserver`, `scheduler` (LocalExecutor).
- `Dockerfile` custom basé sur `apache/airflow:2.9-python3.11` avec `dbt-postgres` installé.
- Monter `src/dags/`, `src/ingestion/`, `src/dbt/`, `dataset/` en volumes.

### 5.2 Premier DAG minimal (une seule tâche)
- `src/dags/hr_pipeline_dag.py` avec **une seule tâche** `ingest_csv` (réutilise `load_csv.py` de la Phase 2).
- Lancer depuis l'UI Airflow (http://localhost:8080), observer les logs.
- Objectif : comprendre l'UI, le déclenchement manuel, la lecture des logs, avant de complexifier.

### 5.3 Ajouter les tâches dbt
- Ajouter `dbt_run` (`BashOperator` → `dbt run`).
- Ajouter `dbt_test` (`BashOperator` → `dbt test`).
- Définir les dépendances : `ingest_csv >> dbt_run >> dbt_test`.

### 5.4 Vérification
- Déclencher le DAG complet depuis l'UI, les 3 tâches passent au vert dans l'ordre.

**Checkpoint** : pipeline orchestré de bout en bout, déclenchable en un clic.

---

## Phase 6 — Validation finale & requêtes analytiques
**Objectif d'apprentissage** : clôturer le projet en interrogeant le modèle en étoile comme le ferait un analyste.

### 6.1 Vérifications de cohérence
- `raw.hr_employees` = 311 lignes.
- `marts.fact_employee` = 311 lignes.
- Jointures fait↔dimensions sans perte de lignes.

### 6.2 Requêtes analytiques de démonstration
- Salaire moyen par département.
- Effectif actif vs terminé par manager.
- Satisfaction moyenne par source de recrutement.

### 6.3 Bilan pédagogique
- Relecture rapide de chaque brique construite et de ce qu'elle a appris à l'utilisateur.

---

## Points d'attention data quality (rappel, traités en Phase 3)
- BOM UTF-8 sur `Employee_Name`.
- Espaces en fin de valeur (`Department`, `Sex`).
- Formats de dates mixtes.
- `DateofTermination` vide → NULL.
- Flags 0/1 → booléens.
- **`Zip` a perdu son zéro de tête** (`01960` → `1960`) : pandas a déduit un type numérique lors de l'ingestion Phase 2. À corriger en Phase 3 en castant `Zip` en texte (`LPAD(Zip::text, 5, '0')` ou équivalent) dans `stg_employees`. *Découvert en vérifiant `raw.hr_employees` après le chargement du 2026-07-13.*

## Hors périmètre (volontairement, pour rester simple au premier passage)
- Pas de CI/CD.
- Pas de SCD Type 2.
- Pas d'incrémental dbt (full-refresh à chaque run).
- Pas d'astronomer-cosmos pour l'intégration Airflow/dbt (BashOperator direct, plus simple à comprendre).

## Méthode de travail proposée
On avance **phase par phase**, dans l'ordre ci-dessus. À chaque phase :
1. J'explique le concept avant d'écrire le code.
2. On écrit et exécute le minimum de fichiers nécessaires à cette phase.
3. On vérifie ensemble le résultat (requête SQL, log, UI).
4. On ne passe à la phase suivante qu'une fois le checkpoint validé.

---

## État d'avancement

- [x] Phase 0.1 — Docker et Docker Compose installés, Docker Desktop démarré et fonctionnel (`docker run hello-world` OK)
- [x] Phase 0.2 — Rôles de Postgres / Airflow / dbt expliqués, ELT vs ETL vu
- [x] Phase 0.3 — Structure créée : `src/{docker,ingestion,dags,dbt}` + `dataset/`
- [x] Schémas visuels (architecture pipeline + modèle en étoile) générés et publiés
- [x] Phase 1 — PostgreSQL seul dans Docker : conteneur `hr_postgres` opérationnel, port 5432 publié, schémas `raw/staging/marts` créés
- [x] Phase 2 — Ingestion CSV : 311 lignes chargées dans `raw.hr_employees` via `src/ingestion/load_csv.py` ; `requirements.txt` à la racine. Défaut de qualité supplémentaire découvert : `Zip` a perdu son zéro de tête.
- [ ] Phase 3 à 6 — non démarrées
