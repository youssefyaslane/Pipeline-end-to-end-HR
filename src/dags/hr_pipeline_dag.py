import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/src/ingestion")
from load_csv import load_csv_to_raw

DBT_PROJECT_DIR = "/opt/airflow/src/dbt/hr_dwh"

with DAG(
    dag_id="hr_pipeline",
    description="CSV -> Postgres raw -> dbt staging -> dbt marts",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["hr", "learning"],
) as dag:

    ingest_csv = PythonOperator(
        task_id="ingest_csv",
        python_callable=load_csv_to_raw,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test",
    )

    ingest_csv >> dbt_run >> dbt_test
