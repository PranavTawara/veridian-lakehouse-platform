import os
import sys
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

WORKSPACE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
PYTHON_BIN = sys.executable

default_args = {
    'owner': 'pranav',
    'depends_on_past': False,
    'start_date': datetime(2026, 10, 1),
    'email_on_failure': False,
    'retries': 1,
}

with DAG(
    dag_id='veridian_end_to_end_lakehouse',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['lakehouse', 'pyspark', 'delta', 'dbt', 'reconciliation'],
) as dag:

    # 1. Operational Ingestion Task (CSV -> Postgres source.*)
    task_seed_source = BashOperator(
        task_id='1_seed_operational_postgres_source',
        bash_command=f'"{PYTHON_BIN}" "{WORKSPACE_DIR}/ingestion/seed_postgres_source.py"',
        cwd=WORKSPACE_DIR,
    )

    # 2. Bronze Delta Extractor (Postgres source.* -> Bronze Delta Lake)
    task_extract_bronze = BashOperator(
        task_id='2_extract_to_bronze_delta',
        bash_command=f'"{PYTHON_BIN}" "{WORKSPACE_DIR}/ingestion/extract_to_bronze_delta.py"',
        cwd=WORKSPACE_DIR,
    )

    # 3. Silver Conformation & Quarantine Engine (Bronze Delta -> Silver Delta + Quarantine)
    task_conform_silver = BashOperator(
        task_id='3_conform_to_silver_delta_and_quarantine',
        bash_command=f'"{PYTHON_BIN}" "{WORKSPACE_DIR}/transformation/bronze_to_silver_delta.py"',
        cwd=WORKSPACE_DIR,
    )

    # 4. Stage Silver Delta tables into PostgreSQL staging schema for dbt
    task_stage_silver = BashOperator(
        task_id='4_stage_silver_delta_to_postgres',
        bash_command=f'"{PYTHON_BIN}" "{WORKSPACE_DIR}/transformation/stage_silver_to_postgres.py"',
        cwd=WORKSPACE_DIR,
    )

    # 5. Gold Dimensional Modeling (Build Star Schema Marts via dbt)
    task_dbt_run = BashOperator(
        task_id='5_dbt_run_gold_models',
        bash_command=f'dbt run --profiles-dir . --target dev',
        cwd=os.path.join(WORKSPACE_DIR, "dbt_veridian"),
    )

    # 6. Automated Data Quality Gate (33 dbt tests & quality circuit breaker)
    task_dbt_test = BashOperator(
        task_id='6_dbt_test_quality_circuit_breaker',
        bash_command=f'dbt test --profiles-dir . --target dev',
        cwd=os.path.join(WORKSPACE_DIR, "dbt_veridian"),
    )

    # 7. 10-Gate Enterprise Financial Reconciliation & Audit Gatekeeper
    task_reconciliation_audit = BashOperator(
        task_id='7_macro_reconciliation_audit',
        bash_command=f'"{PYTHON_BIN}" "{WORKSPACE_DIR}/recon/run_reconciliation.py"',
        cwd=WORKSPACE_DIR,
    )

    # Task Execution Lineage
    (
        task_seed_source
        >> task_extract_bronze
        >> task_conform_silver
        >> task_stage_silver
        >> task_dbt_run
        >> task_dbt_test
        >> task_reconciliation_audit
    )