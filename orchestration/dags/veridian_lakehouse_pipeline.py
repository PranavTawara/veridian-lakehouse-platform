import os
import sys
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models.param import Param

WORKSPACE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
PYTHON_BIN = sys.executable

config_env = {}
config_path = os.path.join(WORKSPACE_DIR, "config/pipeline.env")
if os.path.exists(config_path):
    with open(config_path) as f:
        for line in f:
            if line.strip() and not line.startswith("#") and "=" in line:
                k, v = line.strip().split("=", 1)
                config_env[k] = v

default_mode = config_env.get("LOAD_MODE", "full")
default_schedule = config_env.get("PIPELINE_SCHEDULE", "@daily")
default_wm_col = config_env.get("WATERMARK_COL", "_extracted_at")

default_args = {
    'owner': 'pranav',
    'depends_on_past': False,
    'start_date': datetime(2026, 10, 1),
    'retries': 1,
}

with DAG(
    dag_id='veridian_end_to_end_lakehouse',
    description='Dynamic Medallion Lakehouse: Full or Incremental via Single Config Parameter',
    default_args=default_args,
    schedule=default_schedule if default_schedule != "None" else None,
    catchup=False,
    max_active_runs=1,
    params={
        "load_mode": Param(default_mode, enum=["full", "incremental"], description="Set execution load pattern"),
        "watermark_col": Param(default_wm_col, type="string", description="Watermark column for incremental filtering"),
    },
    tags=['lakehouse', 'dynamic', 'medallion', 'pyspark', 'delta-lake', 'dbt'],
) as dag:

    # 1. Operational Ingestion
    task_seed_source = BashOperator(
        task_id='1_seed_raw_csv_to_operational_postgres_source',
        bash_command=f'"{PYTHON_BIN}" "{WORKSPACE_DIR}/ingestion/seed_postgres_source.py"',
        cwd=WORKSPACE_DIR,
    )

    # 2. Bronze Delta Extraction
    task_extract_bronze = BashOperator(
        task_id='2_extract_postgres_source_to_immutable_bronze_delta',
        bash_command=f'"{PYTHON_BIN}" "{WORKSPACE_DIR}/ingestion/extract_to_bronze_delta.py"',
        cwd=WORKSPACE_DIR,
    )

    # 3. Dynamic Silver Conformation
    task_conform_silver = BashOperator(
        task_id='3_pyspark_cleanse_conform_and_quarantine_silver_delta',
        bash_command=(
            f'"{PYTHON_BIN}" "{WORKSPACE_DIR}/transformation/bronze_to_silver_delta.py" '
            f'--mode "{{{{ params.load_mode }}}}" --watermark-col "{{{{ params.watermark_col }}}}"'
        ),
        cwd=WORKSPACE_DIR,
    )

    # 4. Dynamic Staging to Postgres
    task_stage_silver = BashOperator(
        task_id='4_stage_silver_delta_tables_to_postgres_via_jdbc',
        bash_command=f'"{PYTHON_BIN}" "{WORKSPACE_DIR}/transformation/stage_silver_to_postgres.py" --mode "{{{{ params.load_mode }}}}"',
        cwd=WORKSPACE_DIR,
    )

    # 5. Dynamic dbt Star Schema
    task_dbt_run = BashOperator(
        task_id='5_dbt_build_gold_dimensional_star_schema_marts',
        bash_command='''
        if [ "{{ params.load_mode }}" = "full" ]; then
            dbt run --profiles-dir . --target dev --full-refresh
        else
            dbt run --profiles-dir . --target dev
        fi
        ''',
        cwd=os.path.join(WORKSPACE_DIR, "dbt_veridian"),
    )

    # 6. Data Quality Circuit Breaker
    task_dbt_test = BashOperator(
        task_id='6_dbt_test_33_data_quality_circuit_breakers',
        bash_command='dbt test --profiles-dir . --target dev',
        cwd=os.path.join(WORKSPACE_DIR, "dbt_veridian"),
    )

    # 7. Enterprise Financial Reconciliation & Audit
    task_reconciliation_audit = BashOperator(
        task_id='7_execute_11_gate_enterprise_reconciliation_audit',
        bash_command=f'"{PYTHON_BIN}" "{WORKSPACE_DIR}/recon/run_reconciliation.py"',
        cwd=WORKSPACE_DIR,
    )

    (
        task_seed_source
        >> task_extract_bronze
        >> task_conform_silver
        >> task_stage_silver
        >> task_dbt_run
        >> task_dbt_test
        >> task_reconciliation_audit
    )