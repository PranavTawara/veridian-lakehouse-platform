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

dag_documentation = """
### Veridian Medallion Lakehouse: End-to-End Orchestrator

This production DAG automates the complete data lifecycle across the Veridian Lending Platform:
1. **Operational Ingestion:** Ingests raw source CSVs into the operational PostgreSQL database.
2. **Bronze Delta Extraction:** Immutably stores raw tables as Snappy-compressed Parquet with ACID logs.
3. **Silver Conformation & Quarantine:** PySpark engine executing cleaning, deduplication, Two-Wheeler ('TL01') mapping, and quarantine routing.
4. **PostgreSQL JDBC Staging:** High-throughput bridge populating the `silver.*` schema for downstream analytical SQL.
5. **Gold Star Schema Modeling:** dbt-core building dimensional and fact serving marts (`serving.*`).
6. **Data Quality Circuit Breaker:** 33 automated schema and relationship assertions.
7. **11-Gate Financial Reconciliation:** Automated ledger tie-outs, zero-silent-loss validation, and primary key purity assertions.
"""

with DAG(
    dag_id='veridian_end_to_end_lakehouse',
    description='Automated Medallion Lakehouse: Raw -> Bronze Delta -> Silver Quarantine -> dbt Gold -> 11-Gate Audit',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    max_active_runs=1,
    doc_md=dag_documentation,
    tags=['lakehouse', 'medallion', 'pyspark', 'delta-lake', 'dbt', 'reconciliation-audit'],
) as dag:

    # -------------------------------------------------------------------------
    # 1. Operational Ingestion Task
    # -------------------------------------------------------------------------
    task_seed_source = BashOperator(
        task_id='1_seed_raw_csv_to_operational_postgres_source',
        bash_command=f'"{PYTHON_BIN}" "{WORKSPACE_DIR}/ingestion/seed_postgres_source.py"',
        cwd=WORKSPACE_DIR,
        doc_md="""
        #### Task 1: Seed Operational Postgres Source
        * **Purpose:** Simulates the upstream Core Banking System (CBS) and HRMS operational database.
        * **Input:** 11 raw source CSV files (`./data/*.csv`).
        * **Output:** Populates 11 raw operational tables in the PostgreSQL `source.*` schema.
        """,
    )

    # -------------------------------------------------------------------------
    # 2. Bronze Delta Extraction Task
    # -------------------------------------------------------------------------
    task_extract_bronze = BashOperator(
        task_id='2_extract_postgres_source_to_immutable_bronze_delta',
        bash_command=f'"{PYTHON_BIN}" "{WORKSPACE_DIR}/ingestion/extract_to_bronze_delta.py"',
        cwd=WORKSPACE_DIR,
        doc_md="""
        #### Task 2: Extract to Bronze Delta Lake
        * **Purpose:** Ingests raw operational data into the immutable Bronze lakehouse layer with zero business transformations.
        * **Storage:** Snappy-compressed Parquet files with JSON transaction logs in `./lakehouse/bronze/*`.
        * **Guarantees:** Append-only ACID reliability and replayability.
        """,
    )

    # -------------------------------------------------------------------------
    # 3. Silver Conformation & Quarantine Task
    # -------------------------------------------------------------------------
    task_conform_silver = BashOperator(
        task_id='3_pyspark_cleanse_conform_and_quarantine_silver_delta',
        bash_command=f'"{PYTHON_BIN}" "{WORKSPACE_DIR}/transformation/bronze_to_silver_delta.py"',
        cwd=WORKSPACE_DIR,
        doc_md="""
        #### Task 3: PySpark Conformation & Quarantine Engine
        * **Purpose:** Cleanses, standardizes, deduplicates, and validates data across all entities.
        * **Key Logic:**
          * Conforms empty Two-Wheeler product codes to `'TL01'` (protecting ₹25.7 Cr in portfolio assets).
          * Integrates `account_status_events` to compute regulatory NPA flags.
          * Normalizes paise to rupees and imputes missing disbursement amounts.
        * **Quarantine:** Isolates invalid primary keys, unmapped branches (`INVALID_BRANCH_FK`), orphan transactions (`ORPHAN_ACCOUNT_ID`), and duplicate rates into `./lakehouse/silver/quarantine`.
        """,
    )

    # -------------------------------------------------------------------------
    # 4. Silver Staging via Spark JDBC Task
    # -------------------------------------------------------------------------
    task_stage_silver = BashOperator(
        task_id='4_stage_silver_delta_tables_to_postgres_via_jdbc',
        bash_command=f'"{PYTHON_BIN}" "{WORKSPACE_DIR}/transformation/stage_silver_to_postgres.py"',
        cwd=WORKSPACE_DIR,
        doc_md="""
        #### Task 4: Stage Silver Delta to Postgres via JDBC
        * **Purpose:** Bridges the distributed Delta Lake storage on disk to relational PostgreSQL tables.
        * **Mechanism:** Uses PySpark with `org.postgresql:postgresql:42.7.3` JDBC driver in overwrite mode.
        * **Target:** Stages 7 curated tables (`branches`, `products`, `customers`, `employees`, `loans`, `transactions`, `quarantine`) into PostgreSQL `silver.*`.
        """,
    )

    # -------------------------------------------------------------------------
    # 5. Gold Star Schema Modeling Task
    # -------------------------------------------------------------------------
    task_dbt_run = BashOperator(
        task_id='5_dbt_build_gold_dimensional_star_schema_marts',
        bash_command='dbt run --profiles-dir . --target dev',
        cwd=os.path.join(WORKSPACE_DIR, "dbt_veridian"),
        doc_md="""
        #### Task 5: Build Gold Star Schema Marts (dbt run)
        * **Purpose:** Executes business analytical transformations and dimensional modeling.
        * **Outputs in `serving.*`:**
          * Dimensions: `dim_branch`, `dim_customer`, `dim_employee`, `dim_product` (with surrogate keys).
          * Fact Marts: `fact_branch_staffing`, `fact_regional_risk_profile`, and `fact_branch_monthly_pnl`.
        """,
    )

    # -------------------------------------------------------------------------
    # 6. Data Quality Circuit Breaker Task
    # -------------------------------------------------------------------------
    task_dbt_test = BashOperator(
        task_id='6_dbt_test_33_data_quality_circuit_breakers',
        bash_command='dbt test --profiles-dir . --target dev',
        cwd=os.path.join(WORKSPACE_DIR, "dbt_veridian"),
        doc_md="""
        #### Task 6: Data Quality Circuit Breaker (dbt test)
        * **Purpose:** Enforces 33 strict schema, relationship, and value contract assertions.
        * **Checks:**
          * Uniqueness and not-null constraints on all primary and surrogate keys.
          * Foreign key referential integrity across dimensional relationships.
          * Accepted values (e.g., RAG status in `GREEN`, `AMBER`, `RED`).
        * **Behavior:** Pipeline halts immediately if any assertion fails.
        """,
    )

    # -------------------------------------------------------------------------
    # 7. Enterprise Financial Reconciliation & Audit Task
    # -------------------------------------------------------------------------
    task_reconciliation_audit = BashOperator(
        task_id='7_execute_11_gate_enterprise_reconciliation_audit',
        bash_command=f'"{PYTHON_BIN}" "{WORKSPACE_DIR}/recon/run_reconciliation.py"',
        cwd=WORKSPACE_DIR,
        doc_md="""
        #### Task 7: 11-Gate Enterprise Financial Reconciliation Audit
        * **Purpose:** Comprehensive auditor gatekeeper proving 100% accounting and zero silent loss.
        * **Audit Highlights:**
          * Gate 1: Source = Silver + Quarantine + Deduped across all channels (100.0% accounted).
          * Gate 4: Macro Ledger Tie-Out (₹4.53 variance on a ₹568 Crore portfolio / 0.00000008%).
          * Gate 11: Multi-Channel Primary Key Purity Assertions (0 NULLs, 0 empty strings across all 6 core entities).
        """,
    )

    # -------------------------------------------------------------------------
    # Task Dependency Lineage
    # -------------------------------------------------------------------------
    (
        task_seed_source
        >> task_extract_bronze
        >> task_conform_silver
        >> task_stage_silver
        >> task_dbt_run
        >> task_dbt_test
        >> task_reconciliation_audit
    )