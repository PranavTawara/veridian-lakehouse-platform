# Architecture Decision Record 004: Enterprise Delta Lakehouse Platform (HLD)

## Status
Proposed (Awaiting Review)

## Context & Problem Statement
Initial architectural iterations explored hosting Medallion schemas inside PostgreSQL (ADR 001/002) and storing plain Parquet files on disk (ADR 003). However, architectural review and technical evaluation identified two critical platform constraints:

1. **Storage Format Limitation:** Simply landing raw Parquet files on disk constitutes an unmanaged Data Lake, not an enterprise Lakehouse. Plain Parquet lacks ACID transaction guarantees, contains no centralized transaction log, cannot enforce schema constraints at write time, and cannot perform atomic row-level `MERGE INTO` (upsert) operations without rewriting entire directories. This makes managing Day-2 incremental loads ("Wave 2") brittle and prone to silent corruption.
2. **Empirical Data Anomalies:** Profiling 209,926 records across all 11 raw CSV extracts in `initial_load/` exposed severe operational data quality issues:
   - **₹135.5B Macro Reconciliation Gap:** Total disbursements in `loan_transactions` (₹141.4B) diverge 24x from principal in `loan_accounts` (₹5.88B) due to currency symbol contamination (`$`, `₹`), 1,255 duplicate transactions, and 1,002 orphan accounts.
   - **Operational NPA Failure:** 100% of records in `loan_accounts` default to `npa_flag = 'N'` despite hundreds exceeding 90 Days Past Due (DPD).
   - **Master Entity Collisions:** 2,740 conflicting duplicate `customer_id` rows (0 exact duplicates), `product_master` primary key collisions (`GL01` rate discrepancy, blank code for Two-Wheeler), and ghost branches (`BR-999`, `BR-0XX`).
   - **State Machine De-synchronization:** 2,594 false status rollbacks caused by lexicographically sorting 13-digit Unix millisecond epochs against ISO timestamps.
   - **Geographic Drift:** 424 transactions with region codes contradicting the core branch hierarchy.

The platform requires an open table format with ACID transactions (Delta Lake), a distributed execution engine (PySpark), a semantic modeling and testing framework (dbt), and a failure-isolated orchestration control plane (Apache Airflow).

---

## Architecture Topology

```text
┌──────────────────────────────────────────────────────────────────────────┐
│ 1. Operational Source System: PostgreSQL 16 (source.* schema)            │
│    - Emulates core banking OLTP cluster                                  │
│    - Lossless All-TEXT schema ingestion (209,926 raw records landed)     │
└───────────────────────────────────┬──────────────────────────────────────┘
                                    │ (PySpark JDBC Batch Extract)
                                    ▼
┌──────────────────────────────────────────────────────────────────────────┐
│ 2. Analytical Delta Lakehouse (PySpark + Delta Lake Engine)              │
│                                                                          │
│   ┌──────────────────────────────────────────────────────────────────┐   │
│   │ Bronze Layer (lakehouse/bronze/): Raw Append-Only Delta          │   │
│   │ - Lossless 1:1 schema replication + _delta_log/ commit log       │   │
│   │ - Audit metadata: _extracted_at, _batch_id, _source_file         │   │
│   └───────────────────────────────┬──────────────────────────────────┘   │
│                                   │ (PySpark Sanitization & Quarantine)  │
│                                   ▼                                      │
│   ┌──────────────────────────────────────────────────────────────────┐   │
│   │ Silver Layer (lakehouse/silver/): Conformed Delta Tables         │   │
│   │ - Schema enforcement & type casting (Regex numeric, UTC ts)      │   │
│   │ - Casing normalization (UPPER(TRIM())) & SCD-1 deduplication     │   │
│   │ - Regulatory metric derivation (is_regulatory_npa)               │   │
│   │ - Isolated Quarantine dataset (lakehouse/silver/quarantine/)     │   │
│   └───────────────────────────────┬──────────────────────────────────┘   │
│                                   │ (dbt Semantic Transformations)       │
│                                   ▼                                      │
│   ┌──────────────────────────────────────────────────────────────────┐   │
│   │ Gold Layer (lakehouse/gold/): Star Schema Delta Mart             │   │
│   │ - Dimensions (dim_branch, dim_product, dim_benchmark)            │   │
│   │ - Facts (fact_branch_monthly_pnl, fact_regional_risk)            │   │
│   │ - Automated dbt test quality gate assertions                     │   │
│   └──────────────────────────────────────────────────────────────────┘   │
└───────────────────────────────────┬──────────────────────────────────────┘
                                    │ (Atomic PostgreSQL Sync)
                                    ▼
┌──────────────────────────────────────────────────────────────────────────┐
│ 3. Serving Data Mart: PostgreSQL 16 (serving.* schema)                   │
│    - Indexed relational star schema for low-latency queries              │
│    - Sub-millisecond read access for FastAPI REST endpoints              │
└───────────────────────────────────┬──────────────────────────────────────┘
                                    │ (JSON Payloads)
                                    ▼
┌──────────────────────────────────────────────────────────────────────────┐
│ 4. Consumption & Observability                                           │
│    - FastAPI Service: /api/v1/branches/profitability, etc.               │
│    - Apache Airflow 2.9 UI: DAG monitoring, telemetry, circuit breaker   │
└──────────────────────────────────────────────────────────────────────────┘
```

## Component Boundaries & Technology Justification

### 1. Operational Source System (`PostgreSQL: source`)
* **Role:** Emulates Veridian's legacy transactional core banking database[cite: 4].
* **Technology:** PostgreSQL 16 container (`source` schema)[cite: 3].
* **Ingestion Contract:** All business attributes are ingested as raw `TEXT`[cite: 1, 4]. This prevents ingestion failures caused by currency symbols, quoted commas, or epoch timestamps, guaranteeing 100% lossless ingestion before analytical processing begins[cite: 1, 4].
* **Production AWS Equivalent:** Amazon RDS PostgreSQL / Aurora Core Banking.

### 2. Lakehouse Storage Engine: Delta Lake (`delta-spark` / `delta-rs`)
* **Role:** Serves as the open table format providing warehouse capabilities over object storage.
* **Capabilities:**
  * **ACID Transactions (`_delta_log/`):** Ensures writes are atomic[cite: 4, 19]. A failed or aborted batch never leaves corrupted, unreadable Parquet files in the lakehouse[cite: 4].
  * **Storage-Level `MERGE` (Upsert):** Enables native, atomic upserts for Day-2 incremental loads without rewriting entire tables[cite: 4].
  * **Schema Enforcement & Evolution:** Prevents unexpected schema drift from corrupting Silver and Gold layers[cite: 4].
  * **Time Travel:** Allows point-in-time auditing (`VERSION AS OF`) for regulatory compliance[cite: 4].
* **Production AWS Equivalent:** Delta Lake / Apache Iceberg on Amazon S3.

### 3. Compute & Transformation Engine: PySpark (Apache Spark 3.5)
* **Role:** Executes high-throughput data extraction, format repair, master deduplication, and quarantine routing[cite: 4].
* **Processing Boundaries:**
  * Extracts from PostgreSQL `source` via JDBC into Bronze Delta tables[cite: 4].
  * Cleans string corruption, parses multi-format timestamps, strips currency symbols, normalizes casing, and derives regulatory NPA indicators[cite: 4].
  * Implements an automated quarantine split: valid records advance to `lakehouse/silver/`, while invalid records (1,002 orphan transactions, 2,202 orphan applications, ghost branches) are routed to `lakehouse/silver/quarantine/`[cite: 4].
* **Production AWS Equivalent:** AWS Glue 4.0 / Amazon EMR Serverless (PySpark runtime)[cite: 3, 4].

### 4. Semantic Modeling & Governance: dbt (`dbt-core`)
* **Role:** Manages business logic modeling, star schema construction, and automated testing[cite: 4].
* **Processing Boundaries:**
  * Builds Gold star schema dimensional models (`dim_*`) and fact tables (`fact_*`) using version-controlled SQL models (`ref()`)[cite: 2, 4].
  * Executes data quality tests via `dbt test` (`unique`, `not_null`, `relationships`, `accepted_values`) to assert relational integrity[cite: 4].
  * Generates DAG lineage documentation and interactive data catalog (`dbt docs generate`)[cite: 4].
* **Production AWS Equivalent:** dbt Cloud / dbt running in AWS CodePipeline.

### 5. Orchestration, Governance & Observability: Apache Airflow
* **Role:** Acts as the centralized pipeline orchestrator and circuit breaker[cite: 4].
* **Capabilities:**
  * **Task-Level Blast Radius Isolation:** Deconstructs the pipeline into atomic tasks (`seed_source` $\rightarrow$ `extract_bronze` $\rightarrow$ `conform_silver` $\rightarrow$ `recon_gate` $\rightarrow$ `dbt_gold` $\rightarrow$ `sync_serving`)[cite: 4].
  * **Automated Circuit Breaker:** If data reconciliation detects ledger variance $>0.01\%$ or if `dbt test` fails, the pipeline halts immediately, protecting the PostgreSQL `serving` mart from receiving corrupted state[cite: 4].
  * **Incremental Wave Handling:** Manages watermark parameters (`execution_date`, `batch_id`) to orchestrate idempotent Day-2 delta runs[cite: 4].
* **Production AWS Equivalent:** Amazon Managed Workflows for Apache Airflow (MWAA)[cite: 3, 4].

### 6. Serving Data Mart & Consumption: PostgreSQL (`serving`) + FastAPI
* **Role:** Serves low-latency, indexed reporting queries to the leadership dashboard[cite: 4].
* **Technology:** PostgreSQL 16 container (`serving` schema) + FastAPI async REST service[cite: 3, 4].
* **Zero-Downtime Serving:** Sync operations utilize SQL `INSERT ... ON CONFLICT (...) DO UPDATE` (UPSERT) to ensure API endpoints remain responsive during pipeline executions[cite: 4].

---

## Architectural Decision Summary & Trade-offs

| Decision | Selected Approach | Alternative Considered | Architectural Rationale & Trade-off |
| :--- | :--- | :--- | :--- |
| **Lakehouse Format** | **Delta Lake (`_delta_log/`)**[cite: 4, 19] | Plain Parquet files | Plain Parquet lacks ACID guarantees, transaction history, and atomic `MERGE`, making Day-2 incremental updates fragile[cite: 4, 19]. |
| **Compute Engine** | **PySpark 3.5**[cite: 4] | DuckDB / In-Memory Pandas | PySpark provides enterprise-scale distributed processing, native Delta table integration, and maps directly to AWS Glue in production[cite: 3, 4]. |
| **Modeling & DQ** | **dbt (data build tool)**[cite: 4] | Custom Python scripts | dbt standardizes SQL models, automates lineage documentation, and formalizes version-controlled assertions (`unique`, `relationships`)[cite: 4]. |
| **Orchestration** | **Apache Airflow**[cite: 4] | Single Python script / Cron | Airflow provides task-level failure isolation, execution metrics, retry policies, and an operational UI for monitoring pipeline runs[cite: 4]. |
| **Error Handling** | **Quarantine Delta Table**[cite: 4] | Silent row drops or fatal pipeline halts | Silent drops introduce financial errors; fatal halts block daily reporting. Quarantining maintains 100% auditability while keeping pipelines running[cite: 4]. |
