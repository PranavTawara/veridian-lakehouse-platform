# Engineering log

## 2026-10-08: Initial Setup & Data Profiling
**Objective:** Profile `initial_load` data to inform the High-Level Design (HLD) and Silver-layer data model before writing ingestion code.

**Key Discoveries & Handled Edge Cases:**
1. **RFC-4180 Parsing Requirement:** `loan_applications.csv` and `loan_accounts.csv` contain quoted thousands-separators (e.g., `"1,984.47"`). Naive string splitting will fail. Bronze ingestion must use strict CSV parsing.
2. **Product Master Integrity:** `product_master.csv` has primary key collisions (`PL01`, `GL01`, `ML01` have duplicated rows with casing drift) and conflicting business logic (GL01 has both 9.75% and 9.80% interest rates). 
   * *Decision:* Silver layer will enforce `UPPER(TRIM())` deduplication and route unresolved conflicts to a `quarantine` table.
3. **Branch Coverage Gaps:** `branch_master.csv` contains 88 branches, but `branch_financials.csv` only contains 85. 
   * *Decision:* Gold layer P&L models will use a `LEFT JOIN` from the branch dimension to preserve active branches missing financial extracts, tagging them with a `financials_missing = TRUE` flag.
4. **Dates:** Format chaos exists across files (ISO, MM/YYYY, DD-MON-YYYY). Bronze will load as raw `TEXT`, and Silver will enforce standardized `DATE`/`TIMESTAMPTZ` casting.

## 2026-10-12: Architecture Review & Whiteboard Alignment
**Objective:** Align repository High-Level Design (HLD) and Low-Level Design (LLD) with whiteboard discussion feedback.

**Key Updates:**
1. **Separation of OLTP and Lakehouse:** Refactored the architecture from hosting all layers in a single Postgres database into three distinct architectural boundaries:
   - Source OLTP DB (`Postgres: source` schema) simulating core-banking production tables.
   - Analytical Lakehouse (`Bronze` -> `Silver` -> `Gold` stored as Parquet files) to isolate compute from transactional operations.
   - Serving Data Mart (`Postgres: serving` schema) loaded from Gold to serve low-latency queries to FastAPI.
2. **Design Status:** ADR 001 updated to `Status: Proposed (Awaiting Review)` pending senior sign-off.
3. **Data Contracts:** Formalized schemas, join criteria (preserving all 88 branches), surrogate key patterns, and quarantine error logging.

## 2026-10-15: Finalization of Empirical HLD (ADR 003) and LLD Contracts
**Objective:** Formalize ADR 003 and update the Data Model contracts based on exhaustive profiling across all 11 CSV files in `initial_load/`[cite: 4].

**Key Architectural Advancements:**
1. **ADR 003 Promotion:** Promoted HLD to ADR 003, establishing physical compute decoupling (PostgreSQL source -> Parquet Medallion -> PostgreSQL serving) and empirical quarantine architecture[cite: 4].
2. **Ledger Balancing Contract:** Addressed the ₹135.5B transaction-to-account variance through deduplication (1,255 duplicate transactions), currency sanitization, and isolating 1,002 orphan transactions into `silver.quarantine_records`[cite: 4].
3. **Regulatory Truth Derivation:** Codified `is_regulatory_npa = (dpd >= 90 OR npa_flag = 'Y')` to rectify the 100% false negative source flags across all 14,345 loan accounts.
4. **Master Data Management:** Defined deterministic SCD Type 1 window deduplication for 2,740 conflicting customer records and catalog collision resolution for product `GL01`[cite: 4].
5. **Geographic Conformance:** Enforced conformed dimension hierarchy from `dim_branch`, resolving 424 conflicting region records in transactional feeds.

## 2026-10-15: Architecture Formalization — Enterprise Delta Lakehouse (ADR 004)
**Objective:** Incorporate reviewer feedback regarding Lakehouse table format definitions and establish the full PySpark + Delta Lake + dbt + Airflow architecture[cite: 4, 19].

**Key Architectural Decisions & Formalizations:**
1. **Adoption of Delta Lake Open Table Format:** Replaced unmanaged Parquet file dumping with ACID-compliant Delta tables across Bronze, Silver, and Gold[cite: 4, 19]. Every entity now maintains an active `_delta_log/` commit log, enabling atomic writes, schema enforcement, time-travel auditability, and native `MERGE INTO` operations[cite: 4, 19].
2. **PySpark Compute Engine:** Formalized PySpark 3.5 (`delta-spark`) as the core engine for JDBC extraction from PostgreSQL `source`, regex string sanitization, 13-digit Unix ms epoch conversion, and quarantine routing[cite: 4].
3. **dbt Semantic Modeling & Governance:** Integrated dbt for Gold star schema transformations, version-controlled testing (`unique`, `not_null`, `relationships`), and automated lineage generation[cite: 4].
4. **Airflow Orchestration Control Plane:** Documented task-level failure isolation and reconciliation circuit-breakers in Airflow to manage Day-2 incremental loads idempotently[cite: 4].
5. **Data Quality Contracts:** Fully codified the resolution of the ₹135.5B ledger variance, regulatory NPA derivations, customer master SCD-1 window deduplication, and ghost branch quarantining[cite: 4].

# Engineering Log & Decision Record

## [2026-10-28] - Phase 1: Medallion Architecture Conformation, Data Quality & Forensic Reconciliation

### 1. Ingestion & Storage Architecture
- **Decision:** Built a multi-layer Medallion architecture (Bronze Delta $\to$ Silver Conformed Delta $\to$ PostgreSQL Serving Layer) orchestrated via PySpark and dbt-core.
- **Rationale:** Local Delta Lake provides ACID guarantees, schema evolution tracking, and zero-copy versioned table storage. PostgreSQL serving models provide standard SQL access for BI consumption and downstream API endpoints.

---

### 2. Forensic Data Quality Findings & Remediation

#### A. Customer Onboarding Date Corruption
- **Defect:** 517 customer records initially failed validation with null onboarding dates.
- **Root Cause:** Upstream exports contained Indian/UK date formats (`dd/MM/yyyy`, e.g., `05/07/2023`, `13/01/2024`) alongside standard ISO `yyyy-MM-dd`. Standard `to_date()` parsing rejected forward-slash formats.
- **Fix:** Implemented a regex-guarded multi-format date parser in `bronze_to_silver_delta.py` that handles `d/M/yyyy`, `yyyy-MM-dd`, and 13-digit epoch millisecond timestamps. 100% of 19,417 customer records now conform without data loss or default placeholder imputation.

#### B. Unvalued Repayment Transactions
- **Defect:** 568 repayment records contained empty string `""` / `NULL` amounts.
- **Root Cause:** Core banking transaction batches logged unvalued repayment ledger rows without monetary values.
- **Remediation:** In accordance with core banking principles, unvalued postings were routed to `silver.quarantine` with rejection code `INVALID_TXN_AMOUNT`. 
- **Ledger Impact:** 41,656 clean transactions were conformed to Silver, ensuring zero null-value mathematical corruption in loan accounting.

#### C. Quarantine Segregation & Ledger Accounting
- **Total Quarantined Records:** Exactly 5,226 records isolated in `silver.quarantine`:
  - `INVALID_BRANCH_FK` (2,644 rows: 2,364 employees, 280 loans): Non-existent branches (including ghost branches `BR-999` and `BR-0XX`).
  - `ORPHAN_ACCOUNT_ID` (2,011 rows): Transactions referencing non-existent loan account IDs.
  - `INVALID_TXN_AMOUNT` (568 rows): Unvalued repayment records.
  - `PRODUCT_RATE_CONFLICT` (3 rows): Conflicting interest rate definitions deduped and isolated.
- **Reverse Scan Audit:** 0 corrupt records leaked into Silver; 100% referential integrity verified.

---

### 3. Multi-System Delinquency Reconciliation (The NPA vs. Event Stream Contradiction)

#### The Conflict
- `source.account_status_events` contained 8,776 `NPA_TAGGED` rows covering 4,575 distinct loan applications (~32.5% portfolio failure rate).
- `silver.loans` recorded 100% of accounts with `source_npa_flag = 'N'` and portfolio DPD bounded strictly between 0 and 30 days (85% Current at 0 DPD, 15% SMA-0 at 1–30 DPD).

#### Forensic Investigation
- Event timestamps revealed that `NPA_TAGGED` events occurred *prior to loan disbursement* during pre-origination underwriting, rather than post-disbursement default transitions.
- Blindly joining on `event_type = 'NPA_TAGGED'` produced a uniform ~32.5% failure rate across all four regions, obscuring actual geographic performance.

#### Adopted Design: Dual-Horizon Credit Risk Reporting
- **Regulatory NPA (DPD $\ge$ 90 / CBS Default):** Enforced at **0.00%** (₹0 default balance) to reflect legal banking compliance for this baseline load.
- **Early Delinquency Watchlist (SMA-0: 1–30 DPD):** Exposed at **15.1%** (2,106 accounts | ₹69.71 Cr) in `serving.fact_regional_risk_profile`.
- **Strategic Impact:** Highlights real regional risk variation:
  - **North (15.96% SMA-0 | RED):** Tighten underwriting and enhance field collections.
  - **South (15.21% SMA-0 | AMBER):** Selective expansion with active monitoring.
  - **East (14.71% SMA-0 | AMBER):** Moderate risk profile.
  - **West (14.50% SMA-0 | GREEN):** Prime expansion priority with the cleanest credit book.

---

### 4. Dimensional Modeling & Test Automation
- Materialized 4 dimensions (`dim_branch`, `dim_product`, `dim_customer`, `dim_employee`) and 3 fact marts (`fact_branch_monthly_pnl`, `fact_regional_risk_profile`, `fact_branch_staffing`).
- Configured 33 dbt schema and relationship tests (100% passing).
- Engineered a 23-point lakehouse audit suite (`audit_veridian_lakehouse.py`) verifying cross-layer financial ledger balances down to the exact rupee (₹461,614,655.81).