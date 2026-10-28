# Low-Level Design (LLD): Data Model & Delta Table Contracts

## 1. Operational Source Layer (`PostgreSQL: source`)
Represents the core banking operational transactional database. 11 relational tables loaded losslessly as text to preserve original incoming payloads.

* **Schema:** `source`
* **Tables:** `branch_master`, `branch_financials`, `product_master`, `customer_master`, `deposit_accounts`, `employee_roster`, `loan_accounts`, `loan_applications`, `loan_transactions`, `region_demographics`, `account_status_events`.
* **Storage Contract:** All columns defined as `TEXT`. Quoted commas, currency symbols (`$`, `₹`), 13-digit Unix millisecond epochs, and whitespace are retained untouched.
* **Audit Lineage Columns:** `_batch_id` (`VARCHAR`), `_loaded_at` (`TIMESTAMPTZ`), `_source_file` (`VARCHAR`).

---

## 2. Lakehouse Medallion Layers (Delta Lake Format)

### 2.1 Bronze Landing Layer (`lakehouse/bronze/`)
* **Storage Engine:** Delta Lake Open Table Format with `_delta_log/` transaction logging.
* **Table Paths:** `lakehouse/bronze/{entity}/` (e.g., `lakehouse/bronze/loan_accounts/`).
* **Storage Mode:** Append-only ingestion via PySpark JDBC read.
* **Lineage Metadata:** `_extracted_at` (`TIMESTAMPTZ`), `_pipeline_batch_id` (`VARCHAR`), `_source_table` (`VARCHAR`).
* **Contract:** 1:1 lossless schema replication from `source.*`. Zero destructive casting or row filtering.

---

### 2.2 Silver Conformed & Hygiene Layer (`lakehouse/silver/`)
Silver resolves schema drift, standardizes timestamps, deduplicates natural keys, enforces referential integrity, and isolates non-compliant records into quarantine.

#### Entity Specifications & Transformation Contracts:

1. **`silver.branches` (85 conformed rows)**:
   * **Primary Key:** `branch_code` (`VARCHAR`, PK).
   * **Columns:** `branch_code`, `branch_name`, `region`, `state`, `city`, `branch_type`, `open_date` (`DATE`).
   * **Rule 1.1 (Ghost Branch Isolation):** Ghost branches (`BR-999`, `BR-0XX`) are diverted to quarantine (`GHOST_BRANCH`).
   * **Rule 1.2 (Casing & Whitespace):** Natural key normalized via `UPPER(TRIM(branch_code))`. Regions conformed to `NORTH`, `SOUTH`, `EAST`, `WEST`.

2. **`silver.products` (5 conformed rows)**:
   * **Primary Key:** `product_code` (`VARCHAR`, PK).
   * **Columns:** `product_code`, `product_name`, `category`, `interest_rate` (`DECIMAL(5,2)`), `is_active` (`BOOLEAN`).
   * **Rule 2.1 (Missing Key Imputation):** Imputes surrogate code `TL01` for the record with missing `product_code` (`product_name = 'Two-Wheeler Loan'`).
   * **Rule 2.2 (Catalog Collision Resolution):** Clashing rates for `GL01` (9.75% vs 9.80%) resolved by picking highest active rate via `ROW_NUMBER() OVER (PARTITION BY UPPER(TRIM(product_code)) ORDER BY interest_rate DESC)`. Conflicting variants (3 rows) routed to quarantine (`PRODUCT_RATE_CONFLICT`).

3. **`silver.customers` (19,417 conformed rows)**:
   * **Primary Key:** `customer_id` (`VARCHAR`, PK).
   * **Columns:** `customer_id`, `customer_name`, `city`, `state`, `kyc_status`, `income_band`, `onboarding_date` (`DATE`).
   * **Rule 3.1 (Multi-Format Date Engine):** Resolves 517 UK/Indian format dates (`dd/MM/yyyy`, e.g., `05/07/2023`, `14/05/2024`) alongside standard ISO `yyyy-MM-dd`. Zero null dates or default placeholders imputed.
   * **Rule 3.2 (Text Sanitization & Deduplication):** Strips Unicode noise; deduplicates using latest `onboarding_date` and prioritized verified KYC status.

4. **`silver.employees` (46,965 conformed rows)**:
   * **Primary Key:** `employee_id` (`VARCHAR`, PK).
   * **Foreign Key:** `branch_code` references `silver.branches(branch_code)`.
   * **Columns:** `employee_id`, `branch_code`, `full_name`, `role`, `department`, `annual_ctc` (`DECIMAL(14,2)`), `hire_date` (`DATE`).
   * **Rule 4.1 (Referential Quarantine):** Diverts 2,364 employees referencing ghost/invalid branches to quarantine (`INVALID_BRANCH_FK`).
   * **Rule 4.2 (Numeric Cleaning):** Strips currency symbols and normalizes CTC to numeric floats.

5. **`silver.loans` (14,065 conformed rows)**:
   * **Primary Key:** `account_id` (`VARCHAR`, PK).
   * **Foreign Keys:** `branch_code` references `silver.branches`, `product_code` references `silver.products`.
   * **Columns:** `account_id`, `application_id`, `branch_code`, `product_code`, `principal_amount` (`DECIMAL(14,2)`), `outstanding_amount` (`DECIMAL(14,2)`), `dpd` (`INTEGER`), `source_npa_flag` (`VARCHAR`), `is_regulatory_npa` (`BOOLEAN`), `open_date` (`DATE`), `last_updated` (`TIMESTAMPTZ`).
   * **Rule 5.1 (Referential Quarantine):** 280 loans originated under invalid/ghost branches routed to quarantine (`INVALID_BRANCH_FK`).
   * **Rule 5.2 (Regulatory NPA Derivation):** Evaluates `is_regulatory_npa = TRUE` strictly if `dpd >= 90` or `UPPER(TRIM(source_npa_flag)) = 'Y'`. Baseline load exhibits 0.00% regulatory default with 15.0% early watchlist (1–30 DPD).

6. **`silver.transactions` (41,656 conformed rows)**:
   * **Primary Key:** `txn_id` (`VARCHAR`, PK).
   * **Foreign Key:** `account_id` references `silver.loans(account_id)`.
   * **Columns:** `txn_id`, `account_id`, `application_id`, `branch_code`, `txn_type`, `clean_amount` (`DECIMAL(14,2)`), `is_reversal` (`BOOLEAN`), `txn_timestamp` (`TIMESTAMPTZ`).
   * **Rule 6.1 (Orphan Account Isolation):** Diverts 2,011 transactions posting to non-existent loans to quarantine (`ORPHAN_ACCOUNT_ID`).
   * **Rule 6.2 (Unvalued Transaction Quarantine):** Isolates 568 repayment records with empty/null amounts to quarantine (`INVALID_TXN_AMOUNT`) to prevent general ledger distortion.
   * **Rule 6.3 (Numeric Cleaning):** Cleans currency symbols; tags negative amounts as `is_reversal = TRUE` with positive `clean_amount`.

7. **`silver.quarantine` (5,226 audited rejected rows)**:
   * **Storage:** Delta Lake table and PostgreSQL schema preserving rejected records.
   * **Schema:** `quarantine_id` (`UUID`, PK), `source_table` (`VARCHAR`), `record_identifier` (`VARCHAR`), `rejection_code` (`VARCHAR`), `raw_payload` (`TEXT`), `quarantined_at` (`TIMESTAMPTZ`).
   * **Audited Breakdown:**
     - `INVALID_BRANCH_FK`: 2,644 rows (2,364 employee roster, 280 loan accounts)
     - `ORPHAN_ACCOUNT_ID`: 2,011 rows (loan transactions)
     - `INVALID_TXN_AMOUNT`: 568 rows (loan transactions)
     - `PRODUCT_RATE_CONFLICT`: 3 rows (product master)

---

### 2.3 Gold Consumption & Serving Layer (`PostgreSQL: serving`)
Dimensional star schema engineered with dbt-core and materialized in PostgreSQL for low-latency BI and API access.

#### Dimension Tables:
* **`serving.dim_branch`** (85 rows): Surrogate PK `branch_key`, Natural PK `branch_code`. Attributes: `branch_name`, `region`, `state`, `city`, `branch_type`, `open_date`.
* **`serving.dim_product`** (5 rows): Surrogate PK `product_key`, Natural PK `product_code`. Attributes: `product_name`, `category`, `interest_rate`.
* **`serving.dim_customer`** (19,417 rows): Surrogate PK `customer_key`, Natural PK `customer_id`. Attributes: `customer_name`, `city`, `state`, `kyc_status`, `income_band`, `onboarding_date`.
* **`serving.dim_employee`** (46,965 rows): Surrogate PK `employee_key`, Natural PK `employee_id`. Attributes: `branch_code`, `full_name`, `role`, `department`, `annual_ctc`, `hire_date`.

#### Fact Tables:
* **`serving.fact_branch_monthly_pnl`** (86 rows):
  * **Grain:** 1 row per `branch_key` per `reporting_month`.
  * **Metrics:** `interest_income`, `operating_cost`, `net_contribution`, `financials_missing`. Preserves branch `BR-047` with missing financials flagged.
* **`serving.fact_regional_risk_profile`** (4 regional rows):
  * **Grain:** 1 row per geographic region (`NORTH`, `SOUTH`, `EAST`, `WEST`).
  * **Dual-Horizon Metrics:**
    - Regulatory Defaults: `npa_accounts`, `npa_balance`, `npa_rate_pct` (DPD $\ge$ 90 / CBS Default).
    - Early Watchlist: `sma0_accounts`, `sma0_balance`, `sma0_rate_pct` (1–30 DPD).
  * **RAG Classification:** Dynamically flagged (`RED` if NPA $> 5\%$ or SMA-0 $\ge 15.5\%$, `AMBER` if NPA $> 2\%$ or SMA-0 $\ge 14.6\%$, `GREEN` otherwise).
* **`serving.fact_branch_staffing`** (85 rows):
  * **Grain:** 1 row per active branch.
  * **Metrics:** `total_active_staff`, `total_loans_serviced`, `total_outstanding_book`, `loans_per_officer`, `portfolio_per_officer`.

---

## 3. Automated Data Quality & Circuit-Breaker Matrix

| Layer | DQ Dimension | Assertion / Contract Rule | Tool | Action on Failure |
| :--- | :--- | :--- | :--- | :--- |
| **Bronze** | Ingestion Audit | `_batch_id` and `_extracted_at` must not be null | PySpark | **HALT** pipeline |
| **Silver** | Date Validity | Multi-format parsing handles `dd/MM/yyyy` and ISO dates | PySpark | Zero null dates permitted |
| **Silver** | Value Validity | Repayment transactions must have monetary amount $> 0$ | PySpark | **QUARANTINE** (`INVALID_TXN_AMOUNT`) |
| **Silver** | Referential Integrity | `transactions.account_id` must exist in `silver.loans` | PySpark | **QUARANTINE** (`ORPHAN_ACCOUNT_ID`) |
| **Silver** | Referential Integrity | `loans.branch_code` & `employees.branch_code` in active branches | PySpark | **QUARANTINE** (`INVALID_BRANCH_FK`) |
| **Silver** | Catalog Uniqueness | 1 distinct rate per product code | PySpark | **QUARANTINE** clashing rates |
| **Gold** | Star Schema Integrity | PKs unique, FKs match dimensional parent records | `dbt test` | **FAIL RUN** (33 tests enforce) |
| **Recon** | Ledger Balance | $\sum \text{Silver Loans} == \sum \text{Gold Risk Fact}$ to 0.00 rupee | Python Audit | **CIRCUIT BREAKER** |
