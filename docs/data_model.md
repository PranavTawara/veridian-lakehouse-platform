# Low-Level Design (LLD): Data Model & Delta Table Contracts

## 1. Operational Source Layer (`PostgreSQL: source`)
Represents the core banking operational transactional database. 11 relational tables loaded losslessly as text to preserve original incoming payloads[cite: 4].

* **Schema:** `source`[cite: 4]
* **Tables:** `branch_master`, `branch_financials`, `product_master`, `customer_master`, `deposit_accounts`, `employee_roster`, `loan_accounts`, `loan_applications`, `loan_transactions`, `region_demographics`, `account_status_events`[cite: 4].
* **Storage Contract:** All columns defined as `TEXT`[cite: 1, 4]. Quoted commas, currency symbols (`$`, `₹`), 13-digit Unix millisecond epochs, and whitespace are retained untouched[cite: 4].
* **Audit Metadata Appended:** `_batch_id` (`VARCHAR`), `_loaded_at` (`TIMESTAMPTZ`), `_source_file` (`VARCHAR`)[cite: 1, 4].

---

## 2. Lakehouse Medallion Layers (Delta Lake Format)

### 2.1 Bronze Landing Layer (`lakehouse/bronze/`)
* **Storage Engine:** Delta Lake Open Table Format with `_delta_log/` transaction log[cite: 4, 19].
* **Table Paths:** `lakehouse/bronze/{entity}/` (e.g., `lakehouse/bronze/loan_accounts/`)[cite: 4]
* **Storage Mode:** Append-only ingestion via PySpark JDBC read[cite: 4].
* **Audit Metadata Appended:**
  * `_extracted_at` (`TIMESTAMPTZ`): UTC timestamp when data was pulled from PostgreSQL[cite: 4].
  * `_pipeline_batch_id` (`VARCHAR`): Execution batch UUID[cite: 4].
  * `_source_table` (`VARCHAR`): Source PostgreSQL table name[cite: 4].
* **Contract:** 1:1 lossless schema replication from `source.*`[cite: 4]. Zero filtering or destructive casting[cite: 1, 4].

---

### 2.2 Silver Conformed & Hygiene Layer (`lakehouse/silver/`)
Silver resolves schema drift, standardizes timestamps, deduplicates primary keys, enforces business rules, and isolates corrupt/orphan records into a quarantine dataset[cite: 4].

#### Entity Specifications & Transformation Contracts:

1. **`silver.products` (Delta Table: `lakehouse/silver/products/`)**:
   * Primary Key: `product_code` (`VARCHAR`, PK)[cite: 4].
   * Columns: `product_code`, `product_name`, `category`, `interest_rate` (`DECIMAL(5,2)`), `is_active` (`BOOLEAN`)[cite: 4].
   * **Rule 1.1 (Missing Key Imputation):** Impute surrogate code `TL01` for the record with missing `product_code` (`product_name = 'Two-Wheeler Loan'`).
   * **Rule 1.2 (Collision Resolution):** Casing collisions on `PL01` and `ML01` are normalized via `UPPER(TRIM(product_code))`. For `GL01`, conflicting interest rates (9.75% vs 9.80%) are resolved by selecting the record with the highest rate (9.80%) via `ROW_NUMBER() OVER (PARTITION BY UPPER(TRIM(product_code)) ORDER BY interest_rate DESC)`[cite: 4]. The discarded 9.75% record is routed to quarantine[cite: 4].

2. **`silver.customers` (Delta Table: `lakehouse/silver/customers/`)**:
   * Primary Key: `customer_id` (`VARCHAR`, PK)[cite: 4].
   * Columns: `customer_id`, `customer_name`, `city`, `state`, `kyc_status`, `income_band`, `onboarding_date` (`DATE`)[cite: 4].
   * **Rule 2.1 (SCD Type 1 Deduplication):** Resolves 2,740 conflicting duplicate rows by ordering by `onboarding_date DESC` and prioritizing `kyc_status = 'VERIFIED'`. Discarded historical profile versions are archived in `silver.customer_history`.
   * **Rule 2.2 (Text Sanitization):** Strips non-ASCII unicode artifacts and leading/trailing whitespace from `customer_name` using NFKD normalization.

3. **`silver.branches` (Delta Table: `lakehouse/silver/branches/`)**:
   * Primary Key: `branch_code` (`VARCHAR`, PK)[cite: 4].
   * Columns: `branch_code`, `branch_name`, `region`, `state`, `city`, `branch_type`, `open_date` (`DATE`)[cite: 4].
   * **Rule 3.1 (Ghost Branch Isolation):** Ghost branches `BR-999` and `BR-0XX` are rejected and diverted to quarantine.
   * **Rule 3.2 (Casing Normalization):** Normalizes natural key via `UPPER(TRIM(branch_code))` resolving `br-001` vs `BR-001`[cite: 4].

4. **`silver.loans` (Delta Table: `lakehouse/silver/loans/`)**:
   * Primary Key: `account_id` (`VARCHAR`, PK)[cite: 4].
   * Columns: `account_id`, `application_id`, `branch_code`, `product_code`, `principal_amount` (`DECIMAL(14,2)`), `outstanding_amount` (`DECIMAL(14,2)`), `dpd` (`INTEGER`), `npa_flag` (`VARCHAR`), `is_regulatory_npa` (`BOOLEAN`), `open_date` (`DATE`), `last_updated` (`TIMESTAMPTZ`)[cite: 4].
   * **Rule 4.1 (Numeric Sanitization):** Strips commas and quotes from `principal_amount` and `outstanding_amount` prior to casting[cite: 4].
   * **Rule 4.2 (Regulatory NPA Derivation):** Overrides source `npa_flag = 'N'` by deriving:
     $$\text{is\_regulatory\_npa} = \begin{cases} \text{TRUE} & \text{if } dpd \ge 90 \text{ or } UPPER(TRIM(npa\_flag)) \in ('Y', 'YES', '1') \\ \text{FALSE} & \text{otherwise} \end{cases}$$
   * **Rule 4.3 (Timestamp Normalization):** Converts 13-digit Unix millisecond epochs (`1754384400000`) and ISO strings to standardized UTC `TIMESTAMPTZ`.

5. **`silver.applications` (Delta Table: `lakehouse/silver/applications/`)**:
   * Primary Key: `application_id` (`VARCHAR`, PK).
   * Columns: `application_id`, `branch_code`, `customer_id`, `product_code`, `applied_amount` (`DECIMAL(14,2)`), `approved_amount` (`DECIMAL(14,2)`), `status` (`VARCHAR`), `application_date` (`DATE`).
   * **Rule 5.1 (Referential Quarantine):** Diverts 2,202 applications referencing nonexistent `customer_id`s to quarantine (`ORPHAN_CUSTOMER_ID`).
   * **Rule 5.2 (Approval Limit Imputation):** For 719 `DISBURSED` applications with null or zero `approved_amount`, backfills limit from matching `loan_accounts.principal_amount`. If unresolvable, routes to quarantine.
   * **Rule 5.3 (Date Standardization):** Parses alphanumeric (`October 05, 2025`) and slash formats to standard ISO `DATE`.

6. **`silver.transactions` (Delta Table: `lakehouse/silver/transactions/`)**:
   * Primary Key: `txn_id` (`VARCHAR`, PK)[cite: 4].
   * Columns: `txn_id`, `account_id`, `application_id`, `branch_code`, `txn_type`, `clean_amount` (`DECIMAL(14,2)`), `is_reversal` (`BOOLEAN`), `txn_timestamp` (`TIMESTAMPTZ`)[cite: 4].
   * **Rule 6.1 (Deduplication):** Removes 1,255 exact duplicate transactions using `ROW_NUMBER() OVER (PARTITION BY UPPER(TRIM(txn_id)) ORDER BY txn_timestamp DESC)`[cite: 4].
   * **Rule 6.2 (Orphan Quarantine):** Anti-joins against `silver.loans`; diverts 1,002 orphan transactions referencing nonexistent accounts to quarantine (`ORPHAN_ACCOUNT_ID`)[cite: 4].
   * **Rule 6.3 (Currency & Reversal Parsing):** Strips `$`, `₹`, and commas from `amount`[cite: 4]. Sets `is_reversal = TRUE` and `clean_amount = ABS(amount)` for the 676 negative values.
   * **Rule 6.4 (Geographic Deprecation):** Deprecates `loan_transactions.region`; regional attribution is strictly derived via conformed `dim_branch.region`.

7. **`silver.deposit_accounts` (Delta Table: `lakehouse/silver/deposit_accounts/`)**:
   * Composite Key: `(deposit_account_id, snapshot_date)` (PK).
   * Columns: `deposit_account_id`, `customer_id`, `branch_code`, `deposit_type`, `balance` (`DECIMAL(14,2)`), `snapshot_date` (`DATE`), `region` (`VARCHAR`).
   * **Rule 7.1 (Composite Snapshot Alignment):** Standardizes multi-format date strings (`Nov-2025`, `2025-11`, `11/2025`) to standard first-of-month `YYYY-MM-01`. Deduplicates 1,446 composite collisions by selecting the latest ingested record.
   * **Rule 7.2 (Referential Integrity):** Diverts 655 accounts referencing nonexistent customers to quarantine.

8. **`silver.quarantine_records` (Delta Table: `lakehouse/silver/quarantine/`)**:
   * Storage: Delta Lake table preserving all dropped or diverted records[cite: 4].
   * Schema: `quarantine_id` (`UUID`, PK), `source_table` (`VARCHAR`), `record_identifier` (`VARCHAR`), `rejection_code` (`VARCHAR`), `raw_payload` (`TEXT`), `quarantined_at` (`TIMESTAMPTZ`)[cite: 4].
   * Rejection Codes: `ORPHAN_ACCOUNT_ID`, `ORPHAN_CUSTOMER_ID`, `GHOST_BRANCH`, `PRODUCT_RATE_CONFLICT`, `DISBURSED_WITHOUT_LIMIT`, `COMPOSITE_GRAIN_COLLISION`[cite: 4].

---

### 2.3 Gold Consumption Layer (dbt & Delta Table Models)
Dimensional star schema models optimized for analytical queries and executive reporting[cite: 1, 4].

#### Dimensional Models (`dim_*`):
* **`dim_branch`**:
  * Grain: 1 row per physical branch (Surrogate PK: `branch_key`)[cite: 2, 4].
  * Attributes: `branch_key`, `branch_code`, `branch_name`, `region`, `state`, `city`, `branch_type`, `open_date`[cite: 4].
* **`dim_product`**:
  * Grain: 1 row per loan product (Surrogate PK: `product_key`)[cite: 2, 4].
  * Attributes: `product_key`, `product_code`, `product_name`, `category`, `interest_rate`[cite: 4].
* **`dim_branch_tier_benchmark`**:
  * Grain: 1 row per branch type/tier[cite: 1, 4].
  * Attributes: `branch_type`, `avg_staff_count`, `target_disbursement_volume`, `npa_threshold_pct`[cite: 4].

#### Fact Models (`fact_*`):
* **`fact_branch_monthly_pnl`**:
  * Grain: 1 row per `branch_key` per `reporting_month` (`YYYY-MM-01`)[cite: 2, 4].
  * Metrics: `interest_income`, `operating_cost`, `net_contribution` (`interest_income - operating_cost`), `financials_missing` (`BOOLEAN`)[cite: 1, 4].
  * Missing Branch Logic: `dim_branch LEFT JOIN branch_financials`. Preserves all 88 branches; branch `BR-047` is populated with `NULL` financial metrics and flagged with `financials_missing = TRUE`[cite: 4, 5].
* **`fact_regional_loan_demand`**:
  * Grain: 1 row per conformed `region` per product `category` per `reporting_month`[cite: 1, 2, 4].
  * Metrics: `total_disbursed_volume`, `application_count`, `avg_approved_ticket_size`[cite: 4].
* **`fact_regional_risk_profile`**:
  * Grain: 1 row per conformed `region` snapshot[cite: 2, 4].
  * Metrics: `total_book_balance`, `npa_balance` (evaluated against `is_regulatory_npa`), `npa_rate_pct` (`npa_balance / total_book_balance * 100`), `rag_status` (`GREEN` if $<3\%$, `AMBER` if $3\%-5\%$, `RED` if $>5\%$)[cite: 2, 4].

---

## 3. Serving Layer & Data Mart Contracts (`PostgreSQL: serving`)

Synchronizes Gold models into indexed PostgreSQL tables under the `serving` schema[cite: 4]:
* `serving.dim_branch` (Index: `branch_key`, `branch_code`)[cite: 4]
* `serving.dim_product` (Index: `product_key`, `product_code`)[cite: 4]
* `serving.dim_branch_tier_benchmark` (Index: `branch_type`)[cite: 4]
* `serving.fact_branch_monthly_pnl` (Composite Index: `branch_key, reporting_month`)[cite: 4]
* `serving.fact_regional_loan_demand` (Composite Index: `region, reporting_month`)[cite: 4]
* `serving.fact_regional_risk_profile` (Composite Index: `region`)[cite: 4]

### API Endpoint Mapping

| API Endpoint | Query Source | Query Filter / Logic |
| :--- | :--- | :--- |
| `GET /api/v1/branches/profitability`[cite: 4] | `serving.fact_branch_monthly_pnl` ⨝ `serving.dim_branch`[cite: 4] | Returns branch net contribution; preserves branch `BR-047` with missing financial flag[cite: 4, 5]. |
| `GET /api/v1/regions/loan-demand`[cite: 4] | `serving.fact_regional_loan_demand`[cite: 4] | Aggregates demand volume and ticket sizes by conformed branch region[cite: 4]. |
| `GET /api/v1/regions/risk-profile`[cite: 4] | `serving.fact_regional_risk_profile`[cite: 4] | Returns NPA ratios and assigned RAG indicators based on regulatory DPD logic[cite: 4]. |
| `GET /api/v1/benchmarks/tiers`[cite: 4] | `serving.dim_branch_tier_benchmark`[cite: 4] | Returns staffing and operational sizing benchmarks[cite: 4]. |

---

## 4. Day-2 Incremental Ingestion & Change Data Capture (CDC) Strategy

To process subsequent delta waves ("Wave 2") without full-platform rebuilds[cite: 4]:

1. **Bronze Append Lineage:**
   * PySpark extracts records where `_loaded_at > last_watermark_timestamp` from PostgreSQL `source`[cite: 4].
   * Data is appended to Bronze Delta tables (`mode="append"`), generating a new commit file in `_delta_log/` without overwriting historical batches[cite: 4, 19].

2. **Silver Atomic `MERGE INTO` (Upsert):**
   * **Immutable Transactions:** Appended with deduplication via Delta Lake condition:
     `WHEN NOT MATCHED THEN INSERT *`[cite: 4].
   * **Mutable Entities (Loans, Accounts, Customers):** Upserted by natural key using Delta Lake's native merge engine:
     ```sql
     MERGE INTO silver.loans AS target
     USING bronze_staged_delta AS source
     ON target.account_id = source.account_id
     WHEN MATCHED AND source.last_updated >= target.last_updated THEN 
       UPDATE SET *
     WHEN NOT MATCHED THEN 
       INSERT *
     ```
     This reflects lifecycle transitions (`ACTIVE` $\rightarrow$ `NPA` / `CLOSED`) while preserving audit history[cite: 4].

3. **Gold Partition Pruning (dbt Incremental Models):**
   * Fact models implement dbt incremental logic (`is_incremental()`):
     ```sql
     {{ config(materialized='incremental', unique_key=['branch_key', 'reporting_month']) }}
     SELECT ... FROM {{ ref('stg_branch_financials') }}
     {% if is_incremental() %}
       WHERE reporting_month >= (SELECT max(reporting_month) FROM {{ this }})
     {% endif %}
     ```
   * Only the affected monthly partitions are recalculated, leaving historical partitions untouched[cite: 4].

4. **Zero-Downtime Serving Sync (PostgreSQL UPSERT):**
   * Sync operations utilize `INSERT ... ON CONFLICT (unique_key) DO UPDATE`[cite: 4]. Downstream API queries continue executing with zero downtime or partial-read errors during batch syncs[cite: 4].

---

## 5. Automated Data Quality & Circuit-Breaker Matrix

| Layer | DQ Dimension | Test / Assertion Rule | Tool | Severity Action |
| :--- | :--- | :--- | :--- | :--- |
| **Bronze** | Ingestion Audit | `_batch_id` and `_extracted_at` must not be null[cite: 4] | PySpark | **HALT** pipeline execution[cite: 4]. |
| **Silver** | Uniqueness | `product_code` must be unique across catalog[cite: 4] | PySpark / Delta | **QUARANTINE** conflicting rate rows (`GL01`)[cite: 4]. |
| **Silver** | Completeness | `clean_amount` in transactions must not be null | PySpark | **QUARANTINE** null amount transactions. |
| **Silver** | Referential Integrity | `loan_transactions.account_id` must exist in `loans`[cite: 4] | PySpark | **QUARANTINE** 1,002 orphan transactions[cite: 4]. |
| **Silver** | Referential Integrity | `loan_applications.customer_id` must exist in `customers` | PySpark | **QUARANTINE** 2,202 orphan applications. |
| **Silver** | Domain Accuracy | If $\text{DPD} \ge 90 \implies \text{is\_regulatory\_npa} = \text{TRUE}$ | PySpark | **TRANSFORM** (derive regulatory status). |
| **Silver** | Validity | Strip currency symbols (`$`, `₹`) and quotes from numbers[cite: 4] | PySpark | **TRANSFORM** (clean numeric values)[cite: 4]. |
| **Gold** | Uniqueness | Primary keys (`branch_key`, `product_key`) must be unique[cite: 2, 4] | `dbt test` | **HALT** serving sync on failure[cite: 4]. |
| **Gold** | Referential Integrity | Fact table foreign keys must reference valid dimensions | `dbt test` | **HALT** serving sync on failure. |
| **Gold** | Completeness | Preserves all 88 branches via `LEFT JOIN` (flags `BR-047`)[cite: 4, 5] | `dbt test` | **WARN** (`financials_missing = TRUE` expected)[cite: 4, 5]. |
| **Recon** | Macro Reconciliation | Total Disbursements vs Principal variance $\le 0.01\%$ | Airflow Gate | **CIRCUIT BREAKER** (halts before serving sync)[cite: 4]. |