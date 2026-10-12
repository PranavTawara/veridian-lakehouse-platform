# Low-Level Design (LLD): Data Model & Interface Contracts

## 1. Operational Source Layer (`PostgreSQL: source`)
Represents the core-banking transactional database. Raw CSVs are loaded directly into corresponding tables under the `source` schema with column names standardized to snake_case.

* **Schema:** `source`
* **Tables:** `source.branch_master`, `source.branch_financials`, `source.product_master`, `source.customer_master`, `source.loan_applications`, `source.loan_accounts`, `source.loan_transactions`, `source.region_demographics`, `source.employee_roster`, `source.audit_log`, `source.cgt_rules`.
* **Storage Contract:** Unmodified text types (`TEXT`) to prevent type coercion drops or silent precision loss during initial landing.

---

## 2. Lakehouse Medallion Layers

### 2.1 Stage / Bronze Layer (`lakehouse/bronze/`)
* **Storage Format:** Snappy-compressed columnar Parquet.
* **Partition Scheme:** `lakehouse/bronze/{source_table}/snapshot_date={YYYY-MM-DD}/data.parquet`
* **Ingestion Metadata Appended:**
  * `_batch_id` (`VARCHAR`): Unique UUID per pipeline execution.
  * `_source_table` (`VARCHAR`): Origin table name in `source` schema.
  * `_extracted_at` (`TIMESTAMPTZ`): System timestamp when data was pulled from Postgres.
* **Contract:** 1:1 lossless schema replication from `source.*`. Malformed rows and quoted strings are retained untouched.

---

### 2.2 Conformed / Integrated Layer (`lakehouse/silver/`)
Silver resolves schema drift, sanitizes malformed data, deduplicates primary keys, and conforms enterprise entities.

* **Storage Format:** Snappy-compressed Parquet under `lakehouse/silver/`.

#### Cleaned Entity Contracts:
1. **`silver.branches`**:
   * `branch_code` (`VARCHAR`, PK): Normalized via `UPPER(TRIM(branch_code))` to resolve case inconsistencies (e.g., `br-027` vs `BR-027`).
   * `branch_name` (`VARCHAR`), `region_id` (`VARCHAR`), `tier` (`VARCHAR`), `opened_date` (`DATE`).
2. **`silver.products`**:
   * `product_code` (`VARCHAR`, PK): Deduplicated using `ROW_NUMBER() OVER (PARTITION BY UPPER(TRIM(product_code)) ORDER BY effective_date DESC, interest_rate DESC)` to resolve collisions (e.g., `GL01`, `PL01`, `ML01`). Conflicting duplicate entries are routed to quarantine.
   * `product_name` (`VARCHAR`), `product_category` (`VARCHAR`), `interest_rate` (`NUMERIC(5,2)`).
3. **`silver.customers`**:
   * `customer_id` (`VARCHAR`, PK), `credit_score` (`INTEGER`), `annual_income` (`NUMERIC(14,2)`), `kyc_status` (`VARCHAR`).
4. **`silver.loans`**:
   * `account_id` (`VARCHAR`, PK), `customer_id` (`VARCHAR`), `product_code` (`VARCHAR`), `branch_code` (`VARCHAR`).
   * `disbursed_amount` (`NUMERIC(14,2)`): Stripped of thousands-separator commas (e.g., `"1,984.47"` -> `1984.47`).
   * `current_balance` (`NUMERIC(14,2)`), `loan_status` (`VARCHAR`), `disbursal_date` (`DATE`).
5. **`silver.transactions`**:
   * `transaction_id` (`VARCHAR`, PK), `account_id` (`VARCHAR`), `transaction_date` (`DATE`), `amount` (`NUMERIC(14,2)`), `transaction_type` (`VARCHAR`).
6. **`silver.quarantine_records`**:
   * Storage: `lakehouse/silver/quarantine/quarantine_log.parquet`.
   * Captures rejected rows: unresolvable duplicate keys, orphan transactions where `account_id` does not exist in `silver.loans`, and corrupted timestamps.
   * Attributes: `quarantine_id`, `source_table`, `record_identifier`, `rejection_reason`, `raw_payload`, `quarantined_at`.

---

### 2.3 Consumption Layer (`lakehouse/gold/`)
Star schema dimensional tables optimized to answer core business questions.

#### Dimensional Models:
* **`dim_branch`**:
  * Grain: 1 row per physical branch.
  * Attributes: `branch_key` (UUID / Surrogate PK), `branch_code` (Natural Key), `branch_name`, `region_id`, `region_name`, `tier`, `opened_date`.
* **`dim_product`**:
  * Grain: 1 row per loan product.
  * Attributes: `product_key` (Surrogate PK), `product_code`, `product_name`, `product_category`, `base_interest_rate`.
* **`dim_branch_tier_benchmark`**:
  * Grain: 1 row per branch tier (`Metro`, `Urban`, `Semi-Urban`, `Rural`).
  * Attributes: `tier`, `avg_staff_count`, `target_loan_disbursement`, `risk_threshold_npa_pct`.

#### Fact Models:
* **`fact_branch_monthly_pnl`**:
  * Grain: 1 row per `branch_key` per `reporting_month` (`YYYY-MM`).
  * Metrics: `interest_income`, `fee_income`, `operating_cost`, `net_contribution` (`interest_income + fee_income - operating_cost`).
  * Missing Branch Logic: Joined via `dim_branch LEFT JOIN branch_financials`. All 88 branches are retained. If a branch has no financial record (the 3 missing branches), financials are set to `NULL` and `financials_missing = TRUE`.
* **`fact_regional_loan_demand`**:
  * Grain: 1 row per `region_id` per `product_category` per `reporting_month`.
  * Metrics: `total_disbursed_volume`, `loan_application_count`, `avg_approved_ticket_size`.
* **`fact_regional_risk_profile`**:
  * Grain: 1 row per `region_id` per snapshot date.
  * Metrics: `total_book_balance`, `npa_balance`, `npa_rate` (`(npa_balance / total_book_balance) * 100`), `rag_status` (`GREEN` if <3%, `AMBER` if 3%-5%, `RED` if >5%).

---

## 3. Serving Layer & Data Mart Contracts (`PostgreSQL: serving`)

The Gold Parquet models are synchronized into indexed PostgreSQL tables under the `serving` schema:

* `serving.dim_branch`
* `serving.dim_product`
* `serving.dim_branch_tier_benchmark`
* `serving.fact_branch_monthly_pnl`
* `serving.fact_regional_loan_demand`
* `serving.fact_regional_risk_profile`

### API Endpoint Mapping

| API Endpoint | Query Source | Query Filter / Logic |
| :--- | :--- | :--- |
| `GET /api/v1/branches/profitability` | `serving.fact_branch_monthly_pnl` ⨝ `serving.dim_branch` | Evaluates net contribution by branch and filters active branches missing P&L. |
| `GET /api/v1/regions/loan-demand` | `serving.fact_regional_loan_demand` | Aggregates volume and ticket sizes by region and category. |
| `GET /api/v1/regions/risk-profile` | `serving.fact_regional_risk_profile` | Returns NPA ratios and assigned RAG indicators for map/table widgets. |
| `GET /api/v1/benchmarks/tiers` | `serving.dim_branch_tier_benchmark` | Returns headcount and operational sizing benchmarks. |