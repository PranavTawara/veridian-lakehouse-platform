# Low-Level Design (LLD): Data Model & Interface Contracts

## 1. Operational Source Layer (`PostgreSQL: source`)
Represents the core banking transactional database. 11 relational tables loaded losslessly as text to preserve original incoming payloads[cite: 4].

* **Schema:** `source`[cite: 4]
* **Tables:** `branch_master`, `branch_financials`, `product_master`, `customer_master`, `deposit_accounts`, `employee_roster`, `loan_accounts`, `loan_applications`, `loan_transactions`, `region_demographics`, `account_status_events`.
* **Storage Contract:** All columns stored as `TEXT`[cite: 1, 4]. Quoted commas, currency symbols (`$`, `₹`), 13-digit Unix millisecond epochs, and whitespace are retained untouched[cite: 4].

---

## 2. Lakehouse Medallion Layers

### 2.1 Bronze Landing Layer (`lakehouse/bronze/`)
* **Storage Engine:** Snappy-compressed columnar Parquet[cite: 4].
* **Partition Scheme:** `lakehouse/bronze/{entity}/snapshot_date={YYYY-MM-DD}/data.parquet`[cite: 4]
* **Ingestion Metadata Appended:**
  * `_batch_id` (`VARCHAR`): Pipeline run UUID[cite: 4].
  * `_source_table` (`VARCHAR`): Origin table name in `source` schema[cite: 4].
  * `_extracted_at` (`TIMESTAMPTZ`): UTC timestamp when data was extracted[cite: 4].
* **Contract:** 1:1 lossless schema replication from `source.*`[cite: 4]. Zero filtering or type casting[cite: 1, 4].

---

### 2.2 Silver Conformed Layer (`lakehouse/silver/`)
Silver resolves schema drift, standardizes timestamps, deduplicates primary keys, and diverts invalid records to quarantine[cite: 4].

#### Entity Specifications & Cleaning Rules:

1. **`silver.products`**:
   * Natural Key: `product_code` (`VARCHAR`, PK)[cite: 4].
   * Columns: `product_code`, `product_name`, `category`, `interest_rate` (`NUMERIC(5,2)`).
   * **Rule 1.1 (Missing Key):** Impute surrogate code `TL01` for the record with missing `product_code` (`product_name = 'Two-Wheeler Loan'`).
   * **Rule 1.2 (Collision Resolution):** Casing collisions on `PL01` and `ML01` are unified via `UPPER(TRIM(product_code))`. For `GL01`, conflicting interest rates (9.75% vs 9.80%) are resolved by selecting the record with the highest rate (9.80%) via `ROW_NUMBER() OVER (PARTITION BY UPPER(TRIM(product_code)) ORDER BY interest_rate DESC)`[cite: 4]. The conflicting 9.75% record is routed to quarantine[cite: 4].

2. **`silver.customers`**:
   * Natural Key: `customer_id` (`VARCHAR`, PK)[cite: 4].
   * Columns: `customer_id`, `customer_name`, `city`, `state`, `kyc_status`, `income_band`, `onboarding_date` (`DATE`).
   * **Rule 2.1 (SCD Type 1 Deduplication):** Resolves 2,740 conflicting duplicate rows by ordering by `onboarding_date DESC` and prioritizing `kyc_status = 'VERIFIED'`.
   * **Rule 2.2 (Text Normalization):** Strips non-ASCII unicode artifacts and leading/trailing spaces from `customer_name` using NFKD normalization.

3. **`silver.branches`**:
   * Natural Key: `branch_code` (`VARCHAR`, PK)[cite: 4].
   * Columns: `branch_code`, `branch_name`, `region`, `state`, `city`, `branch_type`, `open_date` (`DATE`).
   * **Rule 3.1 (Ghost Branch Isolation):** Ghost branches `BR-999` and `BR-0XX` are rejected and routed to quarantine.
   * **Rule 3.2 (Casing Normalization):** Normalizes natural key via `UPPER(TRIM(branch_code))` resolving `br-001` vs `BR-001`[cite: 4].

4. **`silver.loans`**:
   * Natural Key: `account_id` (`VARCHAR`, PK)[cite: 4].
   * Columns: `account_id`, `application_id`, `branch_code`, `product_code`, `principal_amount` (`NUMERIC(14,2)`), `outstanding_amount` (`NUMERIC(14,2)`), `dpd` (`INTEGER`), `npa_flag` (`VARCHAR`), `is_regulatory_npa` (`BOOLEAN`), `open_date` (`DATE`), `last_updated` (`TIMESTAMPTZ`).
   * **Rule 4.1 (Numeric Sanitization):** Strips commas and quotes from `principal_amount` and `outstanding_amount` prior to casting[cite: 4].
   * **Rule 4.2 (Regulatory NPA Derivation):** Overrides source `npa_flag = 'N'` by deriving:
     `is_regulatory_npa = CASE WHEN CAST(dpd AS INTEGER) >= 90 OR UPPER(TRIM(npa_flag)) IN ('Y', 'YES', '1') THEN TRUE ELSE FALSE END`.
   * **Rule 4.3 (Timestamp Normalization):** Converts 13-digit Unix millisecond epochs (`1754384400000`) and ISO strings to standardized UTC `TIMESTAMPTZ`.

5. **`silver.applications`**:
   * Natural Key: `application_id` (`VARCHAR`, PK).
   * Columns: `application_id`, `branch_code`, `customer_id`, `product_code`, `applied_amount` (`NUMERIC(14,2)`), `approved_amount` (`NUMERIC(14,2)`), `status` (`VARCHAR`), `application_date` (`DATE`).
   * **Rule 5.1 (Referential Quarantine):** Diverts 2,202 applications referencing nonexistent `customer_id`s to quarantine (`ORPHAN_CUSTOMER_ID`).
   * **Rule 5.2 (Approval Limit Imputation):** For 719 `DISBURSED` applications with null or zero `approved_amount`, backfills limit from matching `loan_accounts.principal_amount`. If unresolvable, routes to quarantine.
   * **Rule 5.3 (Date Standardization):** Parses alphanumeric (`October 05, 2025`) and slash formats to `DATE`.

6. **`silver.transactions`**:
   * Natural Key: `txn_id` (`VARCHAR`, PK)[cite: 4].
   * Columns: `txn_id`, `account_id`, `application_id`, `branch_code`, `txn_type`, `clean_amount` (`NUMERIC(14,2)`), `is_reversal` (`BOOLEAN`), `txn_timestamp` (`TIMESTAMPTZ`).
   * **Rule 6.1 (Deduplication):** Removes 1,255 exact duplicate transactions using `ROW_NUMBER() OVER (PARTITION BY UPPER(TRIM(txn_id)) ORDER BY txn_timestamp DESC)`[cite: 4].
   * **Rule 6.2 (Orphan Quarantine):** Anti-joins against `silver.loans`; diverts 1,002 orphan transactions referencing nonexistent accounts to quarantine (`ORPHAN_ACCOUNT_ID`)[cite: 4].
   * **Rule 6.3 (Currency & Reversal Parsing):** Strips `$`, `₹`, and commas from `amount`[cite: 4]. Sets `is_reversal = TRUE` and `clean_amount = ABS(amount)` for the 676 negative values.
   * **Rule 6.4 (Geographic Deprecation):** Deprecates `loan_transactions.region`; regional attribution is strictly derived via `dim_branch`.

7. **`silver.quarantine_records`**:
   * Storage: `lakehouse/silver/quarantine/quarantine_records.parquet`[cite: 4].
   * Attributes: `quarantine_id` (`UUID`), `source_table` (`VARCHAR`), `record_identifier` (`VARCHAR`), `rejection_code` (`VARCHAR`), `raw_payload` (`TEXT`), `quarantined_at` (`TIMESTAMPTZ`)[cite: 4].
   * Standard Rejection Codes: `ORPHAN_ACCOUNT_ID`, `ORPHAN_CUSTOMER_ID`, `GHOST_BRANCH`, `PRODUCT_RATE_CONFLICT`, `DISBURSED_WITHOUT_LIMIT`[cite: 4].

---

### 2.3 Gold Consumption Layer (`lakehouse/gold/`)
Dimensional star schema models serving executive analytics[cite: 1, 4].

#### Dimensional Models:
* **`dim_branch`**:
  * Grain: 1 row per physical branch (Surrogate PK: `branch_key`)[cite: 2, 4].
  * Attributes: `branch_key`, `branch_code`, `branch_name`, `region`, `state`, `city`, `branch_type`, `open_date`[cite: 4].
* **`dim_product`**:
  * Grain: 1 row per loan product (Surrogate PK: `product_key`)[cite: 2, 4].
  * Attributes: `product_key`, `product_code`, `product_name`, `category`, `interest_rate`[cite: 4].
* **`dim_branch_tier_benchmark`**:
  * Grain: 1 row per branch type/tier[cite: 1, 4].
  * Attributes: `branch_type`, `avg_staff_count`, `target_disbursement_volume`, `npa_threshold_pct`[cite: 4].

#### Fact Models:
* **`fact_branch_monthly_pnl`**:
  * Grain: 1 row per `branch_key` per `reporting_month` (`YYYY-MM-01`)[cite: 2, 4].
  * Metrics: `interest_income`, `operating_cost`, `net_contribution` (`interest_income - operating_cost`), `financials_missing` (`BOOLEAN`)[cite: 1, 4].
  * Branch Retention Rule: `dim_branch LEFT JOIN branch_financials`. Preserves all 88 branches; branch `BR-047` is populated with `NULL` financial metrics and flagged with `financials_missing = TRUE`[cite: 4, 5].
* **`fact_regional_loan_demand`**:
  * Grain: 1 row per conformed `region` per product `category` per `reporting_month`[cite: 1, 2, 4].
  * Metrics: `total_disbursed_volume`, `application_count`, `avg_approved_ticket_size`[cite: 4].
* **`fact_regional_risk_profile`**:
  * Grain: 1 row per conformed `region` snapshot[cite: 2, 4].
  * Metrics: `total_book_balance`, `npa_balance` (evaluated against `is_regulatory_npa`), `npa_rate_pct` (`npa_balance / total_book_balance * 100`), `rag_status` (`GREEN` if $<3\%$, `AMBER` if $3\%-5\%$, `RED` if $>5\%$)[cite: 2, 4].

---

## 3. Serving Layer & Data Mart Contracts (`PostgreSQL: serving`)

Synchronizes Gold models into indexed PostgreSQL tables under the `serving` schema[cite: 4]:
* `serving.dim_branch`[cite: 4]
* `serving.dim_product`[cite: 4]
* `serving.dim_branch_tier_benchmark`[cite: 4]
* `serving.fact_branch_monthly_pnl`[cite: 4]
* `serving.fact_regional_loan_demand`[cite: 4]
* `serving.fact_regional_risk_profile`[cite: 4]

### API Endpoint Mapping

| API Endpoint | Query Source | Query Filter / Logic |
| :--- | :--- | :--- |
| `GET /api/v1/branches/profitability`[cite: 4] | `serving.fact_branch_monthly_pnl` ⨝ `serving.dim_branch`[cite: 4] | Returns branch net contribution; preserves branch `BR-047` with missing financial flag[cite: 4, 5]. |
| `GET /api/v1/regions/loan-demand`[cite: 4] | `serving.fact_regional_loan_demand`[cite: 4] | Aggregates demand volume and ticket sizes by conformed branch region[cite: 4]. |
| `GET /api/v1/regions/risk-profile`[cite: 4] | `serving.fact_regional_risk_profile`[cite: 4] | Returns NPA ratios and assigned RAG indicators based on regulatory DPD logic[cite: 4]. |
| `GET /api/v1/benchmarks/tiers`[cite: 4] | `serving.dim_branch_tier_benchmark`[cite: 4] | Returns staffing and operational sizing benchmarks[cite: 4]. |