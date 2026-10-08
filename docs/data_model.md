# Data Model

## Source Layer (Bronze)

**Ingestion Strategy:** One raw table per source CSV file, loaded into a `bronze` schema.

**What we decided NOT to change, and why:**
* **No Type Casting:** All business columns are ingested as raw `TEXT`. Profiling revealed inconsistent date formats (ISO, MM/YYYY, DD-MON-YYYY) and quoted thousands-separators (e.g., `"1,984.47"` in `loan_applications.csv`). Forcing type casting on ingestion would cause silent record drops.
* **No Record Filtering:** Malformed rows are retained in Bronze. 
* **Augmentation:** We strictly append audit metadata (`_batch_id`, `_source_file`, `_loaded_at`) to enable data lineage and replayability.
* **Parsing constraint:** Ingestion utilizes strict RFC-4180 parsing to safely handle embedded commas within quoted numerics and string fields.

## Conformed / Integrated Layer (Silver)

Silver is not a 1:1 copy of Bronze. It represents distinct, deduplicated, and conformed business entities (`silver.branches`, `silver.products`, `silver.customers`, `silver.loans`, `silver.transactions`).

**Handling Ambiguities & Integrations:**
* **Join Keys & Cleaning:** Profiling identified casing drift across regional extracts (e.g., `br-027` vs `BR-027`). All natural join keys (`branch_code`, `product_code`, `customer_id`) are standardized using `UPPER(TRIM(key_col))` before joining.
* **Duplicate-Key Conflicts:** `product_master` contains primary key collisions (e.g., `GL01` appearing twice with conflicting interest rates of 9.75% and 9.80%). 
  * *Resolution:* Silver enforces a deduplication strategy (e.g., `ROW_NUMBER()` prioritizing the latest timestamp or highest rate based on business rules). The discarded duplicate is routed to `silver.quarantine_records`.
* **Orphans (Matched Nothing):** 
  * `branch_master` contains 88 branches, but `branch_financials` only contains 85. 
  * *Resolution:* We do not drop the 3 missing branches. They are retained in `silver.branches`. In the Gold layer, fact tables will use a `LEFT JOIN` from the branch dimension, flagging missing financial periods with `financials_missing = TRUE`.
  * Truly orphaned transactions (e.g., a loan transaction for an `account_id` not in `loan_accounts`) are logged to `silver.quarantine_records` to prevent silent financial misstatements.
* **Fan-Out Prevention:** Before any left join (e.g., joining `region_demographics` to `branches`), the right-side table is explicitly asserted for uniqueness on the join key to prevent Cartesian explosions on transactional facts.

## Consumption Layer (Gold)

Designed as a dimensional star schema to directly power the Leadership Dashboard API endpoints.

**Dimensions:**
* `gold.dim_branch`: Answers geographic and organizational tiering. 
* `gold.dim_product`: Answers category classifications (Personal, Gold, MSME).
* `gold.dim_branch_tier_benchmark`: Answers **"What staffing and risk profile a new branch should be sized for"** (derived from `employee_roster` headcount averages per tier).

**Facts:**
* `gold.fact_branch_monthly_pnl`: 
  * *Answers:* **"Which existing branches are performing best?"**
  * *Derivation:* Combines `branch_financials` with `dim_branch` to compute `net_contribution = interest_income - operating_cost`.
* `gold.fact_regional_loan_demand`: 
  * *Answers:* **"How loan/deposit product demand varies by region."**
  * *Derivation:* Aggregates `loan_transactions` (Disbursements only) by `region` and `product_category`.
* `gold.fact_regional_risk_profile`: 
  * *Answers:* **"Where portfolio risk is concentrated geographically."**
  * *Derivation:* Aggregates `loan_accounts` balances, calculating `npa_rate` (default vs total book) and assigning RAG status thresholds.

## Keys, Types, and Grain

Downstream API consumers and reporting tools must respect the following grains and constraints:

* **Surrogate Keys:** The Gold layer utilizes surrogate keys (e.g., `branch_key` UUID/Integer) instead of natural keys (`branch_code`). This protects the reporting layer if legacy core-banking systems alter their branch code formats in the future.
* **Grains:**
  * `fact_branch_monthly_pnl`: 1 row per `branch_key` per `reporting_month`.
  * `fact_regional_loan_demand`: 1 row per `region` per `product_key` per `quarter/month`.
  * `fact_regional_risk_profile`: 1 row per `region` snapshot.
* **Nullability Notes:** 
  * `approved_amount` in loan applications is explicitly **NULLABLE**. Pending applications will not have an approved amount; downstream average ticket size calculations must strictly use `NULL` rather than coercing to `0.00` to avoid skewing the denominator.
  * `net_contribution` in `fact_branch_monthly_pnl` may be `NULL` for newly launched branches where legacy P&L extracts have not yet been generated.