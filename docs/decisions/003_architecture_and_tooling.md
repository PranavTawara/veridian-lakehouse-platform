# Architecture Decision Record 003: Empirical Decoupled Lakehouse Architecture (HLD)

## Status
Proposed (Awaiting Review)

## Context
Initial data profiling across the 11 raw transactional CSV files in `initial_load/` revealed severe data quality traps, relational anomalies, and scale discrepancies[cite: 4, 14, 16]:
- **Macro Ledger Variance:** A ₹135.5B discrepancy between raw disbursements in `loan_transactions` (₹141.4B) and principal in `loan_accounts` (₹5.88B), driven by currency mixing (`$`, `₹`), 1,255 duplicate transactions, and 1,002 orphan accounts.
- **Regulatory Divergence:** 100% of rows in `loan_accounts` default to `npa_flag = 'N'` despite hundreds of accounts exceeding 90 Days Past Due (DPD).
- **Master Entity Collisions:** 2,740 conflicting duplicate `customer_id` rows (0 exact duplicates), primary key collisions in `product_master` (conflicting rates on `GL01` and missing key on Two-Wheeler), and ghost branches (`BR-999`, `BR-0XX`).
- **State Machine Artifacts:** 2,594 apparent status rollbacks caused by lexicographically sorting mixed 13-digit Unix millisecond epochs against ISO timestamps.
- **Geographic Drift:** 424 transactions with region values that contradict `branch_master`.

Executing transformations, deduplication, and joins directly within an operational transactional database (OLTP) would cause extreme IOPS exhaustion, connection starvation, and table deadlocks[cite: 3]. The architecture must physically decouple transactional storage from analytical processing while remaining fully containerized and reproducible locally[cite: 3, 4].

## Architecture Topology

┌───────────────────────────────────────────────────────────────────────┐
│ 1. Raw Extracts: 11 CSV files in initial_load/                        │
└──────────────────────────────────┬────────────────────────────────────┘
                                   │
                                   ▼
┌───────────────────────────────────────────────────────────────────────┐
│ 2. Operational Source Database: PostgreSQL 16 (source.* schema)       │
└──────────────────────────────────┬────────────────────────────────────┘
                                   │ (Batch extract to Parquet)
                                   ▼
┌───────────────────────────────────────────────────────────────────────┐
│ 3. Decoupled Analytical Lakehouse (Columnar Parquet Storage)          │
│                                                                       │
│   ┌────────────────┐      ┌────────────────┐      ┌───────────────┐   │
│   │  Bronze (Raw)  │ ───► │ Silver (Clean) │ ───► │  Gold (Star)  │   │
│   └────────────────┘      └───────┬────────┘      └───────────────┘   │
└───────────────────────────────────┼───────────────────────────────────┘
                                    │ (Failed checks & orphans)
                                    ▼
                           ┌──────────────────┐
                           │ Quarantine Store │
                           └──────────────────┘
                                    │
                                    ▼ (Sync Gold facts & dims)
┌───────────────────────────────────────────────────────────────────────┐
│ 4. Serving Data Mart: PostgreSQL 16 (serving.* schema)                │
└──────────────────────────────────┬────────────────────────────────────┘
                                   │
                                   ▼
┌───────────────────────────────────────────────────────────────────────┐
│ 5. Reporting Layer: FastAPI REST Endpoints ──► Executive Dashboard UI │
└───────────────────────────────────────────────────────────────────────┘

## Architectural Boundaries & Component Responsibilities

### 1. Operational Source System (`CSV -> PostgreSQL: source`)
* **Engine:** PostgreSQL 16 container (`source` schema)[cite: 3].
* **Role:** Emulates Veridian's core banking transactional OLTP cluster prior to analytical extraction[cite: 3, 4].
* **Storage Contract:** 11 relational tables matching core entities (`branch_master`, `loan_accounts`, `loan_transactions`, etc.)[cite: 1, 4]. All business fields are ingested strictly as `TEXT` without casting or record filtering, guaranteeing 100% lossless ingestion of raw payloads (including currency symbols, quoted commas, and mixed date strings)[cite: 1, 4].
* **Production AWS Equivalent:** Amazon RDS PostgreSQL / Aurora Core Banking.

### 2. Analytical Lakehouse Storage & Compute (`PostgreSQL -> Lakehouse`)
Analytical workloads are decoupled from PostgreSQL compute using an isolated filesystem medallion architecture backed by Snappy-compressed columnar Parquet files[cite: 4]:

* **Bronze Landing Layer (`lakehouse/bronze/`):**
  * Read-optimized, append-only historical landing zone partitioned by extraction date: `lakehouse/bronze/{entity}/snapshot_date={YYYY-MM-DD}/data.parquet`[cite: 4].
  * Preserves raw string representation from `source.*`[cite: 4].
  * Appends audit columns: `_extracted_at` (UTC timestamp), `_batch_id` (UUID), and `_source_table`[cite: 4].
  * *Production AWS Equivalent:* Amazon S3 Raw Landing Bucket.

* **Silver Conformed & Quality Layer (`lakehouse/silver/`):**
  * Conformed, deduplicated, and strictly typed enterprise models (`branches.parquet`, `customers.parquet`, `loans.parquet`, `transactions.parquet`)[cite: 1, 4].
  * Normalizes casing (`UPPER(TRIM())`), sanitizes numeric strings (stripping `[$,₹",]`), and unifies 13-digit Unix millisecond epochs and ISO strings into standardized UTC timestamps[cite: 4].
  * Derives regulatory compliance metrics (`is_regulatory_npa = (dpd >= 90)`) rather than trusting corrupted source flags.
  * Implements an isolated quarantine dataset (`lakehouse/silver/quarantine/quarantine_records.parquet`) where all unresolvable duplicate collisions, ghost branch records (`BR-999`, `BR-0XX`), and orphan foreign keys (1,002 transactions and 2,202 applications) are routed with rejection codes and timestamps[cite: 4].
  * Enforces Kimball's conformed dimension rule: transactional region fields are deprecated in favor of `dim_branch.region`.
  * *Production AWS Equivalent:* AWS Glue / EMR PySpark processing Apache Iceberg or Delta Lake tables on S3[cite: 3, 4].

* **Gold Consumption Layer (`lakehouse/gold/`):**
  * Star schema dimensional models optimized for BI aggregations and executive dashboards[cite: 1, 4].
  * Preserves all 88 physical branches via `dim_branch LEFT JOIN branch_financials`, explicitly populating missing financials for branch `BR-047` with `NULL` and `financials_missing = TRUE`[cite: 4].
  * Reconciles financial metrics to ensure disbursed loan volumes balance against account principals within strict regulatory thresholds.
  * *Production AWS Equivalent:* Amazon Redshift Serverless / Athena federated tables.

### 3. Serving Layer & Consumption (`Lakehouse -> PostgreSQL: serving -> API`)
* **Serving Data Mart (`PostgreSQL: serving` schema):**
  * Synchronizes Gold star schema models into indexed PostgreSQL tables[cite: 4]. B-tree indexes are applied on surrogate keys (`branch_key`, `product_key`) and reporting partitions to guarantee sub-millisecond API response times[cite: 2, 4].
  * *Production AWS Equivalent:* Amazon Redshift / Aurora Read Replica.
* **API Service:** FastAPI application exposing REST endpoints (`/api/v1/branches/profitability`, `/api/v1/regions/loan-demand`, `/api/v1/regions/risk-profile`)[cite: 3, 4].
* **Reconciliation Engine:** Automated data quality suite (`recon/`) asserting row count balances, non-negative bounds, and zero untracked orphan records across pipeline stages.

## Architectural Trade-offs & Justification

1. **Why decouple compute into Parquet rather than using PostgreSQL schemas?**
   Running iterative deduplication across 44k+ transactions, regex sanitization across multi-format currency columns, and large multi-table star schema joins directly on PostgreSQL degrades OLTP throughput and exhausts connection pools[cite: 3, 4]. Columnar Parquet read performance and DuckDB/PyArrow vectorization offer 10x-50x faster analytical scans without impacting operational systems.

2. **Why route bad records to Quarantine instead of dropping or failing?**
   Dropping 1,002 orphan transactions or 2,202 applications silently introduces untraceable financial misstatements[cite: 4]. Failing the pipeline completely causes daily operational halts[cite: 4]. Quarantining preserves 100% auditability, guarantees business metrics remain accurate, and unblocks daily reporting[cite: 4].