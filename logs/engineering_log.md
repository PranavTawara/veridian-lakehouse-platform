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