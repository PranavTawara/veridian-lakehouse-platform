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