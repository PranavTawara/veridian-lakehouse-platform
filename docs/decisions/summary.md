# Executive Summary of Key Engineering Decisions & Trade-Offs

### Decisions I Would Defend
1. **Zero-Drop Quarantine Over Silent Dropping:** Rather than filtering out invalid rows or defaulting them, we routed all 5,226 defective records into an audited quarantine table with structured rejection codes (`INVALID_BRANCH_FK`, `ORPHAN_ACCOUNT_ID`, `INVALID_TXN_AMOUNT`, `PRODUCT_RATE_CONFLICT`). This ensures complete transparency, auditability, and zero corruption of the general ledger.
2. **Dual-Horizon Delinquency Modeling:** When cross-system reconciliation revealed that `NPA_TAGGED` events were legacy pre-disbursement underwriting artifacts rather than active defaults, we refused to fabricate an arbitrary ~32.5% default rate across all regions. We enforced the regulatory banking standard (0.00% NPA at DPD >= 90) while surfacing the 15.1% early-stage watchlist (SMA-0: 1–30 DPD). This gave leadership actionable geographic risk contrast without misrepresenting portfolio solvency.
3. **Automated Cross-Layer Financial Ledger Reconciliation:** We enforced strict mathematical parity between the conformed Silver loan book and the Gold serving mart down to the exact rupee (₹461,614,655.81).
4. **Deterministic Multi-Format Date Parsing:** Conformed 517 Indian/UK slash dates (`dd/MM/yyyy`) without resorting to synthetic placeholder dates or dropping customers.

### Trade-Offs Accepted
1. **PostgreSQL Serving Layer vs. Distributed MPP Warehouse:** For local execution and zero-cloud spend, PostgreSQL served as our Gold serving layer. While ideal for current volumes, scaling to billions of rows would require transitioning to Snowflake, BigQuery, or Amazon Redshift.
2. **Static RAG Calibration on Early Delinquency:** Because the baseline `initial_load` had a maximum DPD of 30 days, RAG status was calibrated against early watchlist density (14.5%–15.5%) rather than regulatory NPA. This was necessary to provide immediate regional contrast for expansion planning.

### What I Would Do Differently With More Time
1. **Streaming / CDC Ingestion:** Implement Change Data Capture (Debezium + Kafka) to stream transactions continuously rather than batch file extracts.
2. **Automated Quarantine Remediation Pipelines:** Create a write-back API allowing branch managers to correct orphan accounts or input missing repayment values, automatically reprocessing quarantined records.
3. **Predictive Roll-Rate Migration Modeling:** Train a transition probability matrix (Markov chain) on loan aging to forecast the percentage of SMA-0 accounts likely to deteriorate into NPA in subsequent quarters.
