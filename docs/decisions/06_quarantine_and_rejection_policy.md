# ADR 006: Quarantine Ledger & Zero-Drop Rejection Policy

## Status
Accepted

## Context
During initial Bronze-to-Silver conformation, thousands of records violated referential integrity or domain validity (e.g., transactions referencing nonexistent accounts, employees mapped to ghost branches, and blank repayment amounts).

## Decision
Adopt a strict **Zero-Drop Quarantine Policy**:
1. No invalid row is silently filtered, deleted, or defaulted.
2. Defective records are routed to `silver.quarantine` with immutable audit fields: `quarantine_id`, `source_table`, `record_identifier`, `rejection_code`, `raw_payload`, and `quarantined_at`.
3. Exactly 5,226 records were successfully quarantined across 4 distinct rejection codes (`INVALID_BRANCH_FK`, `ORPHAN_ACCOUNT_ID`, `INVALID_TXN_AMOUNT`, `PRODUCT_RATE_CONFLICT`).

## Alternatives Considered
- **Silent Filtering (`WHERE account_id IS NOT NULL`):** Violates banking auditability; leads to unexplained discrepancies between source extracts and served data.
- **Default Imputation (assigning orphan transactions to a dummy loan):** Corrupts financial ledger balances and distorts credit risk accounting.

## Consequences
- **Positive:** Complete visibility into upstream source system errors; allows business operations to remediate bad records without data loss.
- **Trade-Off:** Requires maintaining secondary storage and quarantine monitoring scripts.
