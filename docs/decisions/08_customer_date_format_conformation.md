# ADR 008: Multi-Format Deterministic Customer Date Parsing

## Status
Accepted

## Context
In `customer_master`, 517 customer onboarding dates were ingested in Indian/UK notation (`dd/MM/yyyy`, e.g., `05/07/2023`, `14/05/2024`), while the rest used standard ISO `yyyy-MM-dd`. Standard SQL `to_date(col, 'yyyy-MM-dd')` caused 517 records to evaluate to `NULL`, violating the non-null customer contract.

## Decision
Implemented a regex-guarded parsing engine in PySpark using `coalesce(to_date(col, 'yyyy-MM-dd'), to_date(col, 'd/M/yyyy'))`.

## Alternatives Considered
- **Imputing with Current Date / Default Date:** Distorts customer vintage metrics and cohort tenure analysis.
- **Quarantining Customers with Slash Dates:** Drops 517 legitimate customer profiles and breaks referential integrity for associated loan applications.

## Consequences
- **Positive:** 100% of 19,417 customer records conform to ISO standard format with zero data loss.
