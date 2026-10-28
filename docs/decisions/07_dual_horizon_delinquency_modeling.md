# ADR 007: Dual-Horizon Delinquency Modeling (Regulatory NPA vs. SMA-0 Watchlist)

## Status
Accepted

## Context
In `initial_load`, `source.account_status_events` contained 8,776 `NPA_TAGGED` rows covering ~32.5% of the portfolio. Conversely, `loan_accounts` showed 100% of loans with `source_npa_flag = 'N'` and maximum portfolio DPD bounded at 30 days. Timestamp analysis proved `NPA_TAGGED` events occurred *prior to loan disbursement* during pre-approval underwriting. Blindly joining this table fabricated a uniform 32.5% failure rate across all regions.

## Decision
Implement a **Dual-Horizon Asset Quality Model** in `serving.fact_regional_risk_profile`:
1. **Regulatory NPA (DPD >= 90 / CBS Default):** Reported strictly at **0.00%** to comply with RBI/NBFC regulatory definitions for this baseline load.
2. **Early Delinquency Watchlist (SMA-0: 1–30 DPD):** Exposed at **15.1%** (2,106 accounts | ₹69.71 Cr) to provide executive visibility into active credit risk.
3. **Dynamic RAG Status:** Calibrated against SMA-0 concentration to give actionable geographic prioritization:
   - **North (15.96% SMA-0):** RED (Tighten underwriting)
   - **South (15.21% SMA-0):** AMBER (Selective expansion)
   - **East (14.71% SMA-0):** AMBER (Moderate expansion)
   - **West (14.50% SMA-0):** GREEN (Prime expansion priority)

## Alternatives Considered
- **Join Event Stream Directly:** Produces false-positive 32.5% defaults across every region, contradicting core banking ledger balances.
- **Report 0.00% NPA in Isolation:** Leaves leadership with a blank risk profile with zero regional differentiation.

## Consequences
- **Positive:** Defensible, mathematically reconciled, and answers leadership's exact question regarding geographic risk concentration.
