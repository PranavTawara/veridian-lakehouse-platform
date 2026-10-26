from fastapi import FastAPI, HTTPException, Query
from typing import Optional, List, Dict, Any
import psycopg2
from psycopg2.extras import RealDictCursor

app = FastAPI(
    title="Veridian Lakehouse Executive Analytics API",
    description="Low-latency REST endpoints querying conformed Gold data marts.",
    version="1.0.0"
)

DB_CONFIG = {
    "dbname": "veridian_db",
    "user": "postgres",
    "password": "postgrespassword",
    "host": "localhost",
    "port": 5432
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)

@app.get("/health", tags=["System"])
def health_check():
    return {"status": "HEALTHY", "engine": "PostgreSQL 16 (serving schema)"}

@app.get("/api/v1/branches/profitability", tags=["Finance"])
def get_branch_profitability(
    region: Optional[str] = Query(None, description="Filter by region (e.g., North, South, West)"),
    financials_missing: Optional[bool] = Query(None, description="Filter branches with missing financials")
) -> List[Dict[str, Any]]:
    """
    Returns branch net contributions, interest income, and operating costs.
    Preserves all 85 branches, including BR-047 with financials_missing=True.
    """
    query = """
        SELECT 
            b.branch_code,
            b.branch_name,
            b.region,
            b.city,
            pnl.reporting_month,
            pnl.interest_income,
            pnl.operating_cost,
            pnl.net_contribution,
            pnl.financials_missing
        FROM serving.dim_branch b
        JOIN serving.fact_branch_monthly_pnl pnl 
          ON b.branch_key = pnl.branch_key
        WHERE 1=1
    """
    params = []
    if region:
        query += " AND upper(b.region) = upper(%s)"
        params.append(region)
    if financials_missing is not None:
        query += " AND pnl.financials_missing = %s"
        params.append(financials_missing)

    query += " ORDER BY pnl.net_contribution DESC NULLS LAST;"

    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, tuple(params))
                return cur.fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/regions/risk-profile", tags=["Risk"])
def get_regional_risk_profile() -> List[Dict[str, Any]]:
    """
    Returns credit portfolio risk, regulatory NPA balances, NPA rates, and RAG status.
    """
    query = """
        SELECT 
            region,
            total_accounts,
            total_book_balance,
            npa_balance,
            npa_accounts,
            npa_rate_pct,
            rag_status
        FROM serving.fact_regional_risk_profile
        ORDER BY npa_rate_pct DESC;
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                return cur.fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/quarantine/metrics", tags=["Governance"])
def get_quarantine_metrics() -> Dict[str, Any]:
    """
    Returns audit counts of rejected/diverted records from the lakehouse quarantine layer.
    """
    query = """
        SELECT 
            source_table,
            rejection_code,
            count(*) AS rejected_count
        FROM silver.quarantine
        GROUP BY source_table, rejection_code
        ORDER BY rejected_count DESC;
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                records = cur.fetchall()
                total = sum(r["rejected_count"] for r in records)
                return {"total_quarantined": total, "breakdown": records}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))