import psycopg2
from tabulate import tabulate

DB_CONFIG = {
    "dbname": "veridian_db",
    "user": "postgres",
    "password": "postgrespassword",
    "host": "localhost",
    "port": 5432
}

QUERIES = [
    (
        "1. Branch BR-047 Intentional Business Nulls & Missing Financials",
        """
        SELECT *
        FROM serving.fact_branch_monthly_pnl
        WHERE financials_missing = TRUE;
        """
    ),
    (
        "2. Regional Risk, NPA Rates & RAG Status Distribution",
        """
        SELECT *
        FROM serving.fact_regional_risk_profile
        ORDER BY region;
        """
    ),
    (
        "3. Top 10 Branches by Active Staffing",
        """
        SELECT *
        FROM serving.fact_branch_staffing
        ORDER BY total_active_staff DESC
        LIMIT 10;
        """
    ),
    (
        "4. Product Mix & Ticket Size Conformance (Two-Wheeler TL01 Verified)",
        """
        SELECT 
            p.product_code,
            p.product_name,
            p.category,
            p.interest_rate,
            COUNT(l.account_id) AS total_loans,
            ROUND(SUM(l.principal_amount)::numeric, 2) AS total_disbursed_principal,
            ROUND(AVG(l.principal_amount)::numeric, 2) AS avg_ticket_size
        FROM serving.dim_product p
        LEFT JOIN silver.loans l ON p.product_code = l.product_code
        GROUP BY p.product_code, p.product_name, p.category, p.interest_rate
        ORDER BY total_loans DESC;
        """
    ),
    (
        "5. Micro-Ledger Check: Loans With Disbursement Discrepancies (> ₹0.01)",
        """
        SELECT 
            l.account_id,
            l.product_code,
            l.principal_amount,
            t.txn_id,
            t.clean_amount AS disbursement_amount,
            ABS(l.principal_amount - t.clean_amount) AS difference
        FROM silver.loans l
        JOIN silver.transactions t ON l.account_id = t.account_id
        WHERE t.txn_type = 'DISBURSEMENT'
          AND ABS(l.principal_amount - t.clean_amount) > 0.01
        ORDER BY difference DESC
        LIMIT 10;
        """
    ),
    (
        "6. Customer Demographics: Income Band & KYC Conformance",
        """
        SELECT 
            income_band,
            kyc_status,
            COUNT(customer_key) AS customer_count,
            ROUND(COUNT(customer_key) * 100.0 / SUM(COUNT(customer_key)) OVER(), 2) AS share_pct
        FROM serving.dim_customer
        GROUP BY income_band, kyc_status
        ORDER BY income_band, kyc_status;
        """
    )
]

def run_audits():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    for title, sql in QUERIES:
        print("=" * 85)
        print(f"📊 {title}")
        print("=" * 85)
        cur.execute(sql)
        headers = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        if rows:
            print(tabulate(rows, headers=headers, tablefmt="psql"))
        else:
            print("✅ 0 rows returned (Assertion Passed - Zero discrepancies found).")
        print()

    cur.close()
    conn.close()

if __name__ == "__main__":
    run_audits()