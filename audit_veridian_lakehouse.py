import psycopg2
import sys

conn = psycopg2.connect("postgresql://postgres:postgrespassword@localhost:5432/veridian_db")
cur = conn.cursor()

passed_checks = 0
failed_checks = 0

def test(check_name, condition, details=""):
    global passed_checks, failed_checks
    if condition:
        print(f"  [PASS] {check_name}")
        passed_checks += 1
    else:
        print(f"  [FAIL] {check_name} -> {details}")
        failed_checks += 1

print("\n" + "=" * 80)
print("VERIDIAN DATA PLATFORM: COMPREHENSIVE DATA INTEGRITY AUDIT")
print("=" * 80)

# -----------------------------------------------------------------------------
# 1. BRANCHES AUDIT
# -----------------------------------------------------------------------------
print("\n🏢 1. BRANCHES VALIDATION (silver.branches & serving.dim_branch)")

# Check 1: Count == 85
cur.execute("SELECT COUNT(*), COUNT(DISTINCT branch_code) FROM silver.branches;")
sb_cnt, sb_dist = cur.fetchone()
test("Branches Row Count & PK Uniqueness", sb_cnt == 85 and sb_dist == 85, f"Expected 85 unique, got {sb_cnt} (distinct: {sb_dist})")

# Check 2: No Ghost Branches
cur.execute("SELECT COUNT(*) FROM silver.branches WHERE branch_code IN ('BR-999', 'BR-0XX');")
ghost_cnt = cur.fetchone()[0]
test("Ghost Branches Excluded", ghost_cnt == 0, f"Found {ghost_cnt} ghost branches in silver")

# Check 3: Region completeness
cur.execute("SELECT COUNT(*) FROM silver.branches WHERE region IS NULL OR region = '' OR region NOT IN ('NORTH', 'SOUTH', 'EAST', 'WEST');")
invalid_regions = cur.fetchone()[0]
test("Region Domain Conformance (NORTH/SOUTH/EAST/WEST)", invalid_regions == 0, f"Found {invalid_regions} rows with bad region values")

# -----------------------------------------------------------------------------
# 2. PRODUCTS AUDIT
# -----------------------------------------------------------------------------
print("\n📦 2. PRODUCTS VALIDATION (silver.products & serving.dim_product)")

# Check 4: Product count and TL01 conformance
cur.execute("SELECT COUNT(*), COUNT(DISTINCT product_code) FROM silver.products;")
sp_cnt, sp_dist = cur.fetchone()
test("Products Row Count & PK Uniqueness", sp_cnt == 5 and sp_dist == 5, f"Expected 5 products, got {sp_cnt}")

cur.execute("SELECT COUNT(*) FROM silver.products WHERE product_code = 'TL01';")
tl01_exists = cur.fetchone()[0]
test("Two-Wheeler Loan Conformed to 'TL01'", tl01_exists == 1, "TL01 mapping missing")

cur.execute("SELECT COUNT(*) FROM silver.products WHERE interest_rate <= 0 OR interest_rate IS NULL;")
invalid_rates = cur.fetchone()[0]
test("Interest Rates Positive & Non-Null", invalid_rates == 0, f"Found {invalid_rates} invalid interest rates")

# -----------------------------------------------------------------------------
# 3. CUSTOMERS AUDIT
# -----------------------------------------------------------------------------
print("\n👤 3. CUSTOMERS VALIDATION (silver.customers & serving.dim_customer)")

cur.execute("SELECT COUNT(*), COUNT(DISTINCT customer_id) FROM silver.customers;")
sc_cnt, sc_dist = cur.fetchone()
test("Customers Row Count & PK Uniqueness", sc_cnt == 19417 and sc_dist == 19417, f"Expected 19,417 unique, got {sc_cnt} (distinct: {sc_dist})")

cur.execute("SELECT COUNT(*) FROM silver.customers WHERE customer_name IS NULL OR customer_name = '' OR onboarding_date IS NULL;")
bad_cust = cur.fetchone()[0]
test("Customer Name & Onboarding Date Non-Null", bad_cust == 0, f"Found {bad_cust} incomplete customer profiles")

# -----------------------------------------------------------------------------
# 4. EMPLOYEES AUDIT
# -----------------------------------------------------------------------------
print("\n👔 4. EMPLOYEES VALIDATION (silver.employees & serving.dim_employee)")

cur.execute("SELECT COUNT(*), COUNT(DISTINCT employee_id) FROM silver.employees;")
se_cnt, se_dist = cur.fetchone()
test("Employees Row Count & PK Uniqueness", se_cnt == 46965 and se_dist == 46965, f"Expected 46,965, got {se_cnt}")

# Check 8: Branch FK integrity for employees
cur.execute("""
    SELECT COUNT(*) 
    FROM silver.employees e 
    LEFT JOIN silver.branches b ON e.branch_code = b.branch_code 
    WHERE b.branch_code IS NULL;
""")
orphan_emp = cur.fetchone()[0]
test("Employee Branch FK Referential Integrity", orphan_emp == 0, f"Found {orphan_emp} employees assigned to non-existent branches")

cur.execute("SELECT COUNT(*) FROM silver.employees WHERE annual_ctc <= 0 OR annual_ctc IS NULL;")
invalid_ctc = cur.fetchone()[0]
test("Annual CTC Values Clean & Positive", invalid_ctc == 0, f"Found {invalid_ctc} invalid salaries")

# -----------------------------------------------------------------------------
# 5. LOANS AUDIT
# -----------------------------------------------------------------------------
print("\n💳 5. LOANS VALIDATION (silver.loans & serving fact marts)")

cur.execute("SELECT COUNT(*), COUNT(DISTINCT account_id) FROM silver.loans;")
sl_cnt, sl_dist = cur.fetchone()
test("Loans Row Count & PK Uniqueness", sl_cnt == 14065 and sl_dist == 14065, f"Expected 14,065 unique, got {sl_cnt}")

# Check 11: Loans Branch FK Integrity
cur.execute("""
    SELECT COUNT(*) 
    FROM silver.loans l 
    LEFT JOIN silver.branches b ON l.branch_code = b.branch_code 
    WHERE b.branch_code IS NULL;
""")
orphan_loans = cur.fetchone()[0]
test("Loans Branch FK Referential Integrity", orphan_loans == 0, f"Found {orphan_loans} loans assigned to invalid branches")

# Check 12: Loans Product FK Integrity
cur.execute("""
    SELECT COUNT(*) 
    FROM silver.loans l 
    LEFT JOIN silver.products p ON l.product_code = p.product_code 
    WHERE p.product_code IS NULL;
""")
orphan_prods = cur.fetchone()[0]
test("Loans Product FK Referential Integrity", orphan_prods == 0, f"Found {orphan_prods} loans with invalid product codes")

# Check 13: Financial Sanity & DPD Range
cur.execute("""
    SELECT 
        COUNT(CASE WHEN principal_amount <= 0 OR principal_amount IS NULL THEN 1 END),
        COUNT(CASE WHEN outstanding_amount < 0 OR outstanding_amount > principal_amount * 1.5 THEN 1 END),
        COUNT(CASE WHEN dpd < 0 THEN 1 END),
        COUNT(CASE WHEN is_regulatory_npa = TRUE THEN 1 END)
    FROM silver.loans;
""")
bad_prin, bad_os, bad_dpd, npa_cnt = cur.fetchone()
test("Principal Amounts Clean & Positive", bad_prin == 0, f"Found {bad_prin} bad principal amounts")
test("Outstanding Balance Boundaries (>=0 and <= 1.5x principal)", bad_os == 0, f"Found {bad_os} out-of-bound balances")
test("DPD Non-Negative", bad_dpd == 0, f"Found {bad_dpd} negative DPD values")
test("Baseline Delinquency Conformance (0 True NPAs)", npa_cnt == 0, f"Found {npa_cnt} loans tagged as NPA")

# -----------------------------------------------------------------------------
# 6. TRANSACTIONS AUDIT
# -----------------------------------------------------------------------------
print("\n💵 6. TRANSACTIONS VALIDATION (silver.transactions)")

cur.execute("SELECT COUNT(*), COUNT(DISTINCT txn_id) FROM silver.transactions;")
st_cnt, st_dist = cur.fetchone()
test("Transactions Row Count & PK Uniqueness", st_cnt == 41656 and st_dist == 41656, f"Expected 41,656 unique, got {st_cnt}")

# Check 17: Account FK Integrity
cur.execute("""
    SELECT COUNT(*) 
    FROM silver.transactions t 
    LEFT JOIN silver.loans l ON t.account_id = l.account_id 
    WHERE l.account_id IS NULL;
""")
orphan_txns = cur.fetchone()[0]
test("Transaction Loan Account FK Integrity", orphan_txns == 0, f"Found {orphan_txns} orphan transactions")

cur.execute("SELECT COUNT(*) FROM silver.transactions WHERE clean_amount <= 0 OR clean_amount IS NULL;")
invalid_tx_amounts = cur.fetchone()[0]
test("Transaction Clean Amounts Valid & Positive", invalid_tx_amounts == 0, f"Found {invalid_tx_amounts} invalid transaction amounts")

# -----------------------------------------------------------------------------
# 7. QUARANTINE & FINANCIAL RECONCILIATION
# -----------------------------------------------------------------------------
print("\n🛡️ 7. QUARANTINE & CROSS-LAYER RECONCILIATION")

cur.execute("SELECT COUNT(*) FROM silver.quarantine;")
quar_cnt = cur.fetchone()[0]
test("Quarantine Record Count", quar_cnt == 5226, f"Expected 5,226 quarantined records, got {quar_cnt}")

# Reconciliation: Silver loan outstanding balance vs Serving Regional risk profile balance
cur.execute("SELECT ROUND(SUM(outstanding_amount)::numeric, 2) FROM silver.loans;")
silver_book = cur.fetchone()[0]

cur.execute("SELECT ROUND(SUM(total_book_balance)::numeric, 2) FROM serving.fact_regional_risk_profile;")
gold_book = cur.fetchone()[0]
test("Financial Ledger Reconciliation (Silver Loans == Gold Risk Fact)", silver_book == gold_book, f"Silver ₹{silver_book} != Gold ₹{gold_book}")

conn.close()

print("\n" + "=" * 80)
print(f"AUDIT SUMMARY: {passed_checks} PASSED | {failed_checks} FAILED")
print("=" * 80 + "\n")

if failed_checks > 0:
    sys.exit(1)
