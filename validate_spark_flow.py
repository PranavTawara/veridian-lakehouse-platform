from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip
import os

# 1. Initialize Delta-configured Spark Session
builder = SparkSession.builder \
    .appName("VeridianSparkValidation") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.driver.memory", "2g")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

BRONZE_PATH = "lakehouse/bronze"
SILVER_PATH = "lakehouse/silver"

print("=" * 100)
print("1. READING DELTA TABLES VIA SPARK: ROW COUNT AUDIT")
print("=" * 100)

bronze_tables = [
    "branch_master", "product_master", "customer_master", 
    "employee_roster", "loan_accounts", "loan_transactions", 
    "account_status_events", "branch_financials", "loan_applications"
]

silver_tables = ["branches", "products", "customers", "employees", "loans", "transactions", "quarantine"]

print(f"{'BRONZE DELTA TABLE':<28} | {'ROW COUNT':<10} || {'SILVER DELTA TABLE':<24} | {'ROW COUNT':<10}")
print("-" * 85)

for b_tbl, s_tbl in zip(bronze_tables[:len(silver_tables)], silver_tables):
    b_df = spark.read.format("delta").load(f"{BRONZE_PATH}/{b_tbl}")
    s_df = spark.read.format("delta").load(f"{SILVER_PATH}/{s_tbl}")
    print(f"{b_tbl:<28} | {b_df.count():>9,} || {s_tbl:<24} | {s_df.count():>9,}")

print("\n" + "=" * 100)
print("2. DATA FLOW VALIDATION: BRONZE (RAW) -> SPARK CONFORMATION -> SILVER (TYPED)")
print("=" * 100)

# Check 1: Products Imputation & Rate Deduplication
print("\n[A] Product Catalog Transformation (product_master -> products):")
df_raw_prod = spark.read.format("delta").load(f"{BRONZE_PATH}/product_master")
print("-> Bronze Raw Schema (Stored as text strings):")
df_raw_prod.select("product_code", "product_name", "interest_rate").show(truncate=False)

df_clean_prod = spark.read.format("delta").load(f"{SILVER_PATH}/products")
print("-> Silver Conformed Schema (Imputed TL01, cleaned interest_rate cast to numeric):")
df_clean_prod.show(truncate=False)

# Check 2: Customer Onboarding Date Standardization
print("\n[B] Customer Date Normalization (customer_master -> customers):")
df_raw_cust = spark.read.format("delta").load(f"{BRONZE_PATH}/customer_master")
print("-> Bronze Raw Dates with slash separators ('dd/MM/yyyy'):")
df_raw_cust.filter(F.col("onboarding_date").like("%/%")) \
    .select("customer_id", "customer_name", "onboarding_date") \
    .show(5, truncate=False)

df_clean_cust = spark.read.format("delta").load(f"{SILVER_PATH}/customers")
print("-> Silver Standardized ISO Dates ('yyyy-MM-dd'):")
df_clean_cust.select("customer_id", "customer_name", "onboarding_date") \
    .filter(F.col("onboarding_date").isNotNull()) \
    .show(5, truncate=False)

# Check 3: Loan Accounting & DPD Flagging
print("\n[C] Loans Conformation & Risk Status (loan_accounts -> loans):")
df_raw_loans = spark.read.format("delta").load(f"{BRONZE_PATH}/loan_accounts")
print("-> Bronze Raw Amounts (Strings with possible quotes or formatting):")
df_raw_loans.select("account_id", "branch_code", "product_code", "principal_amount", "outstanding_amount", "dpd") \
    .show(5, truncate=False)

df_clean_loans = spark.read.format("delta").load(f"{SILVER_PATH}/loans")
print("-> Silver Cleaned Loans (Typed numeric values, regulatory NPA derived):")
df_clean_loans.select("account_id", "branch_code", "product_code", "principal_amount", "outstanding_amount", "dpd", "is_regulatory_npa") \
    .show(5, truncate=False)

print("\n" + "=" * 100)
print("3. SPARK AUDIT ON QUARANTINE LEDGER (silver.quarantine)")
print("=" * 100)

df_quar = spark.read.format("delta").load(f"{SILVER_PATH}/quarantine")
print("-> Rejection Reason Distribution in Quarantine:")
df_quar.groupBy("rejection_code", "source_table").count().orderBy(F.desc("count")).show(truncate=False)

print("-> Quarantined Row Example Payload:")
df_quar.select("record_identifier", "rejection_code", "raw_payload").show(2, truncate=100)

spark.stop()
