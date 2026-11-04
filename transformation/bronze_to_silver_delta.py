import os
import argparse
from datetime import timedelta
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, trim, upper, regexp_replace, when, lit, 
    to_timestamp, to_date, row_number, current_timestamp, 
    expr, to_json, struct, abs as spark_abs, sha2, concat_ws
)
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

# DYNAMIC CONFIGURATION
LAKEHOUSE_ROOT = os.getenv("LAKEHOUSE_ROOT", os.path.abspath("./lakehouse"))
BRONZE_DIR = os.getenv("BRONZE_DIR", f"{LAKEHOUSE_ROOT}/bronze")
SILVER_DIR = os.getenv("SILVER_DIR", f"{LAKEHOUSE_ROOT}/silver")
QUARANTINE_PATH = os.path.join(SILVER_DIR, "quarantine")

def get_spark_session():
    builder = (
        SparkSession.builder.appName("AutonomousSilverConformation")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
    )
    extra_packages = []
    if SILVER_DIR.startswith("s3a://"):
        extra_packages.extend(["org.apache.hadoop:hadoop-aws:3.3.4", "com.amazonaws:aws-java-sdk-bundle:1.12.262"])
        builder = builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                         .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    return configure_spark_with_delta_pip(builder, extra_packages=extra_packages).config("spark.databricks.delta.schema.autoMerge.enabled", "true").getOrCreate()

def parse_epoch_or_iso_timestamp(col_name):
    return when(col(col_name).rlike(r"^\d{13}$"), (col(col_name).cast("double") / 1000.0).cast("timestamp")).otherwise(to_timestamp(col(col_name)))

def parse_multi_format_date(col_name):
    return when(col(col_name).rlike(r"^\d{1,2}/\d{1,2}/\d{4}$"), to_date(col(col_name), "d/M/yyyy")) \
          .when(col(col_name).rlike(r"^\d{4}-\d{2}-\d{2}$"), to_date(col(col_name), "yyyy-MM-dd")) \
          .when(col(col_name).rlike(r"^\d{13}$"), (col(col_name).cast("double") / 1000.0).cast("timestamp").cast("date")) \
          .otherwise(to_date(col(col_name)))

def clean_currency_to_double(col_name):
    return regexp_replace(col(col_name), r"[\$₹,\s]", "").cast("double")

def determine_entity_mode(spark, entity_name, requested_mode="auto"):
    if requested_mode in ["full", "incremental"]: return requested_mode
    target_path = os.path.join(SILVER_DIR, entity_name)
    if not os.path.exists(target_path) or not DeltaTable.isDeltaTable(spark, target_path): return "full"
    try:
        return "incremental" if spark.read.format("delta").load(target_path).count() > 0 else "full"
    except Exception: return "full"

def get_silver_watermark(spark, entity_name, watermark_col):
    target_path = os.path.join(SILVER_DIR, entity_name)
    if not DeltaTable.isDeltaTable(spark, target_path): return None
    try: return spark.read.format("delta").load(target_path).agg({watermark_col: "max"}).collect()[0][0]
    except Exception: return None

def build_quarantine_record(df_source, source_table, id_col, rejection_code):
    return df_source.select(
        sha2(concat_ws("||", lit(source_table), id_col, lit(rejection_code), to_json(struct("*"))), 256).alias("quarantine_id"),
        lit(source_table).alias("source_table"),
        id_col.alias("record_identifier"),
        lit(rejection_code).alias("rejection_code"),
        to_json(struct("*")).alias("raw_payload"),
        current_timestamp().alias("quarantined_at")
    )

def write_silver_entity(spark, df_incoming, entity_name, pk_cols, mode="auto"):
    target_path = os.path.join(SILVER_DIR, entity_name)
    
    # DYNAMIC HASHING: Inspects current DataFrame columns for robust schema evolution
    data_cols = [c for c in df_incoming.columns if c not in ["_loaded_at", "quarantined_at", "rk"] + pk_cols]
    df_payload = df_incoming.withColumn("row_hash", sha2(concat_ws("||", *[col(c).cast("string") for c in sorted(data_cols)]), 256) if data_cols else lit("constant_hash"))
    df_payload = df_payload.withColumn("_loaded_at", current_timestamp())

    if mode == "full" or not DeltaTable.isDeltaTable(spark, target_path):
        df_payload.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target_path)
        return

    # COST OPTIMIZATION: Left Anti-Join bypasses the Delta MERGE rewrite penalty
    df_existing = spark.read.format("delta").load(target_path)
    df_net_new_or_changed = df_payload.join(df_existing, on=pk_cols + ["row_hash"], how="left_anti")
    
    new_count = df_net_new_or_changed.count()
    if new_count > 0:
        print(f"   📥 [INCREMENTAL] {new_count:,} changed/new rows detected. Appending SCD Type 2 history...")
        df_net_new_or_changed.write.format("delta").mode("append").option("mergeSchema", "true").save(target_path)
    else:
        print(f"   ℹ️ [INCREMENTAL] 0 column-level changes detected. Skipping write.")

def run_silver_pipeline(mode="auto", watermark_col="_extracted_at", lookback_hours=0):
    print("=" * 85)
    print(f"STEP 3: AUTONOMOUS SILVER DELTA CONFORMATION ENGINE | REQUESTED: {mode.upper()}")
    print("=" * 85)
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    quarantine_accum = []

    def filter_bronze_dataset(df_raw, entity_name, effective_mode):
        if effective_mode == "full": return df_raw
        watermark = get_silver_watermark(spark, entity_name, "_loaded_at")
        if watermark and watermark_col in df_raw.columns:
            return df_raw.filter(col(watermark_col) >= (watermark - timedelta(hours=lookback_hours)))
        return df_raw

    # --- 1. Branches ---
    print("⚙️ Processing silver.branches...")
    br_mode = determine_entity_mode(spark, "branches", mode)
    df_bm_raw = filter_bronze_dataset(spark.read.format("delta").load(os.path.join(BRONZE_DIR, "branch_master")), "branches", br_mode)
    df_bm_staged = df_bm_raw.select(upper(trim(col("branch_code"))).alias("branch_code"), trim(col("branch_name")).alias("branch_name"), upper(trim(col("region"))).alias("region"), trim(col("state")).alias("state"), trim(col("city")).alias("city"), trim(col("branch_type")).alias("branch_type"), to_date(col("open_date")).alias("open_date"))
    df_bm_invalid_pk = df_bm_staged.filter(col("branch_code").isNull() | (trim(col("branch_code")) == ""))
    if df_bm_invalid_pk.count() > 0: quarantine_accum.append(build_quarantine_record(df_bm_invalid_pk, "branch_master", expr("coalesce(branch_name, 'UNKNOWN_BRANCH')"), "INVALID_PRIMARY_KEY"))
    df_bm_valid_pk = df_bm_staged.filter(col("branch_code").isNotNull() & (trim(col("branch_code")) != ""))
    ghost_branches = df_bm_valid_pk.filter(col("branch_code").isin("BR-999", "BR-0XX"))
    if ghost_branches.count() > 0: quarantine_accum.append(build_quarantine_record(ghost_branches, "branch_master", col("branch_code"), "GHOST_BRANCH"))
    df_bm_valid = df_bm_valid_pk.filter(~col("branch_code").isin("BR-999", "BR-0XX"))
    w_branch = Window.partitionBy("branch_code").orderBy(col("open_date").desc(), col("branch_name").asc())
    df_branches = df_bm_valid.withColumn("rk", row_number().over(w_branch)).filter(col("rk") == 1).drop("rk")
    write_silver_entity(spark, df_branches, "branches", ["branch_code"], mode=br_mode)
    valid_branch_codes = [r.branch_code for r in spark.read.format("delta").load(os.path.join(SILVER_DIR, "branches")).select("branch_code").distinct().collect()]

    # --- 2. Products ---
    print("⚙️ Processing silver.products...")
    prod_mode = determine_entity_mode(spark, "products", mode)
    df_pm_raw = filter_bronze_dataset(spark.read.format("delta").load(os.path.join(BRONZE_DIR, "product_master")), "products", prod_mode)
    df_pm_staged = df_pm_raw.select(when(trim(col("product_name")) == "Two-Wheeler Loan", lit("TL01")).otherwise(upper(trim(col("product_code")))).alias("product_code"), trim(col("product_name")).alias("product_name"), trim(col("category")).alias("category"), clean_currency_to_double("interest_rate").alias("interest_rate"))
    df_pm_invalid_pk = df_pm_staged.filter(col("product_code").isNull() | (trim(col("product_code")) == ""))
    if df_pm_invalid_pk.count() > 0: quarantine_accum.append(build_quarantine_record(df_pm_invalid_pk, "product_master", expr("coalesce(product_name, 'UNKNOWN_PRODUCT')"), "INVALID_PRIMARY_KEY"))
    df_pm_valid_pk = df_pm_staged.filter(col("product_code").isNotNull() & (trim(col("product_code")) != ""))
    w_prod = Window.partitionBy("product_code").orderBy(col("interest_rate").desc())
    df_pm_ranked = df_pm_valid_pk.withColumn("rk", row_number().over(w_prod))
    df_pm_conflicts = df_pm_ranked.filter(col("rk") > 1)
    if df_pm_conflicts.count() > 0: quarantine_accum.append(build_quarantine_record(df_pm_conflicts, "product_master", col("product_code"), "PRODUCT_RATE_CONFLICT"))
    df_products = df_pm_ranked.filter(col("rk") == 1).drop("rk")
    write_silver_entity(spark, df_products, "products", ["product_code"], mode=prod_mode)

    # --- 3. Customers ---
    print("⚙️ Processing silver.customers...")
    cust_mode = determine_entity_mode(spark, "customers", mode)
    df_cm_raw = filter_bronze_dataset(spark.read.format("delta").load(os.path.join(BRONZE_DIR, "customer_master")), "customers", cust_mode)
    df_cm_staged = df_cm_raw.select(upper(trim(col("customer_id"))).alias("customer_id"), regexp_replace(trim(col("customer_name")), r"\s+", " ").alias("customer_name"), trim(col("city")).alias("city"), trim(col("state")).alias("state"), upper(trim(col("kyc_status"))).alias("kyc_status"), trim(col("income_band")).alias("income_band"), parse_multi_format_date("onboarding_date").alias("onboarding_date"))
    df_cm_invalid_pk = df_cm_staged.filter(col("customer_id").isNull() | (trim(col("customer_id")) == ""))
    if df_cm_invalid_pk.count() > 0: quarantine_accum.append(build_quarantine_record(df_cm_invalid_pk, "customer_master", expr("coalesce(customer_name, 'UNKNOWN_CUSTOMER')"), "INVALID_PRIMARY_KEY"))
    df_cm_valid_pk = df_cm_staged.filter(col("customer_id").isNotNull() & (trim(col("customer_id")) != ""))
    w_cust = Window.partitionBy("customer_id").orderBy(col("onboarding_date").desc(), when(col("kyc_status") == "VERIFIED", 1).otherwise(0).desc())
    df_customers = df_cm_valid_pk.withColumn("rk", row_number().over(w_cust)).filter(col("rk") == 1).drop("rk")
    write_silver_entity(spark, df_customers, "customers", ["customer_id"], mode=cust_mode)

    # --- 4. Employees ---
    print("⚙️ Processing silver.employees...")
    emp_mode = determine_entity_mode(spark, "employees", mode)
    df_emp_raw = filter_bronze_dataset(spark.read.format("delta").load(os.path.join(BRONZE_DIR, "employee_roster")), "employees", emp_mode)
    df_emp_staged = df_emp_raw.select(upper(trim(col("employee_id"))).alias("employee_id"), regexp_replace(trim(col("employee_name")), r"\s+", " ").alias("employee_name"), upper(trim(col("branch_code"))).alias("branch_code"), trim(col("branch_name")).alias("branch_name"), upper(trim(col("role"))).alias("role"), to_date(col("join_date")).alias("join_date"), to_date(col("exit_date")).alias("exit_date"), upper(trim(col("status"))).alias("status"), clean_currency_to_double("annual_ctc").alias("annual_ctc"))
    df_emp_invalid_pk = df_emp_staged.filter(col("employee_id").isNull() | (trim(col("employee_id")) == ""))
    if df_emp_invalid_pk.count() > 0: quarantine_accum.append(build_quarantine_record(df_emp_invalid_pk, "employee_roster", expr("coalesce(employee_name, 'UNKNOWN_EMPLOYEE')"), "INVALID_PRIMARY_KEY"))
    df_emp_valid_pk = df_emp_staged.filter(col("employee_id").isNotNull() & (trim(col("employee_id")) != ""))
    df_emp_invalid_branch = df_emp_valid_pk.filter(~col("branch_code").isin(valid_branch_codes))
    if df_emp_invalid_branch.count() > 0: quarantine_accum.append(build_quarantine_record(df_emp_invalid_branch, "employee_roster", col("employee_id"), "INVALID_BRANCH_FK"))
    df_emp_valid = df_emp_valid_pk.filter(col("branch_code").isin(valid_branch_codes))
    w_emp = Window.partitionBy("employee_id").orderBy(col("join_date").desc(), when(col("status") == "ACTIVE", 1).otherwise(0).desc())
    df_employees = df_emp_valid.withColumn("rk", row_number().over(w_emp)).filter(col("rk") == 1).drop("rk")
    write_silver_entity(spark, df_employees, "employees", ["employee_id"], mode=emp_mode)

    # --- 5. Loans ---
    print("⚙️ Processing silver.loans...")
    loans_mode = determine_entity_mode(spark, "loans", mode)
    df_la_raw = filter_bronze_dataset(spark.read.format("delta").load(os.path.join(BRONZE_DIR, "loan_accounts")), "loans", loans_mode)
    df_ase_raw = spark.read.format("delta").load(os.path.join(BRONZE_DIR, "account_status_events"))
    df_ase_parsed = df_ase_raw.filter(col("application_id").isNotNull() & (trim(col("application_id")) != "")).select(upper(trim(col("application_id"))).alias("app_id"), parse_epoch_or_iso_timestamp("event_time").alias("event_timestamp"), upper(trim(col("new_status"))).alias("latest_status"))
    w_ase = Window.partitionBy("app_id").orderBy(col("event_timestamp").desc())
    df_active_npa_events = df_ase_parsed.withColumn("rk", row_number().over(w_ase)).filter(col("rk") == 1).filter(col("latest_status") == "NPA").select(col("app_id").alias("npa_app_id")).distinct()
    df_la_staged = df_la_raw.select(upper(trim(col("account_id"))).alias("account_id"), upper(trim(col("application_id"))).alias("application_id"), upper(trim(col("branch_code"))).alias("branch_code"), when(col("product_code").isNull() | (trim(col("product_code")) == ""), lit("TL01")).otherwise(upper(trim(col("product_code")))).alias("product_code"), clean_currency_to_double("principal_amount").alias("principal_amount"), clean_currency_to_double("outstanding_amount").alias("outstanding_amount"), when(col("dpd").isNull(), lit(0)).otherwise(col("dpd").cast("integer")).alias("dpd"), upper(trim(col("npa_flag"))).alias("source_npa_flag"), to_date(col("open_date")).alias("open_date"), parse_epoch_or_iso_timestamp("last_updated").alias("last_updated"))
    df_la_invalid_pk = df_la_staged.filter(col("account_id").isNull() | (trim(col("account_id")) == ""))
    if df_la_invalid_pk.count() > 0: quarantine_accum.append(build_quarantine_record(df_la_invalid_pk, "loan_accounts", expr("coalesce(application_id, 'UNKNOWN_APPLICATION')"), "INVALID_PRIMARY_KEY"))
    df_la_valid_pk = df_la_staged.filter(col("account_id").isNotNull() & (trim(col("account_id")) != ""))
    df_la_joined = df_la_valid_pk.join(df_active_npa_events, df_la_valid_pk.application_id == df_active_npa_events.npa_app_id, how="left")
    df_la_tagged = df_la_joined.withColumn("is_regulatory_npa", when((col("dpd") >= 90) | (col("source_npa_flag").isin("Y", "YES", "1")) | (col("npa_app_id").isNotNull()), lit(True)).otherwise(lit(False))).drop("npa_app_id")
    df_loans_valid = df_la_tagged.filter(col("branch_code").isin(valid_branch_codes))
    df_loans_invalid = df_la_tagged.filter(~col("branch_code").isin(valid_branch_codes))
    if df_loans_invalid.count() > 0: quarantine_accum.append(build_quarantine_record(df_loans_invalid, "loan_accounts", col("account_id"), "INVALID_BRANCH_FK"))
    write_silver_entity(spark, df_loans_valid, "loans", ["account_id"], mode=loans_mode)
    accumulated_loans = spark.read.format("delta").load(os.path.join(SILVER_DIR, "loans"))

    # --- 6. Transactions ---
    print("⚙️ Processing silver.transactions...")
    tx_mode = determine_entity_mode(spark, "transactions", mode)
    df_tx_raw = filter_bronze_dataset(spark.read.format("delta").load(os.path.join(BRONZE_DIR, "loan_transactions")), "transactions", tx_mode)
    df_tx_staged = df_tx_raw.select(upper(trim(col("txn_id"))).alias("txn_id"), upper(trim(col("account_id"))).alias("account_id"), upper(trim(col("application_id"))).alias("application_id"), upper(trim(col("branch_code"))).alias("branch_code"), upper(trim(col("txn_type"))).alias("txn_type"), clean_currency_to_double("amount").alias("raw_amount"), parse_epoch_or_iso_timestamp("txn_timestamp").alias("txn_timestamp"))
    df_tx_invalid_pk = df_tx_staged.filter(col("txn_id").isNull() | (trim(col("txn_id")) == ""))
    if df_tx_invalid_pk.count() > 0: quarantine_accum.append(build_quarantine_record(df_tx_invalid_pk, "loan_transactions", expr("coalesce(account_id, 'UNKNOWN_TXN')"), "INVALID_PRIMARY_KEY"))
    df_tx_valid_pk = df_tx_staged.filter(col("txn_id").isNotNull() & (trim(col("txn_id")) != ""))
    w_tx = Window.partitionBy("txn_id").orderBy(col("txn_timestamp").desc())
    df_tx_dedupe = df_tx_valid_pk.withColumn("rk", row_number().over(w_tx)).filter(col("rk") == 1).drop("rk")
    df_tx_dedupe = df_tx_dedupe.withColumn("is_reversal", when(col("raw_amount") < 0, lit(True)).otherwise(lit(False))).withColumn("clean_amount", spark_abs(col("raw_amount")))
    df_orphan_tx = df_tx_dedupe.join(accumulated_loans.select("account_id"), on="account_id", how="left_anti")
    if df_orphan_tx.count() > 0: quarantine_accum.append(build_quarantine_record(df_orphan_tx, "loan_transactions", col("txn_id"), "ORPHAN_ACCOUNT_ID"))
    df_tx_valid = df_tx_dedupe.join(accumulated_loans.select("account_id", "principal_amount"), on="account_id", how="inner")
    df_tx_adjusted = df_tx_valid.withColumn("clean_amount", when((col("txn_type") == "DISBURSEMENT") & (col("clean_amount").isNull() | (col("clean_amount") == 0)), col("principal_amount")).when((col("txn_type") == "DISBURSEMENT") & (col("clean_amount") > col("principal_amount") * 50), col("clean_amount") / 100.0).otherwise(col("clean_amount"))).drop("principal_amount")
    df_tx_invalid_amount = df_tx_adjusted.filter(col("clean_amount").isNull() | (col("clean_amount") <= 0))
    if df_tx_invalid_amount.count() > 0: quarantine_accum.append(build_quarantine_record(df_tx_invalid_amount, "loan_transactions", col("txn_id"), "INVALID_TXN_AMOUNT"))
    df_tx_conformed = df_tx_adjusted.filter(col("clean_amount").isNotNull() & (col("clean_amount") > 0))
    write_silver_entity(spark, df_tx_conformed, "transactions", ["txn_id"], mode=tx_mode)

    # --- 7. Quarantine Consolidation ---
    if quarantine_accum:
        print("🛡️ Consolidating Quarantine dataset...")
        df_final_quarantine = quarantine_accum[0]
        for df_q in quarantine_accum[1:]:
            df_final_quarantine = df_final_quarantine.unionByName(df_q)
        w_quar = Window.partitionBy("quarantine_id").orderBy(col("quarantined_at").desc())
        df_quar_deduped = df_final_quarantine.withColumn("rk", row_number().over(w_quar)).filter(col("rk") == 1).drop("rk")
        quar_mode = determine_entity_mode(spark, "quarantine", mode)
        write_silver_entity(spark, df_quar_deduped, "quarantine", ["quarantine_id"], mode=quar_mode)

    spark.stop()
    print("=" * 85)
    print("✅ Silver Lakehouse Conformation Complete.")
    print("=" * 85)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", default=os.getenv("LOAD_MODE", "auto"), choices=["auto", "full", "incremental"])
    parser.add_argument("--watermark-col", default=os.getenv("WATERMARK_COL", "_extracted_at"))
    parser.add_argument("--lookback-hours", type=int, default=int(os.getenv("WATERMARK_LOOKBACK_HOURS", 0)))
    args = parser.parse_args()
    run_silver_pipeline(mode=args.mode, watermark_col=args.watermark_col, lookback_hours=args.lookback_hours)