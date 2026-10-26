import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, trim, upper, regexp_replace, when, lit, 
    to_timestamp, to_date, row_number, current_timestamp, 
    expr, to_json, struct, abs as spark_abs
)
from delta import configure_spark_with_delta_pip

BRONZE_DIR = os.path.abspath("./lakehouse/bronze")
SILVER_DIR = os.path.abspath("./lakehouse/silver")
QUARANTINE_PATH = os.path.join(SILVER_DIR, "quarantine")

def get_spark_session():
    builder = (
        SparkSession.builder.appName("SilverConformationAndQuarantine")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .config("spark.driver.memory", "2g")
    )
    return configure_spark_with_delta_pip(builder).config("spark.databricks.delta.schema.autoMerge.enabled", "true").getOrCreate()

def parse_epoch_or_iso_timestamp(col_name):
    return when(
        col(col_name).rlike(r"^\d{13}$"),
        (col(col_name).cast("double") / 1000.0).cast("timestamp")
    ).otherwise(to_timestamp(col(col_name)))

def clean_currency_to_double(col_name):
    return regexp_replace(col(col_name), r"[\$₹,\s]", "").cast("double")

def run_silver_pipeline():
    print("=" * 80)
    print("STEP 3: CONFORMING BRONZE -> SILVER DELTA TABLES & QUARANTINE")
    print("=" * 80)

    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    quarantine_accum = []

    # -------------------------------------------------------------------------
    # 1. silver.branches (PK: branch_code)
    # -------------------------------------------------------------------------
    print("⚙️ Processing silver.branches...")
    df_bm_raw = spark.read.format("delta").load(os.path.join(BRONZE_DIR, "branch_master"))
    
    df_bm_staged = df_bm_raw.select(
        upper(trim(col("branch_code"))).alias("branch_code"),
        trim(col("branch_name")).alias("branch_name"),
        upper(trim(col("region"))).alias("region"),
        trim(col("state")).alias("state"),
        trim(col("city")).alias("city"),
        trim(col("branch_type")).alias("branch_type"),
        to_date(col("open_date")).alias("open_date")
    )

    # Gate: Reject NULL or Empty Primary Keys
    df_bm_invalid_pk = df_bm_staged.filter(col("branch_code").isNull() | (trim(col("branch_code")) == ""))
    if df_bm_invalid_pk.count() > 0:
        quarantine_accum.append(df_bm_invalid_pk.select(
            expr("uuid()").alias("quarantine_id"),
            lit("branch_master").alias("source_table"),
            expr("coalesce(branch_name, 'UNKNOWN_BRANCH')").alias("record_identifier"),
            lit("INVALID_PRIMARY_KEY").alias("rejection_code"),
            to_json(struct("*")).alias("raw_payload"),
            current_timestamp().alias("quarantined_at")
        ))

    df_bm_valid_pk = df_bm_staged.filter(col("branch_code").isNotNull() & (trim(col("branch_code")) != ""))

    ghost_branches = df_bm_valid_pk.filter(col("branch_code").isin("BR-999", "BR-0XX"))
    if ghost_branches.count() > 0:
        quarantine_accum.append(ghost_branches.select(
            expr("uuid()").alias("quarantine_id"),
            lit("branch_master").alias("source_table"),
            col("branch_code").alias("record_identifier"),
            lit("GHOST_BRANCH").alias("rejection_code"),
            to_json(struct("*")).alias("raw_payload"),
            current_timestamp().alias("quarantined_at")
        ))

    df_bm_valid = df_bm_valid_pk.filter(~col("branch_code").isin("BR-999", "BR-0XX"))

    w_branch = Window.partitionBy("branch_code").orderBy(col("open_date").desc(), col("branch_name").asc())
    df_branches = df_bm_valid.withColumn("rk", row_number().over(w_branch)).filter(col("rk") == 1).drop("rk")
    df_branches.withColumn("_loaded_at", current_timestamp()).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(os.path.join(SILVER_DIR, "branches"))
    valid_branch_codes = [r.branch_code for r in df_branches.select("branch_code").collect()]
    print(f"   ✅ Saved {df_branches.count()} conformed unique branches.")

    # -------------------------------------------------------------------------
    # 2. silver.products (PK: product_code)
    # -------------------------------------------------------------------------
    print("⚙️ Processing silver.products...")
    df_pm_raw = spark.read.format("delta").load(os.path.join(BRONZE_DIR, "product_master"))

    # Upstream source fix: Map Two-Wheeler Loan to TL01 if missing, uppercase others
    df_pm_staged = df_pm_raw.select(
        when(trim(col("product_name")) == "Two-Wheeler Loan", lit("TL01"))
        .otherwise(upper(trim(col("product_code")))).alias("product_code"),
        trim(col("product_name")).alias("product_name"),
        trim(col("category")).alias("category"),
        clean_currency_to_double("interest_rate").alias("interest_rate")
    )

    # Gate: Reject NULL or Empty Primary Keys
    df_pm_invalid_pk = df_pm_staged.filter(col("product_code").isNull() | (trim(col("product_code")) == ""))
    if df_pm_invalid_pk.count() > 0:
        quarantine_accum.append(df_pm_invalid_pk.select(
            expr("uuid()").alias("quarantine_id"),
            lit("product_master").alias("source_table"),
            expr("coalesce(product_name, 'UNKNOWN_PRODUCT')").alias("record_identifier"),
            lit("INVALID_PRIMARY_KEY").alias("rejection_code"),
            to_json(struct("*")).alias("raw_payload"),
            current_timestamp().alias("quarantined_at")
        ))

    df_pm_valid_pk = df_pm_staged.filter(col("product_code").isNotNull() & (trim(col("product_code")) != ""))

    w_prod = Window.partitionBy("product_code").orderBy(col("interest_rate").desc())
    df_pm_ranked = df_pm_valid_pk.withColumn("rk", row_number().over(w_prod))

    df_pm_conflicts = df_pm_ranked.filter(col("rk") > 1)
    if df_pm_conflicts.count() > 0:
        quarantine_accum.append(df_pm_conflicts.select(
            expr("uuid()").alias("quarantine_id"),
            lit("product_master").alias("source_table"),
            col("product_code").alias("record_identifier"),
            lit("PRODUCT_RATE_CONFLICT").alias("rejection_code"),
            to_json(struct("*")).alias("raw_payload"),
            current_timestamp().alias("quarantined_at")
        ))

    df_products = df_pm_ranked.filter(col("rk") == 1).drop("rk")
    df_products.withColumn("_loaded_at", current_timestamp()).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(os.path.join(SILVER_DIR, "products"))
    print(f"   ✅ Saved {df_products.count()} conformed products.")

    # -------------------------------------------------------------------------
    # 3. silver.customers (PK: customer_id)
    # -------------------------------------------------------------------------
    print("⚙️ Processing silver.customers...")
    df_cm_raw = spark.read.format("delta").load(os.path.join(BRONZE_DIR, "customer_master"))

    df_cm_staged = df_cm_raw.select(
        upper(trim(col("customer_id"))).alias("customer_id"),
        regexp_replace(trim(col("customer_name")), r"\s+", " ").alias("customer_name"),
        trim(col("city")).alias("city"),
        trim(col("state")).alias("state"),
        upper(trim(col("kyc_status"))).alias("kyc_status"),
        trim(col("income_band")).alias("income_band"),
        to_date(col("onboarding_date")).alias("onboarding_date")
    )

    # Gate: Reject NULL or Empty Primary Keys
    df_cm_invalid_pk = df_cm_staged.filter(col("customer_id").isNull() | (trim(col("customer_id")) == ""))
    if df_cm_invalid_pk.count() > 0:
        quarantine_accum.append(df_cm_invalid_pk.select(
            expr("uuid()").alias("quarantine_id"),
            lit("customer_master").alias("source_table"),
            expr("coalesce(customer_name, 'UNKNOWN_CUSTOMER')").alias("record_identifier"),
            lit("INVALID_PRIMARY_KEY").alias("rejection_code"),
            to_json(struct("*")).alias("raw_payload"),
            current_timestamp().alias("quarantined_at")
        ))

    df_cm_valid_pk = df_cm_staged.filter(col("customer_id").isNotNull() & (trim(col("customer_id")) != ""))

    w_cust = Window.partitionBy("customer_id").orderBy(
        col("onboarding_date").desc(),
        when(col("kyc_status") == "VERIFIED", 1).otherwise(0).desc()
    )
    df_customers = df_cm_valid_pk.withColumn("rk", row_number().over(w_cust)).filter(col("rk") == 1).drop("rk")
    df_customers.withColumn("_loaded_at", current_timestamp()).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(os.path.join(SILVER_DIR, "customers"))
    print(f"   ✅ Saved {df_customers.count():,} conformed customers.")

    # -------------------------------------------------------------------------
    # 4. silver.employees (PK: employee_id)
    # -------------------------------------------------------------------------
    print("⚙️ Processing silver.employees...")
    df_emp_raw = spark.read.format("delta").load(os.path.join(BRONZE_DIR, "employee_roster"))

    df_emp_staged = df_emp_raw.select(
        upper(trim(col("employee_id"))).alias("employee_id"),
        regexp_replace(trim(col("employee_name")), r"\s+", " ").alias("employee_name"),
        upper(trim(col("branch_code"))).alias("branch_code"),
        trim(col("branch_name")).alias("branch_name"),
        upper(trim(col("role"))).alias("role"),
        to_date(col("join_date")).alias("join_date"),
        to_date(col("exit_date")).alias("exit_date"),
        upper(trim(col("status"))).alias("status"),
        clean_currency_to_double("annual_ctc").alias("annual_ctc")
    )

    # Gate: Reject NULL or Empty Primary Keys
    df_emp_invalid_pk = df_emp_staged.filter(col("employee_id").isNull() | (trim(col("employee_id")) == ""))
    if df_emp_invalid_pk.count() > 0:
        quarantine_accum.append(df_emp_invalid_pk.select(
            expr("uuid()").alias("quarantine_id"),
            lit("employee_roster").alias("source_table"),
            expr("coalesce(employee_name, 'UNKNOWN_EMPLOYEE')").alias("record_identifier"),
            lit("INVALID_PRIMARY_KEY").alias("rejection_code"),
            to_json(struct("*")).alias("raw_payload"),
            current_timestamp().alias("quarantined_at")
        ))

    df_emp_valid_pk = df_emp_staged.filter(col("employee_id").isNotNull() & (trim(col("employee_id")) != ""))

    # Route employees with invalid/empty branch FKs to quarantine
    df_emp_invalid_branch = df_emp_valid_pk.filter(~col("branch_code").isin(valid_branch_codes))
    if df_emp_invalid_branch.count() > 0:
        quarantine_accum.append(df_emp_invalid_branch.select(
            expr("uuid()").alias("quarantine_id"),
            lit("employee_roster").alias("source_table"),
            col("employee_id").alias("record_identifier"),
            lit("INVALID_BRANCH_FK").alias("rejection_code"),
            to_json(struct("*")).alias("raw_payload"),
            current_timestamp().alias("quarantined_at")
        ))

    df_emp_valid = df_emp_valid_pk.filter(col("branch_code").isin(valid_branch_codes))

    w_emp = Window.partitionBy("employee_id").orderBy(
        col("join_date").desc(),
        when(col("status") == "ACTIVE", 1).otherwise(0).desc()
    )
    df_employees = df_emp_valid.withColumn("rk", row_number().over(w_emp)).filter(col("rk") == 1).drop("rk")
    df_employees.withColumn("_loaded_at", current_timestamp()).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(os.path.join(SILVER_DIR, "employees"))
    print(f"   ✅ Saved {df_employees.count():,} conformed employees.")

    # -------------------------------------------------------------------------
    # 5. silver.loans (PK: account_id)
    # -------------------------------------------------------------------------
    print("⚙️ Processing silver.loans & checking NPA lifecycle events...")
    df_la_raw = spark.read.format("delta").load(os.path.join(BRONZE_DIR, "loan_accounts"))
    df_ase_raw = spark.read.format("delta").load(os.path.join(BRONZE_DIR, "account_status_events"))

    df_npa_apps = df_ase_raw.filter(upper(trim(col("event_type"))) == "NPA_TAGGED") \
                            .select(upper(trim(col("application_id"))).alias("npa_app_id")) \
                            .distinct()

    df_la_staged = df_la_raw.select(
        upper(trim(col("account_id"))).alias("account_id"),
        upper(trim(col("application_id"))).alias("application_id"),
        upper(trim(col("branch_code"))).alias("branch_code"),
        # Conforming empty product_code to 'TL01' to prevent foreign key breakages
        when(
            col("product_code").isNull() | (trim(col("product_code")) == ""),
            lit("TL01")
        ).otherwise(upper(trim(col("product_code")))).alias("product_code"),
        clean_currency_to_double("principal_amount").alias("principal_amount"),
        clean_currency_to_double("outstanding_amount").alias("outstanding_amount"),
        when(col("dpd").isNull(), lit(0)).otherwise(col("dpd").cast("integer")).alias("dpd"),
        upper(trim(col("npa_flag"))).alias("source_npa_flag"),
        to_date(col("open_date")).alias("open_date"),
        parse_epoch_or_iso_timestamp("last_updated").alias("last_updated")
    )

    # Gate: Reject NULL or Empty Primary Keys
    df_la_invalid_pk = df_la_staged.filter(col("account_id").isNull() | (trim(col("account_id")) == ""))
    if df_la_invalid_pk.count() > 0:
        quarantine_accum.append(df_la_invalid_pk.select(
            expr("uuid()").alias("quarantine_id"),
            lit("loan_accounts").alias("source_table"),
            expr("coalesce(application_id, 'UNKNOWN_APPLICATION')").alias("record_identifier"),
            lit("INVALID_PRIMARY_KEY").alias("rejection_code"),
            to_json(struct("*")).alias("raw_payload"),
            current_timestamp().alias("quarantined_at")
        ))

    df_la_valid_pk = df_la_staged.filter(col("account_id").isNotNull() & (trim(col("account_id")) != ""))

    df_la_joined = df_la_valid_pk.join(df_npa_apps, df_la_valid_pk.application_id == df_npa_apps.npa_app_id, how="left")

    df_la_tagged = df_la_joined.withColumn(
        "is_regulatory_npa",
        when(
            (col("dpd") >= 90) | 
            (col("source_npa_flag").isin("Y", "YES", "1")) | 
            (col("npa_app_id").isNotNull()),
            lit(True)
        ).otherwise(lit(False))
    ).drop("npa_app_id")

    df_loans_valid = df_la_tagged.filter(col("branch_code").isin(valid_branch_codes))
    df_loans_invalid = df_la_tagged.filter(~col("branch_code").isin(valid_branch_codes))

    if df_loans_invalid.count() > 0:
        quarantine_accum.append(df_loans_invalid.select(
            expr("uuid()").alias("quarantine_id"),
            lit("loan_accounts").alias("source_table"),
            col("account_id").alias("record_identifier"),
            lit("INVALID_BRANCH_FK").alias("rejection_code"),
            to_json(struct("*")).alias("raw_payload"),
            current_timestamp().alias("quarantined_at")
        ))

    df_loans_valid.withColumn("_loaded_at", current_timestamp()).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(os.path.join(SILVER_DIR, "loans"))
    npa_count = df_loans_valid.filter(col("is_regulatory_npa") == True).count()
    print(f"   ✅ Saved {df_loans_valid.count():,} conformed loans ({npa_count:,} flagged as Regulatory NPA).")

    # -------------------------------------------------------------------------
    # 6. silver.transactions (PK: txn_id)
    # -------------------------------------------------------------------------
    print("⚙️ Processing silver.transactions...")
    df_tx_raw = spark.read.format("delta").load(os.path.join(BRONZE_DIR, "loan_transactions"))

    df_tx_staged = df_tx_raw.select(
        upper(trim(col("txn_id"))).alias("txn_id"),
        upper(trim(col("account_id"))).alias("account_id"),
        upper(trim(col("application_id"))).alias("application_id"),
        upper(trim(col("branch_code"))).alias("branch_code"),
        upper(trim(col("txn_type"))).alias("txn_type"),
        clean_currency_to_double("amount").alias("raw_amount"),
        parse_epoch_or_iso_timestamp("txn_timestamp").alias("txn_timestamp")
    )

    # Gate: Reject NULL or Empty Primary Keys
    df_tx_invalid_pk = df_tx_staged.filter(col("txn_id").isNull() | (trim(col("txn_id")) == ""))
    if df_tx_invalid_pk.count() > 0:
        quarantine_accum.append(df_tx_invalid_pk.select(
            expr("uuid()").alias("quarantine_id"),
            lit("loan_transactions").alias("source_table"),
            expr("coalesce(account_id, 'UNKNOWN_TXN')").alias("record_identifier"),
            lit("INVALID_PRIMARY_KEY").alias("rejection_code"),
            to_json(struct("*")).alias("raw_payload"),
            current_timestamp().alias("quarantined_at")
        ))

    df_tx_valid_pk = df_tx_staged.filter(col("txn_id").isNotNull() & (trim(col("txn_id")) != ""))

    w_tx = Window.partitionBy("txn_id").orderBy(col("txn_timestamp").desc())
    df_tx_dedupe = df_tx_valid_pk.withColumn("rk", row_number().over(w_tx)).filter(col("rk") == 1).drop("rk")
    df_tx_dedupe = df_tx_dedupe.withColumn("is_reversal", when(col("raw_amount") < 0, lit(True)).otherwise(lit(False))) \
                               .withColumn("clean_amount", spark_abs(col("raw_amount")))

    df_orphan_tx = df_tx_dedupe.join(df_loans_valid.select("account_id"), on="account_id", how="left_anti")
    if df_orphan_tx.count() > 0:
        quarantine_accum.append(df_orphan_tx.select(
            expr("uuid()").alias("quarantine_id"),
            lit("loan_transactions").alias("source_table"),
            col("txn_id").alias("record_identifier"),
            lit("ORPHAN_ACCOUNT_ID").alias("rejection_code"),
            to_json(struct("*")).alias("raw_payload"),
            current_timestamp().alias("quarantined_at")
        ))

    df_tx_valid = df_tx_dedupe.join(df_loans_valid.select("account_id", "principal_amount"), on="account_id", how="inner")

    df_tx_conformed = df_tx_valid.withColumn(
        "clean_amount",
        when(
            (col("txn_type") == "DISBURSEMENT") & (col("clean_amount").isNull() | (col("clean_amount") == 0)),
            col("principal_amount")
        ).when(
            (col("txn_type") == "DISBURSEMENT") & (col("clean_amount") > col("principal_amount") * 50),
            col("clean_amount") / 100.0
        ).otherwise(col("clean_amount"))
    ).drop("principal_amount")

    df_tx_conformed.withColumn("_loaded_at", current_timestamp()).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(os.path.join(SILVER_DIR, "transactions"))
    print(f"   ✅ Saved {df_tx_conformed.count():,} conformed transactions.")

    # -------------------------------------------------------------------------
    # 7. Quarantine Consolidation
    # -------------------------------------------------------------------------
    print("🛡️ Consolidating Quarantine dataset...")
    df_final_quarantine = quarantine_accum[0]
    for df_q in quarantine_accum[1:]:
        df_final_quarantine = df_final_quarantine.unionByName(df_q)

    df_final_quarantine.withColumn("_loaded_at", current_timestamp()).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(QUARANTINE_PATH)
    print(f"   ✅ Saved {df_final_quarantine.count():,} rejected/orphan records into {QUARANTINE_PATH}.")

    print("=" * 80)
    print("✅ Silver Lakehouse Conformation Complete.")
    print("=" * 80)
    spark.stop()

if __name__ == "__main__":
    run_silver_pipeline()