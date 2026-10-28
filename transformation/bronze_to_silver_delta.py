import os
import sys
import argparse
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, trim, upper, regexp_replace, when, lit, 
    to_timestamp, to_date, row_number, current_timestamp, 
    expr, to_json, struct, abs as spark_abs
)
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

BRONZE_DIR = os.path.abspath("./lakehouse/bronze")
SILVER_DIR = os.path.abspath("./lakehouse/silver")
QUARANTINE_PATH = os.path.join(SILVER_DIR, "quarantine")

def get_spark_session():
    builder = (
        SparkSession.builder.appName("DynamicSilverConformation")
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

def parse_multi_format_date(col_name):
    return when(
        col(col_name).rlike(r"^\d{1,2}/\d{1,2}/\d{4}$"),
        to_date(col(col_name), "d/M/yyyy")
    ).when(
        col(col_name).rlike(r"^\d{4}-\d{2}-\d{2}$"),
        to_date(col(col_name), "yyyy-MM-dd")
    ).when(
        col(col_name).rlike(r"^\d{13}$"),
        (col(col_name).cast("double") / 1000.0).cast("timestamp").cast("date")
    ).otherwise(to_date(col(col_name)))

def clean_currency_to_double(col_name):
    return regexp_replace(col(col_name), r"[\$₹,\s]", "").cast("double")

def get_silver_watermark(spark, entity_name, watermark_col):
    target_path = os.path.join(SILVER_DIR, entity_name)
    if not DeltaTable.isDeltaTable(spark, target_path):
        return None
    try:
        max_ts = spark.read.format("delta").load(target_path).agg({watermark_col: "max"}).collect()[0][0]
        return max_ts
    except Exception:
        return None

def write_silver_entity(spark, df, entity_name, pk_cols, mode="full"):
    target_path = os.path.join(SILVER_DIR, entity_name)
    df_payload = df.withColumn("_loaded_at", current_timestamp())

    if mode == "full" or not DeltaTable.isDeltaTable(spark, target_path):
        df_payload.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target_path)
    else:
        delta_target = DeltaTable.forPath(spark, target_path)
        join_conditions = " AND ".join([f"target.{k} = source.{k}" for k in pk_cols])
        delta_target.alias("target").merge(
            df_payload.alias("source"),
            join_conditions
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

def run_silver_pipeline(mode="full", watermark_col="_extracted_at", lookback_hours=0):
    print("=" * 85)
    print(f"STEP 3: SILVER LAKEHOUSE CONFORMATION | MODE: {mode.upper()}")
    print("=" * 85)

    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    quarantine_accum = []

    def filter_incremental(df_raw, entity_name):
        if mode == "full":
            return df_raw
        watermark = get_silver_watermark(spark, entity_name, "_loaded_at")
        if watermark and watermark_col in df_raw.columns:
            effective_wm = watermark - timedelta(hours=lookback_hours)
            print(f"   Filtering {entity_name} where {watermark_col} >= {effective_wm}")
            return df_raw.filter(col(watermark_col) >= effective_wm)
        return df_raw

    # 1. silver.branches
    print("⚙️ Processing silver.branches...")
    df_bm_raw = filter_incremental(spark.read.format("delta").load(os.path.join(BRONZE_DIR, "branch_master")), "branches")
    df_bm_staged = df_bm_raw.select(
        upper(trim(col("branch_code"))).alias("branch_code"),
        trim(col("branch_name")).alias("branch_name"),
        upper(trim(col("region"))).alias("region"),
        trim(col("state")).alias("state"),
        trim(col("city")).alias("city"),
        trim(col("branch_type")).alias("branch_type"),
        to_date(col("open_date")).alias("open_date")
    )
    df_bm_valid = df_bm_staged.filter(col("branch_code").isNotNull() & ~col("branch_code").isin("BR-999", "BR-0XX"))
    w_branch = Window.partitionBy("branch_code").orderBy(col("open_date").desc())
    df_branches = df_bm_valid.withColumn("rk", row_number().over(w_branch)).filter(col("rk") == 1).drop("rk")
    write_silver_entity(spark, df_branches, "branches", ["branch_code"], mode=mode)

    valid_branch_codes = [r.branch_code for r in spark.read.format("delta").load(os.path.join(SILVER_DIR, "branches")).select("branch_code").distinct().collect()]

    # 2. silver.products
    print("⚙️ Processing silver.products...")
    df_pm_raw = filter_incremental(spark.read.format("delta").load(os.path.join(BRONZE_DIR, "product_master")), "products")
    df_pm_staged = df_pm_raw.select(
        when(trim(col("product_name")) == "Two-Wheeler Loan", lit("TL01"))
        .otherwise(upper(trim(col("product_code")))).alias("product_code"),
        trim(col("product_name")).alias("product_name"),
        trim(col("category")).alias("category"),
        clean_currency_to_double("interest_rate").alias("interest_rate")
    ).filter(col("product_code").isNotNull() & (trim(col("product_code")) != ""))
    w_prod = Window.partitionBy("product_code").orderBy(col("interest_rate").desc())
    df_pm_ranked = df_pm_staged.withColumn("rk", row_number().over(w_prod))
    
    df_pm_conflicts = df_pm_ranked.filter(col("rk") > 1)
    if df_pm_conflicts.count() > 0:
        quarantine_accum.append(df_pm_conflicts.select(
            expr("uuid()").alias("quarantine_id"), lit("product_master").alias("source_table"),
            col("product_code").alias("record_identifier"), lit("PRODUCT_RATE_CONFLICT").alias("rejection_code"),
            to_json(struct("*")).alias("raw_payload"), current_timestamp().alias("quarantined_at")
        ))
    df_products = df_pm_ranked.filter(col("rk") == 1).drop("rk")
    write_silver_entity(spark, df_products, "products", ["product_code"], mode=mode)

    # 3. silver.customers
    print("⚙️ Processing silver.customers...")
    df_cm_raw = filter_incremental(spark.read.format("delta").load(os.path.join(BRONZE_DIR, "customer_master")), "customers")
    df_cm_staged = df_cm_raw.select(
        upper(trim(col("customer_id"))).alias("customer_id"),
        regexp_replace(trim(col("customer_name")), r"\s+", " ").alias("customer_name"),
        trim(col("city")).alias("city"), trim(col("state")).alias("state"),
        upper(trim(col("kyc_status"))).alias("kyc_status"), trim(col("income_band")).alias("income_band"),
        parse_multi_format_date("onboarding_date").alias("onboarding_date")
    ).filter(col("customer_id").isNotNull())
    w_cust = Window.partitionBy("customer_id").orderBy(col("onboarding_date").desc(), when(col("kyc_status") == "VERIFIED", 1).otherwise(0).desc())
    df_customers = df_cm_staged.withColumn("rk", row_number().over(w_cust)).filter(col("rk") == 1).drop("rk")
    write_silver_entity(spark, df_customers, "customers", ["customer_id"], mode=mode)

    # 4. silver.employees
    print("⚙️ Processing silver.employees...")
    df_emp_raw = filter_incremental(spark.read.format("delta").load(os.path.join(BRONZE_DIR, "employee_roster")), "employees")
    df_emp_staged = df_emp_raw.select(
        upper(trim(col("employee_id"))).alias("employee_id"),
        regexp_replace(trim(col("employee_name")), r"\s+", " ").alias("employee_name"),
        upper(trim(col("branch_code"))).alias("branch_code"), trim(col("branch_name")).alias("branch_name"),
        upper(trim(col("role"))).alias("role"), to_date(col("join_date")).alias("join_date"),
        to_date(col("exit_date")).alias("exit_date"), upper(trim(col("status"))).alias("status"),
        clean_currency_to_double("annual_ctc").alias("annual_ctc")
    ).filter(col("employee_id").isNotNull())
    
    df_emp_invalid = df_emp_staged.filter(~col("branch_code").isin(valid_branch_codes))
    if df_emp_invalid.count() > 0:
        quarantine_accum.append(df_emp_invalid.select(
            expr("uuid()").alias("quarantine_id"), lit("employee_roster").alias("source_table"),
            col("employee_id").alias("record_identifier"), lit("INVALID_BRANCH_FK").alias("rejection_code"),
            to_json(struct("*")).alias("raw_payload"), current_timestamp().alias("quarantined_at")
        ))
    df_emp_valid = df_emp_staged.filter(col("branch_code").isin(valid_branch_codes))
    w_emp = Window.partitionBy("employee_id").orderBy(col("join_date").desc())
    df_employees = df_emp_valid.withColumn("rk", row_number().over(w_emp)).filter(col("rk") == 1).drop("rk")
    write_silver_entity(spark, df_employees, "employees", ["employee_id"], mode=mode)

    # 5. silver.loans
    print("⚙️ Processing silver.loans...")
    df_la_raw = filter_incremental(spark.read.format("delta").load(os.path.join(BRONZE_DIR, "loan_accounts")), "loans")
    df_la_staged = df_la_raw.select(
        upper(trim(col("account_id"))).alias("account_id"),
        upper(trim(col("application_id"))).alias("application_id"),
        upper(trim(col("branch_code"))).alias("branch_code"),
        when(col("product_code").isNull() | (trim(col("product_code")) == ""), lit("TL01")).otherwise(upper(trim(col("product_code")))).alias("product_code"),
        clean_currency_to_double("principal_amount").alias("principal_amount"),
        clean_currency_to_double("outstanding_amount").alias("outstanding_amount"),
        when(col("dpd").isNull(), lit(0)).otherwise(col("dpd").cast("integer")).alias("dpd"),
        upper(trim(col("npa_flag"))).alias("source_npa_flag"),
        to_date(col("open_date")).alias("open_date"),
        parse_epoch_or_iso_timestamp("last_updated").alias("last_updated")
    ).filter(col("account_id").isNotNull())

    df_la_tagged = df_la_staged.withColumn(
        "is_regulatory_npa",
        when((col("dpd") >= 90) | (col("source_npa_flag").isin("Y", "YES", "1")), lit(True)).otherwise(lit(False))
    )
    df_loans_invalid = df_la_tagged.filter(~col("branch_code").isin(valid_branch_codes))
    if df_loans_invalid.count() > 0:
        quarantine_accum.append(df_loans_invalid.select(
            expr("uuid()").alias("quarantine_id"), lit("loan_accounts").alias("source_table"),
            col("account_id").alias("record_identifier"), lit("INVALID_BRANCH_FK").alias("rejection_code"),
            to_json(struct("*")).alias("raw_payload"), current_timestamp().alias("quarantined_at")
        ))
    df_loans_valid = df_la_tagged.filter(col("branch_code").isin(valid_branch_codes))
    write_silver_entity(spark, df_loans_valid, "loans", ["account_id"], mode=mode)

    accumulated_loans = spark.read.format("delta").load(os.path.join(SILVER_DIR, "loans"))

    # 6. silver.transactions
    print("⚙️ Processing silver.transactions...")
    df_tx_raw = filter_incremental(spark.read.format("delta").load(os.path.join(BRONZE_DIR, "loan_transactions")), "transactions")
    df_tx_staged = df_tx_raw.select(
        upper(trim(col("txn_id"))).alias("txn_id"),
        upper(trim(col("account_id"))).alias("account_id"),
        upper(trim(col("application_id"))).alias("application_id"),
        upper(trim(col("branch_code"))).alias("branch_code"),
        upper(trim(col("txn_type"))).alias("txn_type"),
        clean_currency_to_double("amount").alias("raw_amount"),
        parse_epoch_or_iso_timestamp("txn_timestamp").alias("txn_timestamp")
    ).filter(col("txn_id").isNotNull())

    w_tx = Window.partitionBy("txn_id").orderBy(col("txn_timestamp").desc())
    df_tx_dedupe = df_tx_staged.withColumn("rk", row_number().over(w_tx)).filter(col("rk") == 1).drop("rk") \
                               .withColumn("is_reversal", when(col("raw_amount") < 0, lit(True)).otherwise(lit(False))) \
                               .withColumn("clean_amount", spark_abs(col("raw_amount")))

    df_orphan_tx = df_tx_dedupe.join(accumulated_loans.select("account_id"), on="account_id", how="left_anti")
    if df_orphan_tx.count() > 0:
        quarantine_accum.append(df_orphan_tx.select(
            expr("uuid()").alias("quarantine_id"), lit("loan_transactions").alias("source_table"),
            col("txn_id").alias("record_identifier"), lit("ORPHAN_ACCOUNT_ID").alias("rejection_code"),
            to_json(struct("*")).alias("raw_payload"), current_timestamp().alias("quarantined_at")
        ))

    df_tx_valid = df_tx_dedupe.join(accumulated_loans.select("account_id", "principal_amount"), on="account_id", how="inner")
    df_tx_adjusted = df_tx_valid.withColumn(
        "clean_amount",
        when((col("txn_type") == "DISBURSEMENT") & (col("clean_amount").isNull() | (col("clean_amount") == 0)), col("principal_amount"))
        .when((col("txn_type") == "DISBURSEMENT") & (col("clean_amount") > col("principal_amount") * 50), col("clean_amount") / 100.0)
        .otherwise(col("clean_amount"))
    ).drop("principal_amount")

    df_tx_invalid = df_tx_adjusted.filter(col("clean_amount").isNull() | (col("clean_amount") <= 0))
    if df_tx_invalid.count() > 0:
        quarantine_accum.append(df_tx_invalid.select(
            expr("uuid()").alias("quarantine_id"), lit("loan_transactions").alias("source_table"),
            col("txn_id").alias("record_identifier"), lit("INVALID_TXN_AMOUNT").alias("rejection_code"),
            to_json(struct("*")).alias("raw_payload"), current_timestamp().alias("quarantined_at")
        ))

    df_tx_conformed = df_tx_adjusted.filter(col("clean_amount").isNotNull() & (col("clean_amount") > 0))
    
    if mode == "full" or not DeltaTable.isDeltaTable(spark, os.path.join(SILVER_DIR, "transactions")):
        df_tx_conformed.withColumn("_loaded_at", current_timestamp()).write.format("delta") \
                       .mode("overwrite").option("overwriteSchema", "true").save(os.path.join(SILVER_DIR, "transactions"))
    else:
        df_tx_conformed.withColumn("_loaded_at", current_timestamp()).write.format("delta") \
                       .mode("append").save(os.path.join(SILVER_DIR, "transactions"))

    # 7. Quarantine Consolidation
    if quarantine_accum:
        df_new_quar = quarantine_accum[0]
        for q in quarantine_accum[1:]:
            df_new_quar = df_new_quar.unionByName(q)
        
        quar_write_mode = "overwrite" if mode == "full" else "append"
        df_new_quar.withColumn("_loaded_at", current_timestamp()).write.format("delta") \
                   .mode(quar_write_mode).option("mergeSchema", "true").save(QUARANTINE_PATH)

    print(f"✅ Conformation completed in {mode.upper()} mode.")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dynamic Medallion Conformation Engine")
    parser.add_argument("--mode", default=os.getenv("LOAD_MODE", "full"), choices=["full", "incremental"])
    parser.add_argument("--watermark-col", default=os.getenv("WATERMARK_COL", "_extracted_at"))
    parser.add_argument("--lookback-hours", type=int, default=int(os.getenv("WATERMARK_LOOKBACK_HOURS", 0)))
    args = parser.parse_args()

    run_silver_pipeline(mode=args.mode, watermark_col=args.watermark_col, lookback_hours=args.lookback_hours)