import os
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from delta import configure_spark_with_delta_pip

JDBC_URL = "jdbc:postgresql://localhost:5432/veridian_db"
JDBC_PROPERTIES = {
    "user": "postgres",
    "password": "postgrespassword",
    "driver": "org.postgresql.Driver"
}
BRONZE_DIR = os.path.abspath("./lakehouse/bronze")

def get_spark_session():
    builder = (
        SparkSession.builder.appName("BronzeDeltaExtraction")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .config("spark.driver.memory", "2g")
    )
    # Pass PostgreSQL JDBC package here so configure_spark_with_delta_pip resolves both
    extra_packages = ["org.postgresql:postgresql:42.7.3"]
    return configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()

def extract_to_bronze():
    print("=" * 80)
    print("STEP 2: EXTRACTING POSTGRESQL (source.*) -> BRONZE DELTA LAKEHOUSE")
    print("=" * 80)

    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    tables = [
        "account_status_events",
        "branch_financials",
        "branch_master",
        "customer_master",
        "deposit_accounts",
        "employee_roster",
        "loan_accounts",
        "loan_applications",
        "loan_transactions",
        "product_master",
        "region_demographics",
    ]

    batch_id = f"BATCH_BRONZE_{uuid.uuid4().hex[:8]}"

    for table in tables:
        target_path = os.path.join(BRONZE_DIR, table)
        print(f"📦 Reading source.{table} via JDBC -> {target_path}...")

        # 1:1 lossless read from operational Postgres
        df_source = spark.read.jdbc(
            url=JDBC_URL, table=f"source.{table}", properties=JDBC_PROPERTIES
        )

        # Append bronze lineage audit fields
        df_bronze = (
            df_source.withColumn("_extracted_at", current_timestamp())
            .withColumn("_pipeline_batch_id", lit(batch_id))
            .withColumn("_source_table", lit(f"source.{table}"))
        )

        # Write to Delta table (generates Parquet files + _delta_log/ transaction log)
        df_bronze.write.format("delta").mode("overwrite").save(target_path)

        count = spark.read.format("delta").load(target_path).count()
        print(f"   ✅ Saved {count:,} rows into Bronze Delta table ({table})")

    print("=" * 80)
    print("✅ Bronze Lakehouse Extraction Complete: 11 Delta tables populated.")
    print("=" * 80)
    spark.stop()

if __name__ == "__main__":
    extract_to_bronze()
