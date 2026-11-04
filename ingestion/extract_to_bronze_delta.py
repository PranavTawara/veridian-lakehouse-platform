import os
import psycopg2
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

# DYNAMIC CONFIGURATION
LAKEHOUSE_ROOT = os.getenv("LAKEHOUSE_ROOT", os.path.abspath("./lakehouse"))
BRONZE_DIR = os.getenv("BRONZE_DIR", f"{LAKEHOUSE_ROOT}/bronze")
MAX_CONCURRENT_TABLES = int(os.getenv("INGESTION_CONCURRENCY", "4"))
JDBC_URL = os.getenv("SOURCE_DB_URL", "jdbc:postgresql://localhost:5432/veridian_db")
JDBC_PROPERTIES = {
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASS", "postgrespassword"),
    "driver": "org.postgresql.Driver"
}

def get_dynamic_tables():
    """Dynamically fetches all table names from PostgreSQL 'source' schema."""
    conn_str = JDBC_URL.replace("jdbc:postgresql", "postgresql") + f"?user={JDBC_PROPERTIES['user']}&password={JDBC_PROPERTIES['password']}"
    conn = psycopg2.connect(conn_str)
    with conn.cursor() as cur:
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'source';")
        tables = [row[0] for row in cur.fetchall()]
    conn.close()
    return tables

def get_spark():
    builder = (
        SparkSession.builder.appName("AutonomousBronzeExtraction")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.scheduler.mode", "FAIR") 
    )
    extra_packages = ["org.postgresql:postgresql:42.7.3"]
    if BRONZE_DIR.startswith("s3a://"):
        extra_packages.extend(["org.apache.hadoop:hadoop-aws:3.3.4", "com.amazonaws:aws-java-sdk-bundle:1.12.262"])
        builder = builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                         .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    return configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()

def process_single_table(spark, tbl):
    target_path = os.path.join(BRONZE_DIR, tbl)
    watermark = None
    if os.path.exists(target_path) and DeltaTable.isDeltaTable(spark, target_path):
        try:
            df_bronze = spark.read.format("delta").load(target_path)
            if "_extracted_at" in df_bronze.columns and df_bronze.count() > 0:
                watermark = df_bronze.agg({"_extracted_at": "max"}).collect()[0][0]
        except Exception:
            pass

    try:
        if watermark is None:
            query = f"source.{tbl}"
            mode = "overwrite"
            action = "COLD_START"
        else:
            wm_str = watermark.strftime("%Y-%m-%d %H:%M:%S")
            query = f"(SELECT * FROM source.{tbl} WHERE _loaded_at::timestamp > '{wm_str}'::timestamp) AS incremental_slice"
            mode = "append"
            action = "APPEND"

        df_incoming = spark.read.jdbc(url=JDBC_URL, table=query, properties=JDBC_PROPERTIES)
        rows_pulled = df_incoming.count()

        if rows_pulled == 0:
            return {"status": "SUCCESS", "table": tbl, "action": "SKIPPED", "rows": 0, "error": None}

        (
            df_incoming.withColumn("_extracted_at", current_timestamp())
            .write.format("delta")
            .mode(mode)
            .option("mergeSchema", "true")
            .save(target_path)
        )
        return {"status": "SUCCESS", "table": tbl, "action": action, "rows": rows_pulled, "error": None}

    except Exception as e:
        return {"status": "FAILED", "table": tbl, "action": "ERROR", "rows": 0, "error": str(e)}

def extract_to_bronze_concurrently():
    dynamic_tables = get_dynamic_tables()
    print("=" * 85)
    print(f"STEP 2: AUTONOMOUS BRONZE EXTRACTION | DISCOVERED {len(dynamic_tables)} TABLES")
    print("=" * 85)

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    successful, failed = [], []

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_TABLES) as executor:
        futures = {executor.submit(process_single_table, spark, tbl): tbl for tbl in dynamic_tables}
        for future in as_completed(futures):
            res = future.result()
            tbl = res["table"]
            if res["status"] == "SUCCESS":
                successful.append(tbl)
                if res["action"] == "SKIPPED":
                    print(f"   ℹ️ [{tbl}] In sync. 0 new rows transferred.")
                else:
                    print(f"   ✅ [{tbl}] {res['action']} complete ({res['rows']:,} rows).")
            else:
                failed.append(res)
                print(f"   ❌ [{tbl}] FAILED: {res['error']}")

    spark.stop()
    print(f"\n✅ Passed: {len(successful)} | ❌ Failed: {len(failed)}")
    if failed:
        raise RuntimeError("Pipeline halted: Table ingestion failure detected.")

if __name__ == "__main__":
    extract_to_bronze_concurrently()