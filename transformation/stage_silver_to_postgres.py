import os
import argparse
import psycopg2
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number
from delta import configure_spark_with_delta_pip

# DYNAMIC CONFIGURATION
LAKEHOUSE_ROOT = os.getenv("LAKEHOUSE_ROOT", os.path.abspath("./lakehouse"))
SILVER_DIR = os.getenv("SILVER_DIR", f"{LAKEHOUSE_ROOT}/silver")
JDBC_URL = os.getenv("TARGET_DB_URL", "jdbc:postgresql://localhost:5432/veridian_db")
JDBC_PROPERTIES = {
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASS", "postgrespassword"),
    "driver": "org.postgresql.Driver"
}

PRIMARY_KEYS = {
    "branches": ["branch_code"],
    "products": ["product_code"],
    "customers": ["customer_id"],
    "employees": ["employee_id"],
    "loans": ["account_id"],
    "transactions": ["txn_id"],
    "quarantine": ["quarantine_id"]
}

def stage_silver(mode="auto"):
    print("=" * 85)
    print(f"AUTONOMOUS SILVER DELTA STAGING TO POSTGRESQL | MODE: {mode.upper()}")
    print("=" * 85)

    conn = psycopg2.connect(JDBC_URL.replace("jdbc:postgresql", "postgresql") + f"?user={JDBC_PROPERTIES['user']}&password={JDBC_PROPERTIES['password']}")
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS silver;")
        cur.execute("CREATE SCHEMA IF NOT EXISTS serving;")

    builder = (
        SparkSession.builder.appName("AutonomousStageSilver")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
    )
    extra_packages = ["org.postgresql:postgresql:42.7.3"]
    if SILVER_DIR.startswith("s3a://"):
        extra_packages.extend(["org.apache.hadoop:hadoop-aws:3.3.4", "com.amazonaws:aws-java-sdk-bundle:1.12.262"])
        builder = builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                         .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        
    spark = configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    for entity, pk_cols in PRIMARY_KEYS.items():
        delta_path = os.path.join(SILVER_DIR, entity)
        if not os.path.exists(delta_path):
            continue

        df_raw = spark.read.format("delta").load(delta_path)
        w = Window.partitionBy(*pk_cols).orderBy(col("_loaded_at").desc())
        df = df_raw.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).drop("rn")

        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass(%s);", (f"silver.{entity}",))
            table_exists = cur.fetchone()[0] is not None
            row_count = 0
            if table_exists:
                cur.execute(f"SELECT COUNT(*) FROM silver.{entity};")
                row_count = cur.fetchone()[0]

        entity_mode = "incremental" if (table_exists and row_count > 0 and mode != "full") else "full"

        if entity_mode == "full":
            print(f"📥 [Full Overwrite] Staging {entity} -> Postgres silver.{entity}...")
            df.write.jdbc(url=JDBC_URL, table=f"silver.{entity}", mode="overwrite", properties=JDBC_PROPERTIES)
            with conn.cursor() as cur:
                try: cur.execute(f"ALTER TABLE silver.{entity} ADD PRIMARY KEY ({', '.join([f'\"{k}\"' for k in pk_cols])});")
                except Exception: pass
        else:
            print(f"📥 [Incremental Upsert] Staging {entity} -> Postgres silver.{entity}...")
            temp_table = f"silver_temp_{entity}"
            df.write.jdbc(url=JDBC_URL, table=f"silver.{temp_table}", mode="overwrite", properties=JDBC_PROPERTIES)
            
            pk = pk_cols[0]
            col_list = ", ".join([f'"{c}"' for c in df.columns])
            update_set = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in df.columns if c != pk])

            upsert_sql = f"""
                INSERT INTO silver.{entity} ({col_list})
                SELECT DISTINCT ON ("{pk}") {col_list} FROM silver.{temp_table} ORDER BY "{pk}", "_loaded_at" DESC
                ON CONFLICT ("{pk}") DO UPDATE SET {update_set};
                DROP TABLE IF EXISTS silver.{temp_table};
            """
            with conn.cursor() as cur:
                cur.execute(upsert_sql)
            print(f"   ✅ Merged delta into silver.{entity}")

    conn.close()
    spark.stop()
    print("=" * 85)
    print("✅ Staging Complete.")
    print("=" * 85)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", default=os.getenv("LOAD_MODE", "auto"), choices=["auto", "full", "incremental"])
    args = parser.parse_args()
    stage_silver(mode=args.mode)