import os
import argparse
import psycopg2
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

JDBC_URL = "jdbc:postgresql://localhost:5432/veridian_db"
JDBC_PROPERTIES = {
    "user": "postgres",
    "password": "postgrespassword",
    "driver": "org.postgresql.Driver"
}
SILVER_DIR = os.path.abspath("./lakehouse/silver")

PRIMARY_KEYS = {
    "branches": ["branch_code"],
    "products": ["product_code"],
    "customers": ["customer_id"],
    "employees": ["employee_id"],
    "loans": ["account_id"],
    "transactions": ["txn_id"],
    "quarantine": ["quarantine_id"]
}

def stage_silver(mode="full"):
    print("=" * 85)
    print(f"STAGING SILVER DELTA TO POSTGRESQL | MODE: {mode.upper()}")
    print("=" * 85)

    conn = psycopg2.connect("postgresql://postgres:postgrespassword@localhost:5432/veridian_db")
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS silver;")
        cur.execute("CREATE SCHEMA IF NOT EXISTS serving;")

    builder = (
        SparkSession.builder.appName("DynamicStageSilver")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "2g")
    )
    spark = configure_spark_with_delta_pip(builder, extra_packages=["org.postgresql:postgresql:42.7.3"]).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    silver_entities = ["branches", "products", "customers", "employees", "loans", "transactions", "quarantine"]

    for entity in silver_entities:
        delta_path = os.path.join(SILVER_DIR, entity)
        if not os.path.exists(delta_path):
            continue

        df = spark.read.format("delta").load(delta_path)

        if mode == "full":
            df.write.jdbc(url=JDBC_URL, table=f"silver.{entity}", mode="overwrite", properties=JDBC_PROPERTIES)
            pk = ", ".join(PRIMARY_KEYS[entity])
            with conn.cursor() as cur:
                try:
                    cur.execute(f"ALTER TABLE silver.{entity} ADD PRIMARY KEY ({pk});")
                except Exception:
                    pass
            print(f"   ✅ [Full Overwrite] Staged silver.{entity} ({df.count():,} rows)")
        else:
            temp_table = f"silver_temp_{entity}"
            df.write.jdbc(url=JDBC_URL, table=f"silver.{temp_table}", mode="overwrite", properties=JDBC_PROPERTIES)
            
            cols = [f'"{c}"' for c in df.columns]
            col_list = ", ".join(cols)
            pk = PRIMARY_KEYS[entity][0]
            update_set = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in df.columns if c != pk])

            upsert_sql = f"""
                INSERT INTO silver.{entity} ({col_list})
                SELECT {col_list} FROM silver.{temp_table}
                ON CONFLICT ("{pk}") DO UPDATE SET {update_set};
                DROP TABLE IF EXISTS silver.{temp_table};
            """
            with conn.cursor() as cur:
                cur.execute(upsert_sql)
            print(f"   ✅ [Incremental Upsert] Merged into silver.{entity}")

    conn.close()
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dynamic Silver-to-Postgres Bridge")
    parser.add_argument("--mode", default=os.getenv("LOAD_MODE", "full"), choices=["full", "incremental"])
    args = parser.parse_args()
    stage_silver(mode=args.mode)