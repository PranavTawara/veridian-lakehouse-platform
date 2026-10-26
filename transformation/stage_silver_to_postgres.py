import os
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

def stage_silver():
    print("=" * 80)
    print("STAGING SILVER DELTA TABLES INTO POSTGRESQL (SCHEMA: silver)")
    print("=" * 80)

    # 1. Ensure Postgres schemas exist
    conn = psycopg2.connect("postgresql://postgres:postgrespassword@localhost:5432/veridian_db")
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS silver;")
        cur.execute("CREATE SCHEMA IF NOT EXISTS serving;")
    conn.close()

    # 2. Spark Session with Delta and Postgres JDBC
    builder = (
        SparkSession.builder.appName("StageSilverToPostgres")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .config("spark.driver.memory", "2g")
    )
    spark = configure_spark_with_delta_pip(builder, extra_packages=["org.postgresql:postgresql:42.7.3"]).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    silver_entities = ["branches", "products", "customers", "employees", "loans", "transactions", "quarantine"]

    for entity in silver_entities:
        delta_path = os.path.join(SILVER_DIR, entity)
        if not os.path.exists(delta_path):
            print(f"⚠️ Skipping {entity}, path not found: {delta_path}")
            continue

        print(f"📥 Staging {entity} (Delta) -> Postgres silver.{entity}...")
        df = spark.read.format("delta").load(delta_path)

        df.write.jdbc(
            url=JDBC_URL,
            table=f"silver.{entity}",
            mode="overwrite",
            properties=JDBC_PROPERTIES
        )
        print(f"   ✅ Successfully staged silver.{entity} ({df.count():,} rows)")

    print("=" * 80)
    print("✅ Staging Complete: Silver Delta tables are now queryable in PostgreSQL!")
    print("=" * 80)
    spark.stop()

if __name__ == "__main__":
    stage_silver()