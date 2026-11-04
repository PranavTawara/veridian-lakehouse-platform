import os
import psycopg2
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

WORKSPACE_DIR = os.path.abspath(os.path.dirname(__file__))
BRONZE_DIR = os.path.join(WORKSPACE_DIR, "lakehouse", "bronze")
SILVER_DIR = os.path.join(WORKSPACE_DIR, "lakehouse", "silver")

def verify_layers():
    print("=" * 85)
    print("🔍 VERIDIAN LAKEHOUSE: CROSS-LAYER DATA VERIFICATION")
    print("=" * 85)

    # 1. Start Spark for Bronze & Silver Delta Lakes
    builder = (
        SparkSession.builder.appName("LayerVerification")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "2g")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # 2. Connect to Postgres for Gold Marts
    conn = psycopg2.connect("postgresql://postgres:postgrespassword@localhost:5432/veridian_db")
    cur = conn.cursor()

    # Mapping of logical entities to their physical table names at each layer
    entities = {
        "Branches": {"bronze": "branch_master", "silver": "branches", "gold": "dim_branch"},
        "Customers": {"bronze": "customer_master", "silver": "customers", "gold": "dim_customer"},
        "Employees": {"bronze": "employee_roster", "silver": "employees", "gold": "dim_employee"},
        "Products": {"bronze": "product_master", "silver": "products", "gold": "dim_product"},
        "Loans": {"bronze": "loan_accounts", "silver": "loans", "gold": "N/A (Fact)"},
        "Transactions": {"bronze": "loan_transactions", "silver": "transactions", "gold": "N/A (Fact)"}
    }

    print(f"{'Entity':<15} | {'Bronze (Raw)':<15} | {'Silver (Conformed)':<20} | {'Gold (Dimension)':<15}")
    print("-" * 75)

    for entity, paths in entities.items():
        # Bronze Count
        try:
            b_count = f"{spark.read.format('delta').load(os.path.join(BRONZE_DIR, paths['bronze'])).count():,}"
        except Exception:
            b_count = "Missing"

        # Silver Count
        try:
            s_count = f"{spark.read.format('delta').load(os.path.join(SILVER_DIR, paths['silver'])).count():,}"
        except Exception:
            s_count = "Missing"

        # Gold Count
        if "N/A" not in paths["gold"]:
            try:
                cur.execute(f"SELECT COUNT(*) FROM serving.{paths['gold']};")
                g_count = f"{cur.fetchone()[0]:,}"
            except Exception:
                g_count = "Missing"
        else:
            g_count = "-"

        print(f"{entity:<15} | {b_count:<15} | {s_count:<20} | {g_count:<15}")

    # 3. Check Gold Fact Tables
    print("\n" + "-" * 75)
    print(f"{'Gold Fact Table (PostgreSQL serving.*)':<54} | {'Row Count':<15}")
    print("-" * 75)
    fact_tables = ["fact_branch_monthly_pnl", "fact_regional_risk_profile", "fact_branch_staffing"]
    for ft in fact_tables:
        try:
            cur.execute(f"SELECT COUNT(*) FROM serving.{ft};")
            count = f"{cur.fetchone()[0]:,}"
            print(f"{ft:<54} | {count:<15}")
        except Exception:
            print(f"{ft:<54} | Missing")

    conn.close()
    spark.stop()
    print("=" * 85)

if __name__ == "__main__":
    verify_layers()