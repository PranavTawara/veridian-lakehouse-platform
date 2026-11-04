import os
import glob
import csv
from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import execute_values

# DYNAMIC CONFIGURATION
DATA_DIR = os.getenv("SOURCE_DATA_DIR", os.path.abspath(os.path.join(os.path.dirname(__file__), "../data/initial_load")))
DB_URL = os.getenv("SOURCE_DB_URL", "jdbc:postgresql://localhost:5432/veridian_db").replace("jdbc:postgresql", "postgresql")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "postgrespassword")
DB_CONN = f"{DB_URL}?user={DB_USER}&password={DB_PASS}"

def seed_operational_source():
    print("=" * 80)
    print(f"STEP 1: AUTONOMOUS OPERATIONAL POSTGRESQL SEEDING")
    print(f"Reading CSVs from: {DATA_DIR}")
    print("=" * 80)

    conn = psycopg2.connect(DB_CONN)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("CREATE SCHEMA IF NOT EXISTS source;")
    cur.execute("CREATE SCHEMA IF NOT EXISTS staging;")

    csv_files = sorted(glob.glob(os.path.join(DATA_DIR, "*.csv")))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {DATA_DIR}")

    batch_name = os.path.basename(os.path.normpath(DATA_DIR))
    batch_id = f"BATCH_{batch_name.upper()}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    loaded_at = datetime.now(timezone.utc).isoformat()
    total_inserted = 0

    for file_path in csv_files:
        table_name = os.path.splitext(os.path.basename(file_path))[0].lower()
        filename = os.path.basename(file_path)

        with open(file_path, "r", encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            raw_headers = next(reader)
            clean_cols = [c.strip().lower().replace(" ", "_").replace("-", "_") for c in raw_headers]
            
            # Check if source table already exists
            cur.execute("SELECT to_regclass(%s);", (f'source."{table_name}"',))
            table_exists = cur.fetchone()[0] is not None

            if not table_exists:
                print(f"📦 [COLD START] Creating source.{table_name} from {filename}...")
                all_cols = clean_cols + ["_batch_id", "_loaded_at", "_source_file"]
                col_defs = ", ".join([f'"{c}" TEXT' for c in all_cols])
                cur.execute(f'CREATE TABLE source."{table_name}" ({col_defs});')

                rows = [row + [batch_id, loaded_at, filename] for row in reader]
                cols_sql = ", ".join([f'"{c}"' for c in all_cols])
                insert_query = f'INSERT INTO source."{table_name}" ({cols_sql}) VALUES %s'
                execute_values(cur, insert_query, rows, page_size=5000)
                print(f"   ✅ Inserted {len(rows):,} baseline rows.")
                total_inserted += len(rows)

            else:
                print(f"🔄 [INCREMENTAL] Diffing {filename} against existing source.{table_name}...")
                temp_table = f"temp_{table_name}"
                col_defs = ", ".join([f'"{c}" TEXT' for c in clean_cols])
                cur.execute(f'DROP TABLE IF EXISTS staging."{temp_table}";')
                cur.execute(f'CREATE UNLOGGED TABLE staging."{temp_table}" ({col_defs});')

                rows = [row for row in reader]
                cols_sql = ", ".join([f'"{c}"' for c in clean_cols])
                insert_query = f'INSERT INTO staging."{temp_table}" ({cols_sql}) VALUES %s'
                execute_values(cur, insert_query, rows, page_size=5000)

                # Identify rows in staging that do not match existing data
                data_col_match = " AND ".join([f's."{c}" = t."{c}"' for c in clean_cols])
                diff_query = f"""
                    INSERT INTO source."{table_name}" ({cols_sql}, "_batch_id", "_loaded_at", "_source_file")
                    SELECT t.*, '{batch_id}', '{loaded_at}', '{filename}'
                    FROM staging."{temp_table}" t
                    WHERE NOT EXISTS (
                        SELECT 1 FROM source."{table_name}" s
                        WHERE {data_col_match}
                    );
                """
                cur.execute(diff_query)
                inserted_changes = cur.rowcount
                cur.execute(f'DROP TABLE IF EXISTS staging."{temp_table}";')
                
                if inserted_changes > 0:
                    print(f"   📥 Appended {inserted_changes:,} new/modified rows.")
                    total_inserted += inserted_changes
                else:
                    print(f"   ℹ️ 0 rows changed. Existing timestamps preserved.")

    cur.close()
    conn.close()
    print("=" * 80)
    print(f"✅ Operational Seeding Complete ({total_inserted:,} net rows loaded).")
    print("=" * 80)

if __name__ == "__main__":
    seed_operational_source()