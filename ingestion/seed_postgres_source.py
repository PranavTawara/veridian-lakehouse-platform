import os
import glob
import csv
from datetime import datetime, timezone
import psycopg2
from psycopg2.extras import execute_values

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(BASE_DIR, "data", "initial_load")
DB_CONN = "postgresql://postgres:postgrespassword@localhost:5432/veridian_db"

def seed_operational_source():
    print("=" * 80)
    print("STEP 1: SEEDING OPERATIONAL POSTGRESQL (SCHEMA: source)")
    print("=" * 80)

    conn = psycopg2.connect(DB_CONN)
    conn.autocommit = False
    cur = conn.cursor()

    cur.execute("CREATE SCHEMA IF NOT EXISTS source;")
    cur.execute("CREATE SCHEMA IF NOT EXISTS serving;")
    conn.commit()

    csv_files = sorted(glob.glob(os.path.join(DATA_DIR, "*.csv")))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {DATA_DIR}")

    batch_id = f"BATCH_INIT_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    loaded_at = datetime.now(timezone.utc).isoformat()
    total_records = 0

    for file_path in csv_files:
        table_name = os.path.splitext(os.path.basename(file_path))[0].lower()
        filename = os.path.basename(file_path)

        with open(file_path, "r", encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            raw_headers = next(reader)
            clean_cols = [c.strip().lower().replace(" ", "_").replace("-", "_") for c in raw_headers]
            all_cols = clean_cols + ["_batch_id", "_loaded_at", "_source_file"]

            col_defs = ", ".join([f'"{c}" TEXT' for c in all_cols])
            cur.execute(f'DROP TABLE IF EXISTS source."{table_name}" CASCADE;')
            cur.execute(f'CREATE TABLE source."{table_name}" ({col_defs});')

            rows = [row + [batch_id, loaded_at, filename] for row in reader]
            print(f"📥 Ingesting {filename} -> source.{table_name} ({len(rows):,} rows)...")

            cols_sql = ", ".join([f'"{c}"' for c in all_cols])
            insert_query = f'INSERT INTO source."{table_name}" ({cols_sql}) VALUES %s'
            execute_values(cur, insert_query, rows, page_size=5000)
            conn.commit()
            total_records += len(rows)

    cur.close()
    conn.close()

    print("=" * 80)
    print(f"✅ Ingestion Complete: {total_records:,} rows loaded across {len(csv_files)} tables.")
    print("=" * 80)

if __name__ == "__main__":
    seed_operational_source()