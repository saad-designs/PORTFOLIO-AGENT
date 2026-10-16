import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

conn_string = os.getenv("SUPABASE_DB_URL")
if not conn_string:
    print("Error: SUPABASE_DB_URL not set in .env")
    exit(1)

conn = psycopg2.connect(conn_string)
conn.autocommit = True
cur = conn.cursor()

try:
    print("Running migration to add eps, roic, and intrinsic_value columns...")
    cur.execute("ALTER TABLE fundamentals ADD COLUMN eps DECIMAL;")
    print("[OK] Successfully added 'eps' column.")
except psycopg2.errors.DuplicateColumn:
    print("[OK] 'eps' column already exists.")
except Exception as e:
    print(f"[WARN] Error executing query (eps): {e}")

try:
    cur.execute("ALTER TABLE fundamentals ADD COLUMN roic DECIMAL;")
    print("[OK] Successfully added 'roic' column.")
except psycopg2.errors.DuplicateColumn:
    print("[OK] 'roic' column already exists.")
except Exception as e:
    print(f"[WARN] Error executing query (roic): {e}")

try:
    cur.execute("ALTER TABLE fundamentals ADD COLUMN intrinsic_value DECIMAL;")
    print("[OK] Successfully added 'intrinsic_value' column.")
except psycopg2.errors.DuplicateColumn:
    print("[OK] 'intrinsic_value' column already exists.")
except Exception as e:
    print(f"[WARN] Error executing query (intrinsic_value): {e}")

finally:
    cur.close()
    conn.close()
    print("Migration complete.")
