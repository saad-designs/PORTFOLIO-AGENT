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
    print("Running migration to add dividend_yield column...")
    cur.execute("ALTER TABLE fundamentals ADD COLUMN dividend_yield DECIMAL;")
    print("[OK] Successfully added 'dividend_yield' column.")
except psycopg2.errors.DuplicateColumn:
    print("[OK] 'dividend_yield' column already exists.")
except Exception as e:
    print(f"[WARN] Error executing query (dividend_yield): {e}")
finally:
    cur.close()
    conn.close()
    print("Migration complete.")
