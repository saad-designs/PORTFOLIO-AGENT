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
    cur.execute("ALTER TABLE watchlist_targets ADD COLUMN status VARCHAR(20) DEFAULT 'PENDING';")
    print("✅ Successfully added 'status' column to watchlist_targets table.")
except psycopg2.errors.DuplicateColumn:
    print("✅ 'status' column already exists in watchlist_targets table.")
except Exception as e:
    print(f"⚠️ Error executing query: {e}")
finally:
    cur.close()
    conn.close()
