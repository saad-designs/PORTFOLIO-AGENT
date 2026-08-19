import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()
try:
    conn = psycopg2.connect(os.getenv("SUPABASE_DB_URL"))
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute("ALTER TABLE transactions ADD COLUMN fees NUMERIC DEFAULT 0;")
    print("Added fees column to transactions table.")
except Exception as e:
    print(f"Error (column might already exist): {e}")
