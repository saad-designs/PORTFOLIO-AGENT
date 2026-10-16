import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()
conn = psycopg2.connect(os.getenv("SUPABASE_DB_URL"))
cur = conn.cursor()
cur.execute("SELECT fcf, shares_outstanding, historical_growth, total_cash, total_debt FROM fundamentals WHERE ticker = 'SYS'")
print("Raw DB Values:", cur.fetchone())
cur.close()
conn.close()
