import os
import psycopg2
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
DATABASE_URL = os.getenv("SUPABASE_DB_URL")

if not DATABASE_URL:
    print("No database URL")
    exit(1)

sql_query = """
CREATE TABLE IF NOT EXISTS portfolio_daily_snapshots (
    id SERIAL PRIMARY KEY,
    snapshot_date DATE UNIQUE NOT NULL DEFAULT CURRENT_DATE,
    equity_value NUMERIC(12, 2) NOT NULL,
    cash_balance NUMERIC(12, 2) NOT NULL,
    total_portfolio_value NUMERIC(12, 2) NOT NULL,
    daily_pnl NUMERIC(12, 2) DEFAULT 0.00,
    daily_pnl_pct NUMERIC(6, 3) DEFAULT 0.00,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_snapshot_date ON portfolio_daily_snapshots(snapshot_date);
"""

try:
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.execute(sql_query)
    conn.commit()
    cursor.close()
    conn.close()
    print("Table portfolio_daily_snapshots created successfully.")
except Exception as e:
    print(f"Error creating table: {e}")
