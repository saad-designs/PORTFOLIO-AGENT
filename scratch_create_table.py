import os
import psycopg
from dotenv import load_dotenv

load_dotenv(r"c:\My Things\Portfolio agent\portfolio_agent\.env")

db_url = os.getenv("SUPABASE_DB_URL")
if not db_url:
    print("No SUPABASE_DB_URL found.")
    exit(1)

sql = """
CREATE TABLE IF NOT EXISTS public.cash_ledger (
    transaction_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    discord_user_id VARCHAR(50) NOT NULL,
    transaction_type VARCHAR(20) NOT NULL, -- Restrict to: 'DEPOSIT', 'WITHDRAWAL', 'DIVIDEND'
    amount NUMERIC NOT NULL,
    ticker VARCHAR(20), -- Optional: Leave null for deposits, use ticker for dividends
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
"""

try:
    with psycopg.connect(db_url, autocommit=True) as conn:
        conn.execute(sql)
    print("Table public.cash_ledger created successfully.")
except Exception as e:
    print(f"Error: {e}")
