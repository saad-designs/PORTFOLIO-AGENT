import os, psycopg2
from dotenv import load_dotenv
load_dotenv('.env')
conn = psycopg2.connect(os.getenv('SUPABASE_DB_URL'))
cur = conn.cursor()
try:
    cur.execute("ALTER TABLE portfolio_holdings ADD COLUMN IF NOT EXISTS discord_user_id VARCHAR(255);")
    conn.commit()
    print("Added discord_user_id to portfolio_holdings.")
except Exception as e:
    conn.rollback()
    print("Error adding discord_user_id:", e)

cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public';")
tables = cur.fetchall()
print("Tables:", tables)

cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'cash_ledger';")
print("cash_ledger columns:", cur.fetchall())

cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'portfolio_cash';")
print("portfolio_cash columns:", cur.fetchall())

try:
    # Try inserting cash to cash_ledger (multi-user model)
    cur.execute("INSERT INTO cash_ledger (discord_user_id, transaction_type, amount, ticker) VALUES ('123456', 'DEPOSIT', 500000, NULL);")
    conn.commit()
    print("Injected 500k into cash_ledger for user 123456.")
except Exception as e:
    conn.rollback()
    print("Error injecting to cash_ledger:", e)

try:
    # Try updating portfolio_cash (legacy single-user model)
    cur.execute("UPDATE portfolio_cash SET liquid_cash = 500000 WHERE id = 1;")
    conn.commit()
    print("Injected 500k into portfolio_cash id=1.")
except Exception as e:
    conn.rollback()
    print("Error injecting to portfolio_cash:", e)
