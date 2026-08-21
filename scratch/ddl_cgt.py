import os, psycopg2
from dotenv import load_dotenv
load_dotenv('.env')
conn = psycopg2.connect(os.getenv('SUPABASE_DB_URL'))
cur = conn.cursor()

cur.execute('''
CREATE TABLE IF NOT EXISTS tax_lots (
    lot_id SERIAL PRIMARY KEY,
    discord_user_id VARCHAR(255) NOT NULL,
    ticker VARCHAR(50) NOT NULL,
    buy_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    buy_price NUMERIC(15, 4) NOT NULL,
    initial_quantity NUMERIC(15, 4) NOT NULL,
    remaining_quantity NUMERIC(15, 4) NOT NULL,
    status VARCHAR(20) DEFAULT 'OPEN'
);

CREATE TABLE IF NOT EXISTS realized_cgt_ledger (
    trade_id SERIAL PRIMARY KEY,
    discord_user_id VARCHAR(255) NOT NULL,
    ticker VARCHAR(50) NOT NULL,
    sell_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    sell_quantity NUMERIC(15, 4) NOT NULL,
    sell_price NUMERIC(15, 4) NOT NULL,
    gross_realized_pnl NUMERIC(15, 4) NOT NULL,
    cgt_deducted NUMERIC(15, 4) NOT NULL, 
    net_realized_pnl NUMERIC(15, 4) NOT NULL
);
''')
conn.commit()

cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public';")
print('Tables:', [r[0] for r in cur.fetchall()])

for table in ['portfolio_holdings', 'cash_ledger', 'portfolio_cash', 'users']:
    try:
        cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table}';")
        print(f'{table} cols:', [r[0] for r in cur.fetchall()])
    except Exception as e:
        print(e)
