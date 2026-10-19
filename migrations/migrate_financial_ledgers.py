import os
import sys
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from portfolio_manager import get_db_connection

def run_migration():
    print("🚀 [Migration] Creating capital_ledger, dividend_ledger, and trade_history tables in Supabase...")
    conn = get_db_connection()
    cur = conn.cursor()

    # 1. capital_ledger
    cur.execute("""
        CREATE TABLE IF NOT EXISTS capital_ledger (
            id BIGSERIAL PRIMARY KEY,
            tx_type TEXT NOT NULL,
            amount NUMERIC(14,2) NOT NULL,
            reference_note TEXT,
            tx_date TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_capital_ledger_tx_date ON capital_ledger (tx_date DESC);
        CREATE INDEX IF NOT EXISTS idx_capital_ledger_tx_type ON capital_ledger (tx_type);
        ALTER TABLE capital_ledger ENABLE ROW LEVEL SECURITY;
    """)

    # 2. dividend_ledger
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dividend_ledger (
            id BIGSERIAL PRIMARY KEY,
            ticker TEXT NOT NULL,
            shares_held NUMERIC(14,2) NOT NULL,
            dps_gross NUMERIC(10,4) NOT NULL,
            gross_amount NUMERIC(14,2) NOT NULL,
            tax_rate_pct NUMERIC(5,2) DEFAULT 15.00,
            tax_withheld NUMERIC(14,2) NOT NULL,
            net_amount NUMERIC(14,2) NOT NULL,
            ex_date DATE,
            payment_date DATE,
            status TEXT NOT NULL DEFAULT 'ENTITLED',
            notes TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_dividend_ledger_ticker ON dividend_ledger (ticker);
        CREATE INDEX IF NOT EXISTS idx_dividend_ledger_status ON dividend_ledger (status);
        CREATE INDEX IF NOT EXISTS idx_dividend_ledger_created ON dividend_ledger (created_at DESC);
        ALTER TABLE dividend_ledger ENABLE ROW LEVEL SECURITY;
    """)

    # 3. trade_history
    cur.execute("""
        CREATE TABLE IF NOT EXISTS trade_history (
            id BIGSERIAL PRIMARY KEY,
            ticker TEXT NOT NULL,
            side TEXT NOT NULL,
            quantity NUMERIC(14,2) NOT NULL,
            price NUMERIC(12,4) NOT NULL,
            fees_and_tax NUMERIC(12,2) DEFAULT 0.00,
            net_total NUMERIC(14,2) NOT NULL,
            executed_at TIMESTAMPTZ DEFAULT NOW(),
            notes TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_trade_history_ticker ON trade_history (ticker);
        CREATE INDEX IF NOT EXISTS idx_trade_history_executed ON trade_history (executed_at DESC);
        ALTER TABLE trade_history ENABLE ROW LEVEL SECURITY;
    """)

    # 4. RLS Permissive Policies for Backend Connection
    tables = ["capital_ledger", "dividend_ledger", "trade_history"]
    for tbl in tables:
        cur.execute(f"""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_policies WHERE tablename = '{tbl}' AND policyname = 'Allow service access on {tbl}'
                ) THEN
                    CREATE POLICY "Allow service access on {tbl}" ON {tbl} FOR ALL USING (true);
                END IF;
            END $$;
        """)

    conn.commit()
    print("✅ [Migration] Tables created and RLS configured successfully.")

    # 5. Seed initial capital if capital_ledger is empty
    cur.execute("SELECT COUNT(*) FROM capital_ledger;")
    cap_count = cur.fetchone()[0]
    if cap_count == 0:
        print("🌱 [Migration] Seeding initial baseline capital entry...")
        cur.execute("""
            INSERT INTO capital_ledger (tx_type, amount, reference_note, tx_date)
            VALUES ('DEPOSIT_TO_BROKER', 500000.00, 'Initial Portfolio Seed Capital', NOW())
        """)
        conn.commit()
        print("✅ [Migration] Seeded baseline initial capital of Rs. 500,000.")

    cur.close()
    conn.close()

if __name__ == "__main__":
    run_migration()
