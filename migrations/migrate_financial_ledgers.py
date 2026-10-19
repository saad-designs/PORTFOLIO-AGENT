import os
import sys
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from portfolio_manager import get_db_connection

def run_migration():
    print("🚀 [Migration] Creating/updating agent_feed, chat_messages, capital_ledger, dividend_ledger, and trade_history in Supabase...")
    conn = get_db_connection()
    cur = conn.cursor()

    # 1. agent_feed
    cur.execute("""
        CREATE TABLE IF NOT EXISTS agent_feed (
            id BIGSERIAL PRIMARY KEY,
            category TEXT NOT NULL,
            title TEXT NOT NULL,
            content TEXT NOT NULL,
            metadata JSONB DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_agent_feed_category ON agent_feed (category);
        CREATE INDEX IF NOT EXISTS idx_agent_feed_created ON agent_feed (created_at DESC);
        ALTER TABLE agent_feed ENABLE ROW LEVEL SECURITY;
    """)

    # 2. chat_messages
    cur.execute("""
        CREATE TABLE IF NOT EXISTS chat_messages (
            id BIGSERIAL PRIMARY KEY,
            role TEXT NOT NULL,
            content TEXT NOT NULL,
            actions_taken JSONB DEFAULT '[]'::jsonb,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_chat_messages_created ON chat_messages (created_at ASC);
        ALTER TABLE chat_messages ENABLE ROW LEVEL SECURITY;
    """)

    # 3. capital_ledger
    cur.execute("""
        CREATE TABLE IF NOT EXISTS capital_ledger (
            id BIGSERIAL PRIMARY KEY,
            tx_type TEXT NOT NULL,
            amount NUMERIC(14,2) NOT NULL,
            notes TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        ALTER TABLE capital_ledger ADD COLUMN IF NOT EXISTS notes TEXT;
        ALTER TABLE capital_ledger ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW();
        ALTER TABLE capital_ledger ADD COLUMN IF NOT EXISTS reference_note TEXT;
        ALTER TABLE capital_ledger ADD COLUMN IF NOT EXISTS tx_date TIMESTAMPTZ DEFAULT NOW();
        UPDATE capital_ledger SET notes = reference_note WHERE notes IS NULL AND reference_note IS NOT NULL;
        UPDATE capital_ledger SET created_at = tx_date WHERE created_at IS NULL AND tx_date IS NOT NULL;
        UPDATE capital_ledger SET reference_note = notes WHERE reference_note IS NULL AND notes IS NOT NULL;
        UPDATE capital_ledger SET tx_date = created_at WHERE tx_date IS NULL AND created_at IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_capital_ledger_created ON capital_ledger (created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_capital_ledger_tx_type ON capital_ledger (tx_type);
        ALTER TABLE capital_ledger ENABLE ROW LEVEL SECURITY;
    """)

    # 4. dividend_ledger
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dividend_ledger (
            id BIGSERIAL PRIMARY KEY,
            ticker TEXT NOT NULL,
            shares_held NUMERIC(14,2) NOT NULL,
            dps_gross NUMERIC(10,4) DEFAULT 0,
            gross_amount NUMERIC(14,2) NOT NULL,
            tax_rate_pct NUMERIC(5,2) DEFAULT 15.00,
            tax_withheld NUMERIC(14,2) NOT NULL,
            net_amount NUMERIC(14,2) NOT NULL,
            status TEXT NOT NULL DEFAULT 'CREDITED_TO_BANK',
            notes TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        ALTER TABLE dividend_ledger ADD COLUMN IF NOT EXISTS notes TEXT;
        ALTER TABLE dividend_ledger ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW();
        ALTER TABLE dividend_ledger ALTER COLUMN dps_gross SET DEFAULT 0;
        ALTER TABLE dividend_ledger ALTER COLUMN status SET DEFAULT 'CREDITED_TO_BANK';
        CREATE INDEX IF NOT EXISTS idx_dividend_ledger_ticker ON dividend_ledger (ticker);
        CREATE INDEX IF NOT EXISTS idx_dividend_ledger_status ON dividend_ledger (status);
        CREATE INDEX IF NOT EXISTS idx_dividend_ledger_created ON dividend_ledger (created_at DESC);
        ALTER TABLE dividend_ledger ENABLE ROW LEVEL SECURITY;
    """)

    # 5. trade_history
    cur.execute("""
        CREATE TABLE IF NOT EXISTS trade_history (
            id BIGSERIAL PRIMARY KEY,
            ticker TEXT NOT NULL,
            side TEXT NOT NULL,
            quantity NUMERIC(14,2) NOT NULL,
            price NUMERIC(12,4) NOT NULL,
            fees NUMERIC(12,2) DEFAULT 0.00,
            net_total NUMERIC(14,2) NOT NULL,
            notes TEXT,
            executed_at TIMESTAMPTZ DEFAULT NOW()
        );
        ALTER TABLE trade_history ADD COLUMN IF NOT EXISTS fees NUMERIC(12,2) DEFAULT 0.00;
        ALTER TABLE trade_history ADD COLUMN IF NOT EXISTS fees_and_tax NUMERIC(12,2) DEFAULT 0.00;
        ALTER TABLE trade_history ADD COLUMN IF NOT EXISTS notes TEXT;
        UPDATE trade_history SET fees = fees_and_tax WHERE fees IS NULL OR fees = 0.00;
        UPDATE trade_history SET fees_and_tax = fees WHERE fees_and_tax IS NULL OR fees_and_tax = 0.00;
        CREATE INDEX IF NOT EXISTS idx_trade_history_ticker ON trade_history (ticker);
        CREATE INDEX IF NOT EXISTS idx_trade_history_executed ON trade_history (executed_at DESC);
        ALTER TABLE trade_history ENABLE ROW LEVEL SECURITY;
    """)

    # 6. RLS Permissive Policies
    tables = ["agent_feed", "chat_messages", "capital_ledger", "dividend_ledger", "trade_history"]
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
    print("✅ [Migration] All 5 tables created/migrated and secured with RLS successfully.")

    # 7. Check baseline capital
    cur.execute("SELECT COUNT(*) FROM capital_ledger;")
    cap_count = cur.fetchone()[0]
    if cap_count == 0:
        print("🌱 [Migration] Seeding initial baseline capital entry...")
        cur.execute("""
            INSERT INTO capital_ledger (tx_type, amount, notes, reference_note, created_at, tx_date)
            VALUES ('DEPOSIT_TO_KTRADE', 500000.00, 'Initial Portfolio Seed Capital', 'Initial Portfolio Seed Capital', NOW(), NOW())
        """)
        conn.commit()
        print("✅ [Migration] Seeded baseline initial capital of Rs. 500,000.")

    cur.close()
    conn.close()

if __name__ == "__main__":
    run_migration()
