import os
import sys

# Force UTF-8 on Windows
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from portfolio_manager import get_db_connection

def run_migration():
    print("🚀 [Migration] Creating agent_feed and chat_messages tables in Supabase...")
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

    # 3. RLS Permissive Policies
    for tbl in ["agent_feed", "chat_messages"]:
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
    print("✅ [Migration] agent_feed and chat_messages successfully created and secured with RLS!")
    cur.close()
    conn.close()

if __name__ == "__main__":
    run_migration()
