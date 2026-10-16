import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("SUPABASE_DB_URL")

def run_migration():
    if not DATABASE_URL:
        print("❌ SUPABASE_DB_URL is not set.")
        return

    print("🔄 Connecting to Supabase to create agent_checkpoints...")
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS agent_checkpoints (
                key TEXT PRIMARY KEY,
                value JSONB NOT NULL,
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        
        cur.execute("""
            INSERT INTO agent_checkpoints (key, value)
            VALUES ('whatsapp_last_timestamp', '0'::jsonb)
            ON CONFLICT (key) DO NOTHING;
        """)
        
        conn.commit()
        print("✅ Migration successful: agent_checkpoints created and initialized.")
    except Exception as e:
        conn.rollback()
        print(f"❌ Migration failed: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    run_migration()
