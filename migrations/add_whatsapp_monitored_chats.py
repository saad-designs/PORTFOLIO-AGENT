import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("SUPABASE_DB_URL")

def run_migration():
    if not DATABASE_URL:
        print("❌ SUPABASE_DB_URL is not set.")
        return

    print("🔄 Connecting to Supabase to create whatsapp_monitored_chats...")
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS whatsapp_monitored_chats (
                chat_id TEXT PRIMARY KEY,
                mentor_name TEXT,
                is_active BOOLEAN DEFAULT TRUE,
                added_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        
        # Insert placeholders - the user will update these in Supabase directly
        cur.execute("""
            INSERT INTO whatsapp_monitored_chats (chat_id, mentor_name)
            VALUES ('92300XXXXXXX@c.us', 'Dummy Mentor 1'),
                   ('1203630XXXXXXXX@g.us', 'Dummy Group 1')
            ON CONFLICT (chat_id) DO NOTHING;
        """)
        
        conn.commit()
        print("✅ Migration successful: whatsapp_monitored_chats created and initialized.")
    except Exception as e:
        conn.rollback()
        print(f"❌ Migration failed: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    run_migration()
