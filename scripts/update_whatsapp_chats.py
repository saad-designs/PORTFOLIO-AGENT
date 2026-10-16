import os
import sys
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
import psycopg2
from dotenv import load_dotenv

env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(dotenv_path=env_path)
DATABASE_URL = os.getenv("SUPABASE_DB_URL")

def update_monitored_chats(actual_chats):
    if not DATABASE_URL:
        print("❌ SUPABASE_DB_URL is not set.")
        return

    print("🔄 Connecting to Supabase to update whatsapp_monitored_chats...")
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    try:
        # Delete dummy placeholder records or non-KTrade records
        print("🗑️ Deleting placeholder/dummy records...")
        cur.execute("""
            DELETE FROM whatsapp_monitored_chats 
            WHERE chat_id LIKE '%%XXXX%%' 
               OR chat_id NOT IN (%s, %s);
        """, (actual_chats[0][0], actual_chats[1][0]))
        
        # Insert the actual KTrade records
        print(f"📥 Upserting KTrade records: {actual_chats}")
        insert_query = """
            INSERT INTO whatsapp_monitored_chats (chat_id, mentor_name, is_active)
            VALUES (%s, %s, TRUE)
            ON CONFLICT (chat_id) DO UPDATE 
            SET mentor_name = EXCLUDED.mentor_name, is_active = TRUE;
        """
        cur.executemany(insert_query, actual_chats)
        
        conn.commit()
        print("✅ Database successfully updated.")

        # Query and print current contents
        print("\n📋 Current whatsapp_monitored_chats table:")
        cur.execute("SELECT chat_id, mentor_name, is_active, added_at FROM whatsapp_monitored_chats ORDER BY added_at ASC;")
        rows = cur.fetchall()
        for r in rows:
            print(f"  • ID: {r[0]} | Name: {r[1]} | Active: {r[2]} | Added: {r[3]}")

    except Exception as e:
        conn.rollback()
        print(f"❌ Database update failed: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    actual_chats = [
        ('120363405626670375@g.us', 'KTrade - E Trader Assistance 6'),
        ('120363404462896702@g.us', 'KTrade - E Trader Assistance 6 (Secondary)')
    ]
    
    update_monitored_chats(actual_chats)

