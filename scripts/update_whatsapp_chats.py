import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("SUPABASE_DB_URL")

def update_monitored_chats(actual_chats):
    if not DATABASE_URL:
        print("❌ SUPABASE_DB_URL is not set.")
        return

    print("🔄 Connecting to Supabase to update whatsapp_monitored_chats...")
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    try:
        # Delete the dummy records
        print("🗑️ Deleting dummy records...")
        cur.execute("""
            DELETE FROM whatsapp_monitored_chats 
            WHERE chat_id IN ('92300XXXXXXX@c.us', '1203630XXXXXXXX@g.us');
        """)
        
        # Insert the actual records
        if actual_chats:
            print(f"📥 Inserting new records: {actual_chats}")
            insert_query = """
                INSERT INTO whatsapp_monitored_chats (chat_id, mentor_name)
                VALUES (%s, %s)
                ON CONFLICT (chat_id) DO NOTHING;
            """
            cur.executemany(insert_query, actual_chats)
        
        conn.commit()
        print("✅ Database successfully updated.")
    except Exception as e:
        conn.rollback()
        print(f"❌ Database update failed: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    # TODO: Add the actual IDs and names here!
    # Example format: 
    # actual_chats = [
    #     ('1234567890@c.us', 'Mentor John'),
    #     ('9876543210-123456@g.us', 'Alpha Group')
    # ]
    
    actual_chats = [] 
    
    if actual_chats:
        update_monitored_chats(actual_chats)
    else:
        print("⚠️ No actual chats provided. Please update the 'actual_chats' list with the real IDs and run again.")
