import os
import sys
import time
import json
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
import psycopg2
import urllib.request
from dotenv import load_dotenv

load_dotenv()
db_url = os.getenv("SUPABASE_DB_URL")

def run_cleanup():
    if not db_url:
        print("❌ SUPABASE_DB_URL is not set.")
        return

    print("🔄 Connecting to Supabase to clean up synthetic test rows...")
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    try:
        # 1. Clean active_playbook
        cur.execute("""
            DELETE FROM active_playbook 
            WHERE id IN (27, 28)
               OR rule ILIKE '%TEST KTrade%' 
               OR rule ILIKE '%SYS_technical_call%' 
               OR rule ILIKE '%HUBC_Initiation%' 
               OR rule ILIKE '%1:2.5%'
               OR rule ILIKE '%power and energy infrastructure%'
               OR (source ILIKE '%WhatsApp%' AND created_at > NOW() - INTERVAL '1 hour');
        """)
        del_playbook = cur.rowcount
        print(f"🗑️ Deleted {del_playbook} synthetic row(s) from active_playbook.")

        # 2. Clean institutional_reports
        cur.execute("""
            DELETE FROM institutional_reports 
            WHERE id IN (16, 17)
               OR subject ILIKE '%SYS_technical_call%'
               OR subject ILIKE '%HUBC_Initiation%'
               OR subject ILIKE '%TEST KTrade%'
               OR summary ILIKE '%520%'
               OR (source ILIKE '%WhatsApp%' AND created_at > NOW() - INTERVAL '1 hour');
        """)
        del_inst = cur.rowcount
        print(f"🗑️ Deleted {del_inst} synthetic row(s) from institutional_reports.")

        # 3. Clean pending_signals
        cur.execute("""
            DELETE FROM pending_signals 
            WHERE signal_payload::text ILIKE '%520%' 
               OR signal_payload::text ILIKE '%SYS_technical%' 
               OR signal_payload::text ILIKE '%HUBC_Initiation%'
               OR (source ILIKE '%WhatsApp%' AND created_at > NOW() - INTERVAL '1 hour');
        """)
        del_signals = cur.rowcount
        print(f"🗑️ Deleted {del_signals} synthetic row(s) from pending_signals.")

        # 4. Clean langchain_pg_embedding
        cur.execute("""
            DELETE FROM langchain_pg_embedding 
            WHERE document ILIKE '%TEST KTrade Multimodal Intel%'
               OR document ILIKE '%SYS_technical_call%'
               OR document ILIKE '%SYS_breakout_char%'
               OR document ILIKE '%HUBC_Initiation%'
               OR document ILIKE '%520.00%';
        """)
        del_embeddings = cur.rowcount
        print(f"🗑️ Deleted {del_embeddings} synthetic row(s) from langchain_pg_embedding.")

        # 5. Reset watermark checkpoint back 24 hours
        now_ts = int(time.time())
        reset_ts = now_ts - 86400  # 24 hours ago
        print(f"\n⏱️ Current epoch time: {now_ts} | Resetting watermark to: {reset_ts} (24h prior)")

        cur.execute("""
            UPDATE agent_checkpoints 
            SET value = %s, updated_at = NOW() 
            WHERE key = 'whatsapp_last_timestamp';
        """, (str(reset_ts),))
        print(f"✅ Reset agent_checkpoints 'whatsapp_last_timestamp' to {reset_ts}.")

        # Check if sync_checkpoints table exists, if so update it too
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'sync_checkpoints';")
        if cur.fetchone():
            cur.execute("""
                UPDATE sync_checkpoints 
                SET watermark = %s 
                WHERE key = 'whatsapp' OR key = 'ktrade';
            """, (reset_ts,))
            print("✅ Updated sync_checkpoints table.")

        conn.commit()
        print("✅ Database cleanup successfully committed.")

    except Exception as e:
        conn.rollback()
        print(f"❌ Database cleanup failed: {e}")
        return
    finally:
        cur.close()
        conn.close()

    # 6. Trigger fresh catch-up sweep against WhatsApp Bridge
    print("\n🔄 Triggering live catch-up sweep on WhatsApp Bridge (http://127.0.0.1:3000/api/catchup)...")
    target_chats = [
        "120363405626670375@g.us",
        "120363404462896702@g.us"
    ]
    catchup_payload = json.dumps({
        "since_timestamp": reset_ts,
        "target_chat_ids": target_chats
    }).encode("utf-8")

    req = urllib.request.Request(
        "http://127.0.0.1:3000/api/catchup",
        data=catchup_payload,
        headers={"Content-Type": "application/json"}
    )

    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            resp_body = resp.read().decode("utf-8")
            print(f"✅ WhatsApp Catch-Up Response: {resp_body}")
    except Exception as sweep_err:
        print(f"⚠️ Catch-up request error: {sweep_err}")

if __name__ == "__main__":
    run_cleanup()
