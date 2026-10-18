import os
import sys
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dotenv import load_dotenv
from portfolio_manager import get_db_connection, supabase

load_dotenv()

conn = get_db_connection()
cur = conn.cursor()

print("🔍 Inspecting junk rows in Supabase database...")

# 1. messages via Supabase client if table exists
try:
    if supabase:
        print("Checking messages table via Supabase client...")
        for junk_id in ['120363428147373878@g.us', '923246764769@c.us', '120363041996231812@g.us']:
            res = supabase.table("messages").delete().eq("chat_id", junk_id).execute()
            print(f"Purged {junk_id} from Supabase messages: {len(res.data) if res and res.data else 0}")
except Exception as e:
    print(f"Supabase messages table check: {e}")

# 2. institutional_reports
cur.execute("""
    SELECT count(*), array_agg(DISTINCT source)
    FROM institutional_reports
    WHERE source ILIKE '%120363428147373878%'
       OR source ILIKE '%923246764769%'
       OR source ILIKE '%120363041996231812%'
       OR source ILIKE '%@lid%'
       OR source ILIKE '%@newsletter%'
       OR source ILIKE '%@c.us%';
""")
res_inst = cur.fetchone()
print(f"Institutional reports matching junk: {res_inst[0]} (sources: {res_inst[1]})")

# 3. active_playbook
cur.execute("""
    SELECT count(*), array_agg(DISTINCT source)
    FROM active_playbook
    WHERE source ILIKE '%120363428147373878%'
       OR source ILIKE '%923246764769%'
       OR source ILIKE '%120363041996231812%'
       OR source ILIKE '%@lid%'
       OR source ILIKE '%@newsletter%'
       OR source ILIKE '%@c.us%';
""")
res_play = cur.fetchone()
print(f"Active playbook rules matching junk: {res_play[0]} (sources: {res_play[1]})")

# 4. langchain_pg_embedding
cur.execute("""
    SELECT count(*)
    FROM langchain_pg_embedding
    WHERE document ILIKE '%120363428147373878%'
       OR document ILIKE '%923246764769%'
       OR document ILIKE '%120363041996231812%'
       OR document ILIKE '%@lid%'
       OR document ILIKE '%@newsletter%'
       OR document ILIKE '%@c.us%';
""")
res_emb = cur.fetchone()
print(f"Embeddings matching junk: {res_emb[0]}")

# 5. pending_signals
cur.execute("""
    SELECT count(*)
    FROM pending_signals
    WHERE signal_payload::text ILIKE '%120363428147373878%'
       OR signal_payload::text ILIKE '%923246764769%'
       OR signal_payload::text ILIKE '%120363041996231812%'
       OR signal_payload::text ILIKE '%@lid%'
       OR signal_payload::text ILIKE '%@newsletter%'
       OR signal_payload::text ILIKE '%@c.us%';
""")
res_sig = cur.fetchone()
print(f"Pending signals matching junk: {res_sig[0]}")

print("\n🗑️ Executing PURGE...")

cur.execute("""
    DELETE FROM institutional_reports
    WHERE source ILIKE '%120363428147373878%'
       OR source ILIKE '%923246764769%'
       OR source ILIKE '%120363041996231812%'
       OR source ILIKE '%@lid%'
       OR source ILIKE '%@newsletter%'
       OR source ILIKE '%@c.us%';
""")
del_inst = cur.rowcount

cur.execute("""
    DELETE FROM active_playbook
    WHERE source ILIKE '%120363428147373878%'
       OR source ILIKE '%923246764769%'
       OR source ILIKE '%120363041996231812%'
       OR source ILIKE '%@lid%'
       OR source ILIKE '%@newsletter%'
       OR source ILIKE '%@c.us%';
""")
del_play = cur.rowcount

cur.execute("""
    DELETE FROM langchain_pg_embedding
    WHERE document ILIKE '%120363428147373878%'
       OR document ILIKE '%923246764769%'
       OR document ILIKE '%120363041996231812%'
       OR document ILIKE '%@lid%'
       OR document ILIKE '%@newsletter%'
       OR document ILIKE '%@c.us%';
""")
del_emb = cur.rowcount

cur.execute("""
    DELETE FROM pending_signals
    WHERE signal_payload::text ILIKE '%120363428147373878%'
       OR signal_payload::text ILIKE '%923246764769%'
       OR signal_payload::text ILIKE '%120363041996231812%'
       OR signal_payload::text ILIKE '%@lid%'
       OR signal_payload::text ILIKE '%@newsletter%'
       OR signal_payload::text ILIKE '%@c.us%';
""")
del_sig = cur.rowcount

conn.commit()
cur.close()
conn.close()

print(f"\n✅ PURGE COMPLETE:\n- Institutional Reports: {del_inst}\n- Active Playbook: {del_play}\n- Embeddings: {del_emb}\n- Pending Signals: {del_sig}")
