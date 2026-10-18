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

print("🔍 Inspecting junk & synthetic rows in database...")

# 1. messages
cur.execute("""
    SELECT count(*)
    FROM messages
    WHERE body ILIKE '%520%'
       OR body ILIKE '%SYS buy target 520 stop 480%'
       OR chat_id IN ('120363428147373878@g.us', '923246764769@c.us', '120363041996231812@g.us')
       OR chat_id LIKE '%@lid'
       OR chat_id LIKE '%@newsletter'
       OR chat_id LIKE '%@c.us';
""")
cnt_msgs = cur.fetchone()[0]
print(f"Messages to delete: {cnt_msgs}")

# 2. institutional_reports
cur.execute("""
    SELECT count(*)
    FROM institutional_reports
    WHERE subject ILIKE '%520%'
       OR summary ILIKE '%520%'
       OR source ILIKE '%120363428147373878%'
       OR source ILIKE '%923246764769%'
       OR source ILIKE '%120363041996231812%'
       OR source ILIKE '%@lid%'
       OR source ILIKE '%@newsletter%'
       OR source ILIKE '%@c.us%';
""")
cnt_inst = cur.fetchone()[0]
print(f"Institutional reports to delete: {cnt_inst}")

# 3. active_playbook
cur.execute("""
    SELECT count(*)
    FROM active_playbook
    WHERE rule ILIKE '%520%'
       OR source ILIKE '%TestDesk%'
       OR source ILIKE '%120363428147373878%'
       OR source ILIKE '%923246764769%'
       OR source ILIKE '%120363041996231812%'
       OR source ILIKE '%@lid%'
       OR source ILIKE '%@newsletter%'
       OR source ILIKE '%@c.us%';
""")
cnt_play = cur.fetchone()[0]
print(f"Active playbook rules to delete: {cnt_play}")

# 4. langchain_pg_embedding
cur.execute("""
    SELECT count(*)
    FROM langchain_pg_embedding
    WHERE document ILIKE '%520%'
       OR document ILIKE '%TestDesk%'
       OR document ILIKE '%SYS buy target 520 stop 480%'
       OR document ILIKE '%120363428147373878%'
       OR document ILIKE '%923246764769%'
       OR document ILIKE '%120363041996231812%'
       OR document ILIKE '%@lid%'
       OR document ILIKE '%@newsletter%'
       OR document ILIKE '%@c.us%';
""")
cnt_emb = cur.fetchone()[0]
print(f"Embeddings to delete: {cnt_emb}")

# 5. pending_signals
cur.execute("""
    SELECT count(*)
    FROM pending_signals
    WHERE signal_payload::text ILIKE '%520%'
       OR signal_payload::text ILIKE '%SYS buy target 520 stop 480%'
       OR signal_payload::text ILIKE '%120363428147373878%'
       OR signal_payload::text ILIKE '%923246764769%'
       OR signal_payload::text ILIKE '%120363041996231812%'
       OR signal_payload::text ILIKE '%@lid%'
       OR signal_payload::text ILIKE '%@newsletter%'
       OR signal_payload::text ILIKE '%@c.us%';
""")
cnt_sig = cur.fetchone()[0]
print(f"Pending signals to delete: {cnt_sig}")

print("\n🗑️ Executing PURGE...")

cur.execute("""
    DELETE FROM messages
    WHERE body ILIKE '%520%'
       OR body ILIKE '%SYS buy target 520 stop 480%'
       OR chat_id IN ('120363428147373878@g.us', '923246764769@c.us', '120363041996231812@g.us')
       OR chat_id LIKE '%@lid'
       OR chat_id LIKE '%@newsletter'
       OR chat_id LIKE '%@c.us';
""")
del_msgs = cur.rowcount

cur.execute("""
    DELETE FROM institutional_reports
    WHERE subject ILIKE '%520%'
       OR summary ILIKE '%520%'
       OR source ILIKE '%120363428147373878%'
       OR source ILIKE '%923246764769%'
       OR source ILIKE '%120363041996231812%'
       OR source ILIKE '%@lid%'
       OR source ILIKE '%@newsletter%'
       OR source ILIKE '%@c.us%';
""")
del_inst = cur.rowcount

cur.execute("""
    DELETE FROM active_playbook
    WHERE rule ILIKE '%520%'
       OR source ILIKE '%TestDesk%'
       OR source ILIKE '%120363428147373878%'
       OR source ILIKE '%923246764769%'
       OR source ILIKE '%120363041996231812%'
       OR source ILIKE '%@lid%'
       OR source ILIKE '%@newsletter%'
       OR source ILIKE '%@c.us%';
""")
del_play = cur.rowcount

cur.execute("""
    DELETE FROM langchain_pg_embedding
    WHERE document ILIKE '%520%'
       OR document ILIKE '%TestDesk%'
       OR document ILIKE '%SYS buy target 520 stop 480%'
       OR document ILIKE '%120363428147373878%'
       OR document ILIKE '%923246764769%'
       OR document ILIKE '%120363041996231812%'
       OR document ILIKE '%@lid%'
       OR document ILIKE '%@newsletter%'
       OR document ILIKE '%@c.us%';
""")
del_emb = cur.rowcount

cur.execute("""
    DELETE FROM pending_signals
    WHERE signal_payload::text ILIKE '%520%'
       OR signal_payload::text ILIKE '%SYS buy target 520 stop 480%'
       OR signal_payload::text ILIKE '%120363428147373878%'
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

print(f"\n✅ PURGE COMPLETE:\n- Messages: {del_msgs}\n- Institutional Reports: {del_inst}\n- Active Playbook: {del_play}\n- Embeddings: {del_emb}\n- Pending Signals: {del_sig}")
