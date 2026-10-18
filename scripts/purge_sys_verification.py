import os
import sys
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from dotenv import load_dotenv
load_dotenv()
from portfolio_manager import get_db_connection

conn = get_db_connection()
cur = conn.cursor()

print("🧹 Executing targeted purge of synthetic SYS 520 test payloads...")

cur.execute("""
    DELETE FROM messages
    WHERE body ILIKE '%SYS buy target 520 stop 480%'
       OR (body ILIKE '%SYS%' AND body ILIKE '%520%');
""")
del_msgs = cur.rowcount

cur.execute("""
    DELETE FROM active_playbook
    WHERE rule ILIKE '%SYS%520%'
       OR rule ILIKE '%520%SYS%'
       OR rule ILIKE '%SYS buy target 520 stop 480%';
""")
del_play = cur.rowcount

cur.execute("""
    DELETE FROM institutional_reports
    WHERE (ticker ILIKE '%SYS%' AND (subject ILIKE '%520%' OR summary ILIKE '%520%'))
       OR subject ILIKE '%SYS buy target 520 stop 480%'
       OR summary ILIKE '%SYS buy target 520 stop 480%';
""")
del_inst = cur.rowcount

cur.execute("""
    DELETE FROM pending_signals
    WHERE signal_payload::text ILIKE '%SYS buy target 520 stop 480%'
       OR (signal_payload::text ILIKE '%SYS%' AND signal_payload::text ILIKE '%520%');
""")
del_sig = cur.rowcount

cur.execute("""
    DELETE FROM langchain_pg_embedding
    WHERE document ILIKE '%SYS buy target 520 stop 480%'
       OR (document ILIKE '%SYS%' AND document ILIKE '%520%');
""")
del_emb = cur.rowcount

conn.commit()
cur.close()
conn.close()

print(f"✅ PURGE SUMMARY:")
print(f"  - messages deleted: {del_msgs}")
print(f"  - active_playbook deleted: {del_play}")
print(f"  - institutional_reports deleted: {del_inst}")
print(f"  - pending_signals deleted: {del_sig}")
print(f"  - langchain_pg_embedding deleted: {del_emb}")
