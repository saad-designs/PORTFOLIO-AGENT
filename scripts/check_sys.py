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

print("--- MESSAGES ---")
cur.execute("SELECT id, sender, body, timestamp, chat_id FROM messages")
for row in cur.fetchall():
    print(row)

print("\n--- PENDING SIGNALS with SYS or 520 ---")
cur.execute("SELECT id, source, signal_payload FROM pending_signals WHERE signal_payload::text ILIKE '%520%' OR signal_payload::text ILIKE '%SYS%'")
for row in cur.fetchall():
    print(row)

print("\n--- INSTITUTIONAL REPORTS with SYS or 520 ---")
cur.execute("SELECT id, source, ticker, subject, summary FROM institutional_reports WHERE ticker ILIKE '%SYS%' OR subject ILIKE '%520%' OR summary ILIKE '%520%'")
for row in cur.fetchall():
    print(row)

print("\n--- ACTIVE PLAYBOOK with SYS or 520 ---")
cur.execute("SELECT id, source, rule FROM active_playbook WHERE rule ILIKE '%520%' OR rule ILIKE '%SYS%'")
for row in cur.fetchall():
    print(row)

print("\n--- LANGCHAIN_PG_EMBEDDING with 520 or SYS ---")
cur.execute("SELECT id, document FROM langchain_pg_embedding WHERE document ILIKE '%520%' OR document ILIKE '%SYS buy target 520%'")
for row in cur.fetchall():
    print(row)

cur.close()
conn.close()
