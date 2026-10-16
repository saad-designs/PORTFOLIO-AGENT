import os
import sys
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
import psycopg2
from dotenv import load_dotenv

load_dotenv()
conn = psycopg2.connect(os.getenv('SUPABASE_DB_URL'))
cur = conn.cursor()

print("=== Database State Verification ===")

# 1. active_playbook
cur.execute("SELECT count(*) FROM active_playbook WHERE rule ILIKE '%520%' OR rule ILIKE '%SYS_technical%' OR rule ILIKE '%HUBC_Initiation%' OR rule ILIKE '%TEST KTrade%';")
print(f"active_playbook test rows remaining: {cur.fetchone()[0]}")

# 2. institutional_reports
cur.execute("SELECT count(*) FROM institutional_reports WHERE subject ILIKE '%520%' OR subject ILIKE '%SYS_technical%' OR subject ILIKE '%HUBC_Initiation%' OR subject ILIKE '%TEST KTrade%';")
print(f"institutional_reports test rows remaining: {cur.fetchone()[0]}")

# 3. langchain_pg_embedding
cur.execute("SELECT count(*) FROM langchain_pg_embedding WHERE document ILIKE '%TEST KTrade Multimodal Intel%' OR document ILIKE '%SYS_technical_call%' OR document ILIKE '%HUBC_Initiation%';")
print(f"langchain_pg_embedding test rows remaining: {cur.fetchone()[0]}")

# 4. pending_signals
cur.execute("SELECT count(*) FROM pending_signals WHERE signal_payload::text ILIKE '%520%' OR signal_payload::text ILIKE '%SYS_technical%';")
print(f"pending_signals test rows remaining: {cur.fetchone()[0]}")

# 5. Checkpoint
cur.execute("SELECT key, value, updated_at FROM agent_checkpoints WHERE key = 'whatsapp_last_timestamp';")
r = cur.fetchone()
print(f"agent_checkpoints: key={r[0]}, value={r[1]}, updated_at={r[2]}")

cur.close()
conn.close()
