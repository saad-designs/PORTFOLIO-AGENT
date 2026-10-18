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

# Get all tables in public schema
cur.execute("""
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
""")
tables = [r[0] for r in cur.fetchall()]

print(f"Checking tables: {tables}")
for table in tables:
    try:
        # Find text/character columns
        cur.execute(f"""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = 'public' AND table_name = '{table}';
        """)
        cols = cur.fetchall()
        conditions = []
        for c, t in cols:
            if 'char' in t or 'text' in t or 'json' in t:
                conditions.append(f"\"{c}\"::text ILIKE '%520%'")
        if conditions:
            query = f"SELECT count(*) FROM \"{table}\" WHERE {' OR '.join(conditions)}"
            cur.execute(query)
            cnt = cur.fetchone()[0]
            if cnt > 0:
                print(f"⚠️ Found {cnt} rows matching '520' in table '{table}'!")
                # show them
                show_query = f"SELECT * FROM \"{table}\" WHERE {' OR '.join(conditions)} LIMIT 5"
                cur.execute(show_query)
                for row in cur.fetchall():
                    print(f"   Row: {row}")
            else:
                print(f"✓ Table '{table}': clean (0 matches for 520)")
    except Exception as e:
        print(f"Error checking table {table}: {e}")
        conn.rollback()

cur.close()
conn.close()
