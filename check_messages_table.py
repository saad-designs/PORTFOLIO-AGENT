from portfolio_manager import get_db_connection
try:
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'messages');")
    exists = cur.fetchone()[0]
    print(f"Table exists: {exists}")
    if exists:
        cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'messages';")
        print(cur.fetchall())
except Exception as e:
    print(f"Error: {e}")
