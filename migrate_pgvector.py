import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(os.getenv("SUPABASE_DB_URL"))
conn.autocommit = True
cur = conn.cursor()

sql = """
-- 1. Enable the pgvector extension
CREATE EXTENSION IF NOT EXISTS vector;

-- 2. Add the embedding column to your existing table
ALTER TABLE active_playbook 
ADD COLUMN IF NOT EXISTS embedding vector(768);

-- 3. Create the cosine similarity search function
CREATE OR REPLACE FUNCTION match_playbook_rules (
  query_embedding vector(768),
  match_threshold float,
  match_count int
)
RETURNS TABLE (
  source text,
  rule text,
  similarity float
)
LANGUAGE plpgsql STABLE
AS $$
BEGIN
  RETURN QUERY
  SELECT
    active_playbook.source,
    active_playbook.rule,
    1 - (active_playbook.embedding <=> query_embedding) AS similarity
  FROM active_playbook
  -- Only return rules that meet the minimum relevance threshold
  WHERE 1 - (active_playbook.embedding <=> query_embedding) > match_threshold
  -- Order by closest cosine distance
  ORDER BY active_playbook.embedding <=> query_embedding
  LIMIT match_count;
END;
$$;
"""

try:
    cur.execute(sql)
    print("Migration successful.")
except Exception as e:
    print(f"Migration failed: {e}")
finally:
    cur.close()
    conn.close()
