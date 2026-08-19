import os
import psycopg2
from dotenv import load_dotenv

# Load the environment variables from your .env file
load_dotenv()

DATABASE_URL = os.getenv("SUPABASE_DB_URL")

def create_tables():
    if not DATABASE_URL:
        print("🚨 Error: SUPABASE_DB_URL not found in .env file.")
        return

    print("DIAGNOSTIC: Connecting to Supabase...")
    try:
        # Establish connection to Supabase PostgreSQL
        conn = psycopg2.connect(DATABASE_URL)
        cursor = conn.cursor()

        # 1. Create Transactions Table (The Ledger)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id SERIAL PRIMARY KEY,
            ticker VARCHAR(20) NOT NULL,
            action VARCHAR(10) NOT NULL CHECK (action IN ('BUY', 'SELL')),
            quantity DECIMAL NOT NULL,
            price DECIMAL NOT NULL,
            transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        print("✅ Created/Verified 'transactions' table.")

        # 2. Create Portfolio Table (The Current State)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS portfolio (
            id SERIAL PRIMARY KEY,
            ticker VARCHAR(20) UNIQUE NOT NULL,
            total_quantity DECIMAL NOT NULL DEFAULT 0,
            average_price DECIMAL NOT NULL DEFAULT 0,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        print("✅ Created/Verified 'portfolio' table.")

        # 3. Create Watchlist Targets Table (The Strategy)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS watchlist_targets (
            id SERIAL PRIMARY KEY,
            ticker VARCHAR(20) UNIQUE NOT NULL,
            target_price DECIMAL,
            stop_loss DECIMAL,
            notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        print("✅ Created/Verified 'watchlist_targets' table.")

        # Commit changes and close the connection
        conn.commit()
        cursor.close()
        conn.close()
        print("🎯 Phase 2 DB Setup Complete! Supabase is fully configured.")

    except Exception as e:
        print(f"🚨 CRITICAL CRASH (DB Setup): {e}")

if __name__ == "__main__":
    create_tables()
