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
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_recalculated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        print("✅ Created/Verified 'watchlist_targets' table.")

        # 4. Create Sector Valuations Table (Macro Guardrails)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS sector_valuations (
            id SERIAL PRIMARY KEY,
            sector_name VARCHAR(50) UNIQUE NOT NULL,
            historical_fair_pe DECIMAL NOT NULL,
            current_trailing_pe DECIMAL,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        print("✅ Created/Verified 'sector_valuations' table.")

        # 5. Create System Macro State Table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS system_macro_state (
            key TEXT PRIMARY KEY,
            value NUMERIC NOT NULL,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        """)
        
        cursor.execute("""
        INSERT INTO system_macro_state (key, value)
        VALUES ('cpi_inflation_rate', 0.092)
        ON CONFLICT (key) DO NOTHING;
        """)
        print("✅ Created/Verified 'system_macro_state' table.")

        # 6. Create Active Playbook Table (Learned Rules)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS active_playbook (
            id SERIAL PRIMARY KEY,
            source TEXT NOT NULL,
            rule TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT NOW()
        );
        """)
        print("✅ Created/Verified 'active_playbook' table.")

        # Commit changes and close the connection
        conn.commit()
        cursor.close()
        conn.close()
        print("🎯 Phase 2 DB Setup Complete! Supabase is fully configured.")

    except Exception as e:
        print(f"🚨 CRITICAL CRASH (DB Setup): {e}")

if __name__ == "__main__":
    create_tables()
