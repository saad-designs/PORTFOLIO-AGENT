import sys
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

import psycopg2
import psycopg2.extras
import os
import requests
# --- SAFE PSYCOPG2 MODULE-LEVEL IMPORT ---
try:
    import psycopg2
    import psycopg2.extras
    PSYCOPG2_AVAILABLE = True
except ImportError:
    psycopg2 = None
    PSYCOPG2_AVAILABLE = False
# -----------------------------------------
import functools
import datetime
import pandas as pd
import numpy as np
import yfinance as yf
import concurrent.futures
from bs4 import BeautifulSoup
from dotenv import load_dotenv
try:
    from langchain_core.tools import tool
except ImportError:
    from langchain.tools import tool
from typing import List, Dict
from cachetools import cached, TTLCache

load_dotenv()

# --- STRIP SURROUNDING QUOTES FROM CRITICAL ENV VARS ---
for _k in ["SUPABASE_DB_URL", "SUPABASE_URL", "SUPABASE_KEY", "GOOGLE_API_KEY", "GROQ_API_KEY", "YOUTUBE_API_KEY", "DATABASE_URL"]:
    _v = os.getenv(_k)
    if _v is not None:
        os.environ[_k] = _v.strip().strip('"').strip("'").strip()

DATABASE_URL = os.getenv("SUPABASE_DB_URL")

supabase = None
try:
    from supabase import create_client, Client
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY")
    if supabase_url and supabase_key:
        supabase = create_client(supabase_url, supabase_key)
    else:
        print("ℹ️ [Supabase] SUPABASE_URL or SUPABASE_KEY missing. Supabase Client disabled.", flush=True)
except Exception as sb_err:
    supabase = None
    print(f"⚠️ [Supabase Warning] Failed to initialize Supabase client: {sb_err}", flush=True)

import aiohttp
import json

DISCORD_BRIEFS_WEBHOOK = os.getenv("DISCORD_BRIEFS_WEBHOOK")

async def send_learning_brief(source: str, title: str, summary: str):
    """
    Fires an educational summary to persistent Agent Feed. Replaces legacy Discord webhooks.
    """
    cat = 'MACRO_NEWS'
    s_lower = source.lower()
    if 'youtube' in s_lower or 'mentor' in s_lower:
        cat = 'YOUTUBE_MENTOR'
    elif 'whatsapp' in s_lower or 'ktrade' in s_lower:
        cat = 'KTRADE_WHATSAPP'
    elif 'desk' in s_lower or 'manager' in s_lower:
        cat = 'DESK_MANAGER'
    elif 'alert' in s_lower or 'error' in s_lower or 'warning' in s_lower:
        cat = 'SYSTEM_ALERT'

    publish_agent_event(
        category=cat,
        title=f"📚 {source}: {title}",
        content=summary,
        metadata={"source": source, "title": title}
    )

    if DISCORD_BRIEFS_WEBHOOK:
        try:
            payload = {
                "embeds": [{
                    "title": f"📚 {source} Intelligence: {title}",
                    "description": summary[:2000],
                    "color": 3447003,
                    "footer": {"text": "Agent Ingestion Pipeline"}
                }]
            }
            async with aiohttp.ClientSession() as session:
                await session.post(DISCORD_BRIEFS_WEBHOOK, json=payload, timeout=aiohttp.ClientTimeout(total=3))
        except Exception:
            pass

import threading
import time as _time

_PG_POOL = None
_PG_POOL_LOCK = threading.Lock()
_CACHED_FIN_SUMMARY = None
_CACHED_FIN_SUMMARY_TS: float = 0.0


def invalidate_financial_summary_cache():
    global _CACHED_FIN_SUMMARY, _CACHED_FIN_SUMMARY_TS
    _CACHED_FIN_SUMMARY = None
    _CACHED_FIN_SUMMARY_TS = 0.0


class _PooledConnectionProxy:
    """Thin proxy returning psycopg2 connections to ThreadedConnectionPool on .close()."""
    __slots__ = ("_conn", "_pool", "_returned")

    def __init__(self, conn, pool):
        self._conn = conn
        self._pool = pool
        self._returned = False

    def __getattr__(self, name):
        return getattr(self._conn, name)

    def close(self):
        if self._returned:
            return
        self._returned = True
        try:
            if self._conn.closed:
                self._pool.putconn(self._conn, close=True)
            else:
                if self._conn.info.transaction_status not in (0,):
                    try:
                        self._conn.rollback()
                    except Exception:
                        self._pool.putconn(self._conn, close=True)
                        return
                self._pool.putconn(self._conn)
        except Exception:
            try:
                self._conn.close()
            except Exception:
                pass


def _get_or_init_pg_pool(conn_str: str):
    global _PG_POOL
    if _PG_POOL is not None:
        return _PG_POOL
    with _PG_POOL_LOCK:
        if _PG_POOL is None:
            from psycopg2 import pool as pg_pool
            _PG_POOL = pg_pool.ThreadedConnectionPool(minconn=1, maxconn=10, dsn=conn_str)
    return _PG_POOL


def get_db_connection(db_url=None):
    if not PSYCOPG2_AVAILABLE or psycopg2 is None:
        raise RuntimeError(
            "CRITICAL: 'psycopg2-binary' is missing from Python 3.12. "
            "Run 'py -3.12 -m pip install psycopg2-binary' in your terminal."
        )

    conn_str = db_url if db_url else DATABASE_URL
    if not db_url and conn_str:
        for _ in range(2):
            try:
                pool_inst = _get_or_init_pg_pool(conn_str)
                raw_conn = pool_inst.getconn()
                if raw_conn.closed:
                    pool_inst.putconn(raw_conn, close=True)
                    raw_conn = pool_inst.getconn()
                return _PooledConnectionProxy(raw_conn, pool_inst)
            except Exception:
                global _PG_POOL
                _PG_POOL = None

    max_retries = 3
    for attempt in range(max_retries):
        try:
            return psycopg2.connect(conn_str)
        except psycopg2.OperationalError as e:
            if attempt < max_retries - 1:
                _time.sleep(1)
            else:
                raise e

def create_dashboard_and_ledger_tables():
    """
    Creates and migrates the 5 essential dashboard & financial ledger tables in Supabase:
    1. agent_feed (with RLS enabled)
    2. capital_ledger (with RLS enabled)
    3. dividend_ledger (with RLS enabled)
    4. trade_history (with RLS enabled)
    5. chat_messages (with RLS enabled)
    """
    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        # 1. agent_feed
        cur.execute("""
            CREATE TABLE IF NOT EXISTS agent_feed (
                id BIGSERIAL PRIMARY KEY,
                category TEXT NOT NULL,
                title TEXT NOT NULL,
                content TEXT NOT NULL,
                metadata JSONB DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_agent_feed_category ON agent_feed (category);
            CREATE INDEX IF NOT EXISTS idx_agent_feed_created ON agent_feed (created_at DESC);
            ALTER TABLE agent_feed ENABLE ROW LEVEL SECURITY;
        """)

        # 2. capital_ledger
        cur.execute("""
            CREATE TABLE IF NOT EXISTS capital_ledger (
                id BIGSERIAL PRIMARY KEY,
                tx_type TEXT NOT NULL,
                amount NUMERIC(14,2) NOT NULL,
                notes TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            ALTER TABLE capital_ledger ADD COLUMN IF NOT EXISTS notes TEXT;
            ALTER TABLE capital_ledger ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW();
            ALTER TABLE capital_ledger ADD COLUMN IF NOT EXISTS reference_note TEXT;
            ALTER TABLE capital_ledger ADD COLUMN IF NOT EXISTS tx_date TIMESTAMPTZ DEFAULT NOW();
            CREATE INDEX IF NOT EXISTS idx_capital_ledger_created ON capital_ledger (created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_capital_ledger_tx_type ON capital_ledger (tx_type);
            ALTER TABLE capital_ledger ENABLE ROW LEVEL SECURITY;
        """)

        # 3. dividend_ledger
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dividend_ledger (
                id BIGSERIAL PRIMARY KEY,
                ticker TEXT NOT NULL,
                shares_held NUMERIC(14,2) NOT NULL,
                dps_gross NUMERIC(10,4) DEFAULT 0,
                gross_amount NUMERIC(14,2) NOT NULL,
                tax_rate_pct NUMERIC(5,2) DEFAULT 15.00,
                tax_withheld NUMERIC(14,2) NOT NULL,
                net_amount NUMERIC(14,2) NOT NULL,
                status TEXT NOT NULL DEFAULT 'CREDITED_TO_BANK',
                notes TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            ALTER TABLE dividend_ledger ADD COLUMN IF NOT EXISTS notes TEXT;
            ALTER TABLE dividend_ledger ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW();
            ALTER TABLE dividend_ledger ALTER COLUMN dps_gross SET DEFAULT 0;
            ALTER TABLE dividend_ledger ALTER COLUMN status SET DEFAULT 'CREDITED_TO_BANK';
            CREATE INDEX IF NOT EXISTS idx_dividend_ledger_ticker ON dividend_ledger (ticker);
            CREATE INDEX IF NOT EXISTS idx_dividend_ledger_status ON dividend_ledger (status);
            CREATE INDEX IF NOT EXISTS idx_dividend_ledger_created ON dividend_ledger (created_at DESC);
            ALTER TABLE dividend_ledger ENABLE ROW LEVEL SECURITY;
        """)

        # 4. trade_history
        cur.execute("""
            CREATE TABLE IF NOT EXISTS trade_history (
                id BIGSERIAL PRIMARY KEY,
                ticker TEXT NOT NULL,
                side TEXT NOT NULL,
                quantity NUMERIC(14,2) NOT NULL,
                price NUMERIC(12,4) NOT NULL,
                fees NUMERIC(12,2) DEFAULT 0.00,
                net_total NUMERIC(14,2) NOT NULL,
                notes TEXT,
                executed_at TIMESTAMPTZ DEFAULT NOW()
            );
            ALTER TABLE trade_history ADD COLUMN IF NOT EXISTS fees NUMERIC(12,2) DEFAULT 0.00;
            ALTER TABLE trade_history ADD COLUMN IF NOT EXISTS fees_and_tax NUMERIC(12,2) DEFAULT 0.00;
            ALTER TABLE trade_history ADD COLUMN IF NOT EXISTS notes TEXT;
            CREATE INDEX IF NOT EXISTS idx_trade_history_ticker ON trade_history (ticker);
            CREATE INDEX IF NOT EXISTS idx_trade_history_executed ON trade_history (executed_at DESC);
            ALTER TABLE trade_history ENABLE ROW LEVEL SECURITY;
        """)

        # 5. chat_messages
        cur.execute("""
            CREATE TABLE IF NOT EXISTS chat_messages (
                id BIGSERIAL PRIMARY KEY,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                actions_taken JSONB DEFAULT '[]'::jsonb,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_chat_messages_created ON chat_messages (created_at ASC);
            ALTER TABLE chat_messages ENABLE ROW LEVEL SECURITY;
        """)

        # 6. Unified portfolio_holdings, watchlist_targets, agent_checkpoints, institutional_reports
        cur.execute("""
            CREATE TABLE IF NOT EXISTS portfolio_holdings (
                id BIGSERIAL PRIMARY KEY,
                ticker TEXT UNIQUE NOT NULL,
                shares NUMERIC(14,4) NOT NULL DEFAULT 0,
                avg_buy_price NUMERIC(12,4) NOT NULL DEFAULT 0,
                last_price NUMERIC(12,4),
                last_change_pct NUMERIC(8,4) DEFAULT 0,
                price_updated_at TIMESTAMPTZ,
                dividend_yield NUMERIC(8,4),
                research_sentiment TEXT DEFAULT 'NEUTRAL',
                sentiment_updated_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );
            ALTER TABLE portfolio_holdings ADD COLUMN IF NOT EXISTS last_price NUMERIC(12,4);
            ALTER TABLE portfolio_holdings ADD COLUMN IF NOT EXISTS last_change_pct NUMERIC(8,4) DEFAULT 0;
            ALTER TABLE portfolio_holdings ADD COLUMN IF NOT EXISTS price_updated_at TIMESTAMPTZ;
            ALTER TABLE portfolio_holdings ADD COLUMN IF NOT EXISTS dividend_yield NUMERIC(8,4);
            ALTER TABLE portfolio_holdings ADD COLUMN IF NOT EXISTS research_sentiment TEXT DEFAULT 'NEUTRAL';
            ALTER TABLE portfolio_holdings ADD COLUMN IF NOT EXISTS sentiment_updated_at TIMESTAMPTZ;
            ALTER TABLE portfolio_holdings ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();

            CREATE TABLE IF NOT EXISTS watchlist_targets (
                ticker TEXT PRIMARY KEY,
                target_price NUMERIC(12,4),
                stop_loss NUMERIC(12,4),
                entry_zone NUMERIC(12,4),
                historical_fair_pe NUMERIC(8,2),
                current_trailing_pe NUMERIC(8,2),
                forward_eps NUMERIC(10,4),
                last_recalculated_date DATE,
                notes TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            ALTER TABLE watchlist_targets ADD COLUMN IF NOT EXISTS stop_loss NUMERIC(12,4);
            ALTER TABLE watchlist_targets ADD COLUMN IF NOT EXISTS entry_zone NUMERIC(12,4);
            ALTER TABLE watchlist_targets ADD COLUMN IF NOT EXISTS historical_fair_pe NUMERIC(8,2);
            ALTER TABLE watchlist_targets ADD COLUMN IF NOT EXISTS current_trailing_pe NUMERIC(8,2);
            ALTER TABLE watchlist_targets ADD COLUMN IF NOT EXISTS forward_eps NUMERIC(10,4);
            ALTER TABLE watchlist_targets ADD COLUMN IF NOT EXISTS last_recalculated_date DATE;

            CREATE TABLE IF NOT EXISTS agent_checkpoints (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS institutional_reports (
                id BIGSERIAL PRIMARY KEY,
                source TEXT,
                ticker TEXT,
                subject TEXT,
                sentiment TEXT,
                summary TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)

        # Ensure unique index on portfolio_holdings(ticker)
        cur.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_portfolio_holdings_ticker_uniq ON portfolio_holdings (ticker);
        """)

        # Permissive Policies for Service Role
        for tbl in ["agent_feed", "capital_ledger", "dividend_ledger", "trade_history", "chat_messages"]:
            cur.execute(f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_policies WHERE tablename = '{tbl}' AND policyname = 'Allow service access on {tbl}'
                    ) THEN
                        CREATE POLICY "Allow service access on {tbl}" ON {tbl} FOR ALL USING (true);
                    END IF;
                END $$;
            """)

        conn.commit()
    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        print(f"⚠️ [Database Setup Warning] Could not ensure dashboard tables: {e}", flush=True)
    finally:
        if cur:
            try:
                cur.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass

def get_whatsapp_checkpoint() -> int:
    """Returns the unix timestamp of the last analyzed WhatsApp message."""
    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT value FROM agent_checkpoints WHERE key = 'whatsapp_last_timestamp'")
        res = cur.fetchone()
        if res:
            return int(res[0])
        return 0
    except Exception as e:
        print(f"⚠️ [Checkpoints Warning] fetching whatsapp checkpoint: {e}", flush=True)
        return 0
    finally:
        if cur:
            try:
                cur.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass

def update_whatsapp_checkpoint(timestamp: int):
    """Advances the WhatsApp watermark forward."""
    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO agent_checkpoints (key, value, updated_at)
            VALUES ('whatsapp_last_timestamp', %s, NOW())
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW();
        """, (str(timestamp),))
        conn.commit()
    except Exception as e:
        print(f"⚠️ [Checkpoints Warning] updating whatsapp checkpoint: {e}", flush=True)
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
    finally:
        if cur:
            try:
                cur.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass

def get_checkpoint(task_key: str) -> int:
    """Fetches the Unix timestamp watermark for a specific ingestion pipeline."""
    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT value FROM agent_checkpoints WHERE key = %s", (task_key,))
        res = cur.fetchone()
        if res:
            raw_val = res[0]
            if isinstance(raw_val, dict):
                if 'value' in raw_val:
                    return int(raw_val['value'])
                return raw_val
            try:
                return int(raw_val)
            except (ValueError, TypeError):
                return raw_val
        return 0
    except Exception as e:
        print(f"⚠️ [Checkpoints Warning] fetching checkpoint {task_key}: {e}", flush=True)
        return 0
    finally:
        if cur:
            try:
                cur.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass

def update_checkpoint(task_key: str, timestamp: int):
    """Advances the watermark forward after successful ingestion."""
    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO agent_checkpoints (key, value, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW();
        """, (task_key, str(timestamp)))
        conn.commit()
    except Exception as e:
        print(f"⚠️ [Checkpoints Warning] updating checkpoint {task_key}: {e}", flush=True)
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
    finally:
        if cur:
            try:
                cur.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass

def get_whatsapp_monitored_chats() -> list[str]:
    """Dynamically fetches active mentor WhatsApp IDs from the database."""
    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT chat_id FROM whatsapp_monitored_chats WHERE is_active = TRUE")
        res = cur.fetchall()
        return [r[0] for r in res]
    except Exception as e:
        print(f"⚠️ [Checkpoints Warning] fetching monitored chats: {e}", flush=True)
        return []
    finally:
        if cur:
            try:
                cur.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass

def sanitize_price(raw_price) -> float:
    """Strips commas and handles raw API strings to prevent PnL hallucinations."""
    if raw_price is None:
        return 0.0
    if isinstance(raw_price, (int, float)):
        return float(raw_price)
    
    clean_string = str(raw_price).replace(',', '').replace('Rs.', '').replace(' ', '').strip()
    try:
        return float(clean_string)
    except ValueError:
        print(f"[ERROR] Could not parse price: {raw_price}")
        return 0.0

@cached(cache=TTLCache(maxsize=100, ttl=900))
def get_live_price(ticker: str) -> float:
    """Scrapes the live current price from the PSX Data Portal."""
    try:
        url = f"https://dps.psx.com.pk/company/{ticker.upper()}"
        from market_data_scraper import fetch_psx_data_robust_sync
        html_data = fetch_psx_data_robust_sync(url, is_json=False)
        if not html_data:
            return 0.0
            
        soup = BeautifulSoup(html_data, 'html.parser')
        
        # The PSX Data Portal stores the live price inside the 'quote__close' class
        price_div = soup.find('div', class_='quote__close')
        if price_div:
            return sanitize_price(price_div.text)
        return 0.0
    except Exception as e:
        print(f"Error fetching live price for {ticker}: {e}")
        return 0.0

@cached(cache=TTLCache(maxsize=100, ttl=900))
def get_live_price_and_change(ticker: str) -> tuple[float, float]:
    """Scrapes the live current price and daily percentage change from the PSX Data Portal."""
    try:
        url = f"https://dps.psx.com.pk/company/{ticker.upper()}"
        from market_data_scraper import fetch_psx_data_robust_sync
        html_data = fetch_psx_data_robust_sync(url, is_json=False)
        if not html_data:
            return 0.0, 0.0
            
        soup = BeautifulSoup(html_data, 'html.parser')
        
        price = 0.0
        change_pct = 0.0
        
        price_div = soup.find('div', class_='quote__close')
        if price_div:
            price = sanitize_price(price_div.text)
            
        change_div = soup.find('div', class_='quote__change')
        if change_div:
            import re
            match = re.search(r'\(([-+]?[0-9]*\.?[0-9]+)%\)', change_div.text)
            if match:
                change_pct = float(match.group(1))
                
        return price, change_pct
    except Exception as e:
        print(f"Error fetching live price and change for {ticker}: {e}")
        return 0.0, 0.0

def log_trade(ticker: str, action: str, quantity: float, price: float, fees: float = 0.0) -> str:
    action_clean = str(action or "").strip().upper()
    if action_clean not in ['BUY', 'SELL']:
        return "🚨 Error: Action must be 'BUY' or 'SELL'."
    try:
        res = record_trade_execution(
            ticker=ticker,
            side=action_clean,
            quantity=float(quantity),
            price=float(price),
            fees=float(fees or 0.0),
            notes="Logged via Agent Tool"
        )
        return f"✅ **{res['side']} LOGGED:** {res['quantity']} shares of {res['ticker']} at Rs. {res['price']} (Fees: Rs. {res['fees']})."
    except Exception as e:
        import traceback
        return f"🚨 Database Error: {e}\n{traceback.format_exc()}"

def set_target(ticker: str, target_price: float, stop_loss: float = 0.0) -> str:
    ticker = ticker.upper()
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Upsert target logic
        cursor.execute("""
            INSERT INTO watchlist_targets (ticker, target_price, stop_loss)
            VALUES (%s, %s, %s)
            ON CONFLICT (ticker) DO UPDATE SET
            target_price = EXCLUDED.target_price,
            stop_loss = EXCLUDED.stop_loss,
            notes = 'Target updated via Discord',
            created_at = CURRENT_TIMESTAMP;
        """, (ticker, target_price, stop_loss))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return f"🎯 **TARGET SET:** {ticker} | Target: Rs. {target_price} | Stop Loss: Rs. {stop_loss}"
    except Exception as e:
        import traceback
        return f"🚨 Database Error: {e}\n{traceback.format_exc()}"

def get_portfolio_summary() -> str:
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # We join the portfolio table with targets to show everything at once
        cursor.execute("""
            SELECT p.ticker, p.shares as total_quantity, p.avg_buy_price as average_price, w.target_price
            FROM portfolio_holdings p
            LEFT JOIN watchlist_targets w ON p.ticker = w.ticker
            WHERE p.shares > 0 
            ORDER BY p.ticker ASC
        """)
        rows = cursor.fetchall()
        
        cursor.close()
        conn.close()

        if not rows:
            return "📊 **Your Portfolio is currently empty.**"

        summary = "📊 **Live PSX Portfolio & PnL:**\n```text\n"
        summary += f"{'TICKER':<8} | {'QTY':<6} | {'AVG COST':<8} | {'LIVE':<8} | {'VALUE (Rs)':<10} | {'PnL (Rs)':<10} | {'PnL %'}\n"
        summary += "-" * 75 + "\n"
        
        total_invested = 0.0
        total_current_value = 0.0
        
        for row in rows:
            ticker = row[0]
            qty = float(row[1])
            avg_price = float(row[2])
            target_price = float(row[3]) if row[3] else None
            
            live_price = get_live_price(ticker)
            if live_price == 0.0:
                live_price = avg_price # Fallback to prevent math errors if PSX site is down
                
            invested = qty * avg_price
            current_value = qty * live_price
            pnl_rs = current_value - invested
            pnl_pct = (pnl_rs / invested) * 100 if invested > 0 else 0
            
            total_invested += invested
            total_current_value += current_value
            
            pnl_sign = "+" if pnl_rs >= 0 else ""
            
            summary += f"{ticker:<8} | {qty:<6.0f} | {avg_price:<8.2f} | {live_price:<8.2f} | {current_value:<10,.2f} | {pnl_sign}{pnl_rs:<9,.2f} | {pnl_sign}{pnl_pct:.2f}%\n"
            
            if target_price:
                summary += f"  ↳ Target: {target_price:.2f} | Away: {((target_price - live_price) / live_price) * 100:.1f}%\n"
        
        total_pnl_rs = total_current_value - total_invested
        total_pnl_pct = (total_pnl_rs / total_invested) * 100 if total_invested > 0 else 0
        total_pnl_sign = "+" if total_pnl_rs >= 0 else ""

        summary += "-" * 75 + "\n"
        summary += f"TOTAL DEPLOYED: Rs. {total_invested:,.2f}\n"
        summary += f"CURRENT VALUE:  Rs. {total_current_value:,.2f}\n"
        summary += f"TOTAL PnL:      {total_pnl_sign}Rs. {total_pnl_rs:,.2f} ({total_pnl_sign}{total_pnl_pct:.2f}%)\n```"
        
        return summary

    except Exception as e:
        import traceback
        return f"🚨 Database Error: {e}\n{traceback.format_exc()}"

def add_pending_signal(source: str, payload: dict) -> bool:
    """Inserts a pending signal into the Supabase pending_signals table."""
    try:
        import json
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO pending_signals (source, signal_payload)
            VALUES (%s, %s)
            """,
            (source, json.dumps(payload))
        )
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        import traceback
        print(f"Error adding pending signal: {e}\n{traceback.format_exc()}")
        return False


def get_watchlist_summary() -> str:
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT ticker, target_price FROM watchlist_targets")
        rows = cursor.fetchall()
        
        cursor.close()
        conn.close()

        if not rows:
            return "📊 **Your Watchlist is currently empty.**"

        summary = ""
        for row in rows:
            ticker = row[0]
            target_price = float(row[1]) if row[1] else 0.0
            
            live_price = get_live_price(ticker)
            
            if live_price > 0 and target_price > 0:
                # distance can be positive or negative, but user just asked for percentage distance away
                distance = abs(((target_price - live_price) / live_price) * 100)
                summary += f"{ticker} | Live: {live_price:.2f} | Target: {target_price:.2f} | Distance: {distance:.1f}% away\n"
            else:
                summary += f"{ticker} | Live: {live_price:.2f} | Target: {target_price:.2f}\n"
                
        return summary.strip()

    except Exception as e:
        import traceback
        return f"🚨 Database Error: {e}\n{traceback.format_exc()}"

import math

def calculate_concentration_trim(ticker: str, current_shares: int, current_price: float, total_portfolio_value: float, max_asset_cap: float = 0.30, dcf_fair_value: float = 0.0) -> dict:
    """
    Calculates the exact whole number of shares to liquidate to bring an asset
    strictly back under the max allowable single-asset portfolio cap, guarded by fundamental value logic.
    """
    if current_price <= 0 or total_portfolio_value <= 0:
        return {"action": "HOLD", "shares_to_sell": 0, "excess_pkr": 0.0, "expected_cash_gen": 0.0}

    current_holding_value = current_shares * current_price
    weight = current_holding_value / total_portfolio_value

    # RULE 1: Only consider concentration risk if the position exceeds an extreme structural limit (e.g., 30%)
    if weight <= max_asset_cap:
        return {
            "action": "HOLD",
            "current_weight": weight,
            "shares_to_sell": 0,
            "excess_pkr": 0.0,
            "expected_cash_gen": 0.0
        }

    # RULE 2: If it does exceed cap, check valuation before selling. 
    # If it's deeply undervalued, we accept the concentration because selling a compounder for tax/friction reasons is illogical.
    margin_of_safety = 0.0
    if dcf_fair_value > 0:
        margin_of_safety = (dcf_fair_value - current_price) / dcf_fair_value
        
    if margin_of_safety > 0.15:
        return {
            "action": "HOLD",
            "current_weight": weight,
            "shares_to_sell": 0,
            "excess_pkr": 0.0,
            "expected_cash_gen": 0.0,
            "override_msg": f"Position exceeds weight limit ({weight*100:.1f}%), but maintains a strong DCF margin of safety ({margin_of_safety*100:.1f}%). Overriding concentration trim to protect compounding."
        }

    # Calculate target shares and exact shares to liquidate
    max_allowed_value = total_portfolio_value * max_asset_cap
    target_shares_to_hold = math.floor(max_allowed_value / current_price)
    shares_to_sell = current_shares - target_shares_to_hold
    expected_cash_gen = shares_to_sell * current_price
    new_weight = (target_shares_to_hold * current_price) / total_portfolio_value

    return {
        "action": "TRIM",
        "current_shares": current_shares,
        "target_shares": target_shares_to_hold,
        "shares_to_sell": shares_to_sell,
        "expected_cash_gen": expected_cash_gen,
        "current_weight": weight,
        "projected_weight": new_weight
    }

def check_profit_and_rebalance_triggers() -> list[dict]:
    """
    Evaluates the portfolio against quantitative rules and returns actionable alerts.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get total cash from unified capital_ledger (single source of truth)
        cursor.execute("""
            SELECT COALESCE(SUM(
                CASE
                    WHEN tx_type IN ('DEPOSIT_TO_KTRADE', 'DEPOSIT_TO_BROKER', 'DIVIDEND_SENT_TO_KTRADE', 'DIVIDEND_BANK_TO_BROKER') THEN amount
                    WHEN tx_type IN ('WITHDRAWAL_FROM_KTRADE', 'WITHDRAWAL_FROM_BROKER') THEN -amount
                    WHEN tx_type = 'FEE_ADJUSTMENT' THEN amount
                    ELSE 0
                END
            ), 0)
            FROM capital_ledger
        """)
        cash_res = cursor.fetchone()
        cash_balance = float(cash_res[0]) if cash_res else 0.0
        
        # Get portfolio holdings
        cursor.execute("SELECT ticker, shares as total_quantity, avg_buy_price as average_price FROM portfolio_holdings WHERE shares > 0")
        holdings = cursor.fetchall()
        
        # Get watchlist targets and sector data
        # Ticker-by-ticker valuation data — reads the new columns added to watchlist_targets
        cursor.execute("SELECT ticker, target_price, last_recalculated_date, historical_fair_pe, current_trailing_pe FROM watchlist_targets")
        watchlist_data = {
            row[0]: {
                "target_price": float(row[1]) if row[1] else None,
                "last_recalculated_date": row[2],
                "historical_fair_pe": float(row[3]) if row[3] else None,
                "current_trailing_pe": float(row[4]) if row[4] else None,
            }
            for row in cursor.fetchall()
        }
        
        cursor.close()
        conn.close()
        
        if not holdings:
            return []
            
        # Calculate total portfolio value
        positions = []
        equity_value = 0.0
        
        for row in holdings:
            ticker = row[0]
            shares = float(row[1])
            avg_price = float(row[2])
            current_price = get_live_price(ticker)
            
            if current_price == 0.0:
                current_price = avg_price # fallback
                
            pos_value = shares * current_price
            equity_value += pos_value
            
            positions.append({
                "ticker": ticker,
                "shares": shares,
                "avg_price": avg_price,
                "current_price": current_price,
                "position_value": pos_value
            })
            
        total_value = equity_value + cash_balance
        if total_value <= 0:
            return []
            
        alerts = []
        
        # Identify best candidate for cash buffer trim (e.g., largest gain)
        best_trim_candidate = None
        max_gain_pct = -100.0
        
        for pos in positions:
            ticker = pos["ticker"]
            shares = pos["shares"]
            avg_price = pos["avg_price"]
            current_price = pos["current_price"]
            pos_value = pos["position_value"]
            
            gain_pct = ((current_price - avg_price) / avg_price) * 100 if avg_price > 0 else 0.0
            
            if gain_pct > max_gain_pct:
                max_gain_pct = gain_pct
                best_trim_candidate = pos
                
            # Fundamental Target Valuation Trigger (Replaces static 15% gain)
            w_data = watchlist_data.get(ticker, {})
            target_price = w_data.get("target_price")
            last_recalculated = w_data.get("last_recalculated_date")
            sector_name = w_data.get("sector_name")
            
            if target_price and current_price >= target_price:
                import datetime
                # 90-day Data Rot Check
                if last_recalculated and (datetime.datetime.now() - last_recalculated).days > 90:
                    alerts.append({
                        "type": "stale_target_warning",
                        "ticker": ticker,
                        "current_price": current_price,
                        "current_shares": shares,
                        "target_price": target_price,
                        "message": f"Target price of {target_price} is over 90 days old. Fundamental re-evaluation required."
                    })
                else:
                    shares_to_sell = max(1, round(shares * 0.30))
                    cash_gen = shares_to_sell * current_price
                    alerts.append({
                        "type": "target_valuation_reached",
                        "ticker": ticker,
                        "current_price": current_price,
                        "current_shares": shares,
                        "target_price": target_price,
                        "gain_pct": gain_pct,
                        "shares_to_sell": shares_to_sell,
                        "cash_generated": cash_gen,
                        "new_cash_balance": cash_balance + cash_gen
                    })
                    
            # Macro Overvaluation Trigger (ticker-by-ticker, no sector proxy)
            hist_pe = w_data.get("historical_fair_pe")
            curr_pe = w_data.get("current_trailing_pe")

            if hist_pe and curr_pe and curr_pe > (hist_pe * 1.2):
                shares_to_sell = max(1, round(shares * 0.30))
                cash_gen = shares_to_sell * current_price
                alerts.append({
                    "type": "macro_overvaluation",
                    "ticker": ticker,
                    "current_price": current_price,
                    "current_shares": shares,
                    "current_trailing_pe": curr_pe,
                    "historical_fair_pe": hist_pe,
                    "shares_to_sell": shares_to_sell,
                    "cash_generated": cash_gen,
                    "new_cash_balance": cash_balance + cash_gen
                })
            # Concentration Trigger
            dcf_fair = w_data.get("target_price") or 0.0
            trim_data = calculate_concentration_trim(ticker, shares, current_price, total_value, max_asset_cap=0.30, dcf_fair_value=dcf_fair)
            
            if trim_data["action"] == "TRIM" and trim_data["shares_to_sell"] > 0:
                shares_to_sell = trim_data["shares_to_sell"]
                cash_gen = trim_data["expected_cash_gen"]
                alerts.append({
                    "type": "concentration_risk",
                    "ticker": ticker,
                    "current_price": current_price,
                    "current_shares": shares,
                    "gain_pct": gain_pct,
                    "shares_to_sell": shares_to_sell,
                    "cash_generated": cash_gen,
                    "new_cash_balance": cash_balance + cash_gen
                })
                
        # Cash Buffer Trigger
        if cash_balance < total_value * 0.05 and best_trim_candidate:
            target_cash = total_value * 0.10
            cash_needed = target_cash - cash_balance
            trim_ticker = best_trim_candidate["ticker"]
            trim_price = best_trim_candidate["current_price"]
            trim_shares = max(1, round(cash_needed / trim_price))
            
            # Ensure we don't sell more than we own
            if trim_shares > best_trim_candidate["shares"]:
                trim_shares = int(best_trim_candidate["shares"])
                
            if trim_shares > 0:
                cash_gen = trim_shares * trim_price
                alerts.append({
                    "type": "low_cash_buffer",
                    "ticker": trim_ticker,
                    "current_price": trim_price,
                    "gain_pct": max_gain_pct,
                    "shares_to_sell": trim_shares,
                    "cash_generated": cash_gen,
                    "new_cash_balance": cash_balance + cash_gen
                })
            
        return alerts
        
    except Exception as e:
        import traceback
        print(f"Error checking profit/rebalance triggers: {e}")
        traceback.print_exc()
        return []

def generate_daily_closing_summary() -> dict:
    """Generates the daily closing summary and liquidity report."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 1. Fetch Liquid Cash from unified capital_ledger
        cursor.execute("""
            SELECT COALESCE(SUM(
                CASE
                    WHEN tx_type IN ('DEPOSIT_TO_KTRADE', 'DEPOSIT_TO_BROKER', 'DIVIDEND_SENT_TO_KTRADE', 'DIVIDEND_BANK_TO_BROKER') THEN amount
                    WHEN tx_type IN ('WITHDRAWAL_FROM_KTRADE', 'WITHDRAWAL_FROM_BROKER') THEN -amount
                    WHEN tx_type = 'FEE_ADJUSTMENT' THEN amount
                    ELSE 0
                END
            ), 0)
            FROM capital_ledger
        """)
        cash_res = cursor.fetchone()
        liquid_cash = float(cash_res[0]) if cash_res else 0.0
        
        # 2. FIXED: Aligned schema columns to match your actual database structure
        cursor.execute("SELECT ticker, shares, avg_buy_price FROM portfolio_holdings WHERE shares > 0")
        holdings = cursor.fetchall()
        
        cursor.execute("SELECT daily_pnl, daily_pnl_pct FROM portfolio_daily_snapshots WHERE snapshot_date = CURRENT_DATE")
        pnl_res = cursor.fetchone()
        daily_pnl = float(pnl_res[0]) if pnl_res else 0.0
        daily_pnl_pct = float(pnl_res[1]) if pnl_res else 0.0
        
        cursor.close()
        conn.close()
        
        equity_value = 0.0
        top_gainer = {"ticker": None, "change": -999.9}
        top_loser = {"ticker": None, "change": 999.9}
        
        if not holdings:
            return {
                "total_portfolio_value": liquid_cash,
                "liquid_cash": liquid_cash,
                "equity_allocation_pct": 0.0,
                "top_gainer": ("N/A", 0.0),
                "top_loser": ("N/A", 0.0),
                "daily_pnl": 0.0,
                "daily_pnl_pct": 0.0,
                "current_holdings": []
            }
            
        current_holdings_list = []
        for row in holdings:
            ticker = row[0]
            shares = float(row[1])
            avg_price = float(row[2])
            
            current_price, change_pct = get_live_price_and_change(ticker)
            if current_price == 0.0:
                current_price = avg_price  # Fallback to avg buy price if PSX API fails
                change_pct = 0.0
                
            market_value = shares * current_price
            equity_value += market_value
            current_holdings_list.append({"ticker": ticker, "market_value": market_value})
            
            if change_pct > top_gainer["change"]:
                top_gainer = {"ticker": ticker, "change": change_pct}
                
            if change_pct < top_loser["change"]:
                top_loser = {"ticker": ticker, "change": change_pct}
                
        total_value = equity_value + liquid_cash
        allocation_pct = (equity_value / total_value) * 100 if total_value > 0 else 0.0
        
        return {
            "total_portfolio_value": total_value,
            "liquid_cash": liquid_cash,
            "equity_allocation_pct": allocation_pct,
            "top_gainer": (top_gainer["ticker"], top_gainer["change"]),
            "top_loser": (top_loser["ticker"], top_loser["change"]),
            "daily_pnl": daily_pnl,
            "daily_pnl_pct": daily_pnl_pct,
            "current_holdings": current_holdings_list
        }
    except Exception as e:
        import traceback
        print(f"Error generating daily summary: {e}")
        traceback.print_exc()
        return {}

def record_daily_snapshot():
    """
    Records the daily portfolio snapshot into the database, calculating daily PnL.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 1. Fetch current cash from unified capital_ledger
        cursor.execute("""
            SELECT COALESCE(SUM(
                CASE
                    WHEN tx_type IN ('DEPOSIT_TO_KTRADE', 'DEPOSIT_TO_BROKER', 'DIVIDEND_SENT_TO_KTRADE', 'DIVIDEND_BANK_TO_BROKER') THEN amount
                    WHEN tx_type IN ('WITHDRAWAL_FROM_KTRADE', 'WITHDRAWAL_FROM_BROKER') THEN -amount
                    WHEN tx_type = 'FEE_ADJUSTMENT' THEN amount
                    ELSE 0
                END
            ), 0)
            FROM capital_ledger
        """)
        cash_res = cursor.fetchone()
        cash_balance = float(cash_res[0]) if cash_res else 0.0
        
        # 2. Fetch current holdings and calculate equity value
        cursor.execute("SELECT ticker, shares as total_quantity, avg_buy_price as average_price FROM portfolio_holdings WHERE shares > 0")
        holdings = cursor.fetchall()
        
        equity_value = 0.0
        for row in holdings:
            ticker = row[0]
            shares = float(row[1])
            current_price = get_live_price(ticker)
            if current_price == 0.0:
                current_price = float(row[2]) # fallback
            equity_value += shares * current_price
            
        total_portfolio_value = equity_value + cash_balance
        
        # 3. Query the previous trading day's total_portfolio_value
        cursor.execute("""
            SELECT total_portfolio_value 
            FROM portfolio_daily_snapshots 
            WHERE snapshot_date < CURRENT_DATE 
            ORDER BY snapshot_date DESC LIMIT 1
        """)
        prev_res = cursor.fetchone()
        prev_value = float(prev_res[0]) if prev_res else total_portfolio_value
        
        # 4. Calculate PnL
        daily_pnl = total_portfolio_value - prev_value
        daily_pnl_pct = (daily_pnl / prev_value) * 100 if prev_value > 0 else 0.0
        
        # 5. Insert or update the snapshot
        cursor.execute("""
            INSERT INTO portfolio_daily_snapshots 
                (snapshot_date, equity_value, cash_balance, total_portfolio_value, daily_pnl, daily_pnl_pct)
            VALUES (CURRENT_DATE, %s, %s, %s, %s, %s)
            ON CONFLICT (snapshot_date) DO UPDATE SET
                equity_value = EXCLUDED.equity_value,
                cash_balance = EXCLUDED.cash_balance,
                total_portfolio_value = EXCLUDED.total_portfolio_value,
                daily_pnl = EXCLUDED.daily_pnl,
                daily_pnl_pct = EXCLUDED.daily_pnl_pct,
                created_at = NOW();
        """, (equity_value, cash_balance, total_portfolio_value, daily_pnl, daily_pnl_pct))
        
        conn.commit()
        cursor.close()
        conn.close()
        
    except Exception as e:
        import traceback
        print(f"Error recording daily snapshot: {e}")
        traceback.print_exc()

def scan_shariah_market() -> str:
    """Scans predefined Shariah-compliant heavyweights for buying opportunities based on historical fair-value zones."""
    # Define an exclusive subset of the official KMI-30 / PSX-KMI compliant universe
    # This acts as our strict Shariah filter layer.
    kmi30_universe = {
        "SYS": 300.0,    # Systems Limited (Technology)
        "MEBL": 200.0,   # Meezan Bank (Islamic Banking)
        "LUCK": 800.0,   # Lucky Cement (Cement)
        "HUBC": 115.0,   # Hub Power (Power Generation)
        "ENGRO": 300.0,  # Engro Corporation (Fertilizer)
        "POL": 400.0,    # Pakistan Oilfields (Oil & Gas)
        "MTL": 500.0,    # Millat Tractors (Auto)
        "MARI": 2500.0,  # Mari Petroleum (Oil & Gas)
        "HUBC": 115.0    # Hub Power
    }
    
    summary = "🕌 **Strict Shariah-Compliant Market Scan (KMI-30 Universe)**\n```text\n"
    summary += f"{'TICKER':<8} | {'LIVE PRICE':<12} | {'FAIR VALUE':<12} | {'STATUS'}\n"
    summary += "-" * 55 + "\n"
    
    opportunities = 0
    
    for ticker, fair_value in kmi30_universe.items():
        live_price = get_live_price(ticker)
        
        if live_price == 0.0:
            status = "DATA UNAVAILABLE"
        elif live_price <= fair_value:
            distance = ((fair_value - live_price) / fair_value) * 100
            status = f"🟢 BUY ZONE ({distance:.1f}% discount)"
            opportunities += 1
        else:
            distance = ((live_price - fair_value) / fair_value) * 100
            status = f"🔴 OVERVALUED ({distance:.1f}% premium)"
            
        summary += f"{ticker:<8} | {live_price:<12.2f} | {fair_value:<12.2f} | {status}\n"
        
    summary += "```\n"
    
    if opportunities > 0:
        summary += f"✅ Found **{opportunities}** KMI-30 compliant equities trading in their fair-value entry zone."
    else:
        summary += "⚠️ No KMI-30 compliant heavyweights are currently trading below their historical fair-value."
        
    return summary

import json
import os
import psycopg2

def sync_portfolio_tool(holdings_json: str) -> str:
    """Purges the current portfolio table and injects clean, type-safe records."""
    try:
        holdings = json.loads(holdings_json)
        db_url = os.getenv("DATABASE_URL")
        conn = get_db_connection(db_url)
        cursor = conn.cursor()
        
        # Purge old records completely
        cursor.execute("DELETE FROM portfolio_holdings;")
        
        for item in holdings:
            ticker = str(item.get("ticker")).strip().upper()
            shares = int(item.get("shares"))          # Enforce integer
            avg_cost = float(item.get("avg_cost"))    # Enforce float
            
            cursor.execute(
                """
                INSERT INTO portfolio_holdings (ticker, shares, avg_buy_price)
                VALUES (%s, %s, %s)
                """,
                (ticker, shares, avg_cost)
            )
            
        conn.commit()
        cursor.close()
        conn.close()
        return "Database synchronization successful: Old records purged, clean portfolio rows inserted."
    except Exception as e:
        import traceback
        return f"Database sync failed: {str(e)}\n{traceback.format_exc()}"

@cached(cache=TTLCache(maxsize=100, ttl=900))
def get_agent_trade_recommendation(ticker: str) -> dict:
    """This function is obsolete and stubbed. Agent logic lives in agent.py graph."""
    return {}


def _scrape_psx_pe(ticker: str) -> float | None:
    """
    Scrapes the live TTM P/E ratio for a single ticker directly from the PSX Data Portal.
    The page is server-rendered (Nuxt SSR), so BeautifulSoup reads the static HTML payload.
    Target element: <div class="stats_label">P/E Ratio (TTM) **</div>
                    <div class="stats_value">11.21</div>

    FRAGILITY NOTE: This scraper targets CSS class names (stats_label / stats_value).
    A PSX frontend update that renames these classes will cause silent NoneType failures.
    Monitor ingestion logs for the [PSX Scraper STRUCTURAL FAILURE] warning below.
    """
    try:
        url = f"https://dps.psx.com.pk/company/{ticker.upper()}"
        from market_data_scraper import fetch_psx_data_robust_sync
        html_data = fetch_psx_data_robust_sync(url, is_json=False)
        if not html_data:
            return None
            
        soup = BeautifulSoup(html_data, "html.parser")

        # Sanity check: verify the DOM still has the expected structure at all.
        # If stats_label divs are absent entirely, the PSX UI has changed.
        all_labels = soup.find_all("div", class_="stats_label")
        if not all_labels:
            print(
                f"[PSX Scraper STRUCTURAL FAILURE] {ticker}: No 'stats_label' divs found. "
                "The PSX portal DOM structure has likely changed. "
                "Re-inspect dps.psx.com.pk/company/{ticker} and update _scrape_psx_pe()."
            )
            return None

        # Locate the stats_label containing "P/E Ratio (TTM)" and grab its sibling value
        for label_div in all_labels:
            if "P/E Ratio (TTM)" in label_div.get_text():
                value_div = label_div.find_next_sibling("div", class_="stats_value")
                if value_div:
                    text = value_div.get_text(strip=True).replace(",", "")
                    try:
                        pe = float(text)
                        if pe > 0:
                            return pe
                    except ValueError:
                        print(f"[PSX Scraper] {ticker}: Could not parse P/E value '{text}' as float.")
                        return None
                else:
                    # Label found but sibling value div missing — partial DOM change
                    print(
                        f"[PSX Scraper STRUCTURAL FAILURE] {ticker}: 'P/E Ratio (TTM)' label found "
                        "but sibling 'stats_value' div is missing. Check for DOM structure change."
                    )
                    return None

        # Label not found at all — P/E may genuinely not be listed (e.g., negative earnings)
        print(f"[PSX Scraper] {ticker}: 'P/E Ratio (TTM)' label not present (earnings may be negative or N/A).")
        return None

    except requests.exceptions.HTTPError as e:
        print(f"[PSX Scraper] HTTP error fetching {ticker}: {e}")
        return None
    except Exception as e:
        print(f"[PSX Scraper] Failed to fetch P/E for {ticker}: {e}")
        return None



def update_ticker_valuations():
    """
    Iterates through all active watchlist tickers and scrapes their live TTM P/E
    directly from the PSX Data Portal, storing results in watchlist_targets.current_trailing_pe.
    Replaces the flawed yfinance sector-proxy approach (update_sector_valuations).
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT ticker FROM watchlist_targets")
        tickers = [row[0] for row in cursor.fetchall()]

        updated = 0
        for ticker in tickers:
            pe = _scrape_psx_pe(ticker)
            if pe is not None:
                cursor.execute("""
                    UPDATE watchlist_targets
                    SET current_trailing_pe = %s
                    WHERE ticker = %s
                """, (pe, ticker))
                updated += 1
                print(f"[update_ticker_valuations] {ticker}: TTM P/E = {pe}")
            else:
                print(f"[update_ticker_valuations] {ticker}: P/E not available on PSX portal")

        conn.commit()
        cursor.close()
        conn.close()
        print(f"[update_ticker_valuations] Done. Updated {updated}/{len(tickers)} tickers.")
    except Exception as e:
        print(f"Error in update_ticker_valuations: {e}")

import requests
import re

def fetch_dividend_yield(ticker: str) -> float:
    """
    Fetches the dividend yield securely via Yahoo Finance API, bypassing 
    regional WAFs completely. 
    """
    try:
        # Append .KA for Karachi Stock Exchange indexing
        yahoo_ticker = f"{ticker}.KA"
        stock = yf.Ticker(yahoo_ticker)
        
        # yfinance sometimes uses trailingAnnualDividendYield instead of dividendYield
        div_yield = stock.info.get("dividendYield")
        if div_yield is None:
            div_yield = stock.info.get("trailingAnnualDividendYield", 0.0)
        
        # Guard against Yahoo returning None for stocks with no yield
        if div_yield is None:
            return 0.0
            
        return round(float(div_yield), 4)

    except Exception as e:
        print(f"[ERROR] yfinance routing failed for {ticker}: {e}")
        return 0.0

@cached(cache=TTLCache(maxsize=100, ttl=900))
def get_fundamentals(ticker: str) -> dict:
    """
    Fetches available fundamentals from the PSX Data Portal JSON API (Option A)
    with a fallback to scraping Sarmaaya.pk (Option B).
    Returns a dict with 'eps', 'roic', and 'trailingPE'.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Query only the columns present in the Supabase schema
        cur.execute("SELECT dividend_yield, eps, roic FROM fundamentals WHERE ticker = %s", (ticker,))
        row = cur.fetchone()
        conn.close()
        
        if row and row[0] is not None:
            return {
                "dividend_yield": float(row[0]) if row[0] is not None else 0.0,
                "trailingPE": 0.0,  # Not stored in table schema
                "eps": float(row[1]) if row[1] is not None else 0.0,
                "roic": float(row[2]) if row[2] is not None else 0.0
            }
    except Exception as e:
        print(f"[Fundamentals] DB check failed for {ticker}: {e}")
        
    # 2. Only fall back to APIs if the database has nothing
    print(f"[Fundamentals] Falling back to APIs/Yahoo Finance for {ticker}...")
    import re
    result = {'eps': 0.0, 'roic': 0.0, 'trailingPE': None, 'dividend_yield': 0.0}
    
    # Also fetch trailing PE if possible
    pe = _scrape_psx_pe(ticker)
    if pe is not None:
        result["trailingPE"] = pe

    # Option A: Fetch EPS, ROIC, and Dividend Yield from the PSX DPS internal JSON API endpoint
    try:
        url = f"https://dps.psx.com.pk/api/company/{ticker.upper()}"
        from market_data_scraper import fetch_psx_data_robust_sync
        data = fetch_psx_data_robust_sync(url, is_json=True)
        if data:
            if 'eps' in data:
                result['eps'] = float(data['eps'])
            if 'roic' in data:
                result['roic'] = float(data['roic'])
            if 'dividendYield' in data:
                val = data['dividendYield']
                if val:
                    val_str = str(val).strip()
                    is_percentage_str = '%' in val_str
                    val_clean = val_str.replace('%', '').strip()
                    try:
                        parsed_val = float(val_clean)
                        # If it has a '%' or is > 1.0 (e.g. 16.5), we divide by 100 to get a true decimal.
                        # Some APIs return 0.165 directly. We assume values > 1.0 are definitely not already decimals.
                        if is_percentage_str or parsed_val > 1.0:
                            result['dividend_yield'] = parsed_val / 100.0
                        else:
                            # If it's something like 0.165 (already a decimal), we leave it.
                            # If it's a very low yield like 0.5 meaning 0.5%, and has no %, it'll be treated as 50% (0.5),
                            # but Pakistan yields are rarely that low, and if they are, the % sign is usually present.
                            result['dividend_yield'] = parsed_val
                    except ValueError:
                        pass
            return result
    except Exception as e:
        print(f"[Fundamentals] PSX API failed for {ticker}: {e}")

    # Option B: Fallback to PSX Master Screener for Dividend Yield
    try:
        print(f"[Fundamentals] Falling back to Yahoo Finance for {ticker}...")
        result['dividend_yield'] = fetch_dividend_yield(ticker)
        # EPS and ROIC scrapers are deprecated due to Next.js SSR architecture
        return result
    except Exception as e:
        print(f"[Fundamentals] PSX Screener API fallback failed for {ticker}: {e}")
        return result

@functools.lru_cache(maxsize=32)
def get_cached_historical_prices(ticker: str, start_date: str):
    """
    Fetches and caches historical PSX price data.
    NOTE: yfinance is no longer used. This function returns an empty Series
    as a stub. Historical price fetching should be implemented via PSX /timeseries
    endpoints or manual data seeding if needed.
    """
    print(f"[get_cached_historical_prices] yfinance removed. No historical data for {ticker}.")
    return pd.Series(dtype=float)

def fetch_supabase_equity_curve() -> pd.DataFrame:
    """
    Reconstructs the daily market-to-market equity curve.
    Guarantees a pd.DataFrame return with a 'portfolio_value' column.
    """
    try:
        # 1. Fetch transactions from Supabase
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT ticker, action, quantity, transaction_date FROM transactions ORDER BY transaction_date ASC")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        
        if not rows or len(rows) == 0:
            return pd.DataFrame()

        records = []
        for row in rows:
            records.append({'ticker': row[0], 'action': row[1], 'quantity': float(row[2]), 'transaction_date': row[3]})
        
        tx_df = pd.DataFrame(records)
        tx_df['transaction_date'] = pd.to_datetime(tx_df['transaction_date'])
        
        # Determine date range
        start_date = tx_df['transaction_date'].min().strftime('%Y-%m-%d')
        end_date = datetime.datetime.now().strftime('%Y-%m-%d')
        
        # 2. Extract unique tickers and build position ledger
        tickers = tx_df['ticker'].unique().tolist()
        psx_tickers = [f"{t}.KA" if not t.endswith(".KA") else t for t in tickers]
        
        # Create a full daily business day range
        date_index = pd.date_range(start=start_date, end=end_date, freq='B')
        if len(date_index) == 0:
            return pd.DataFrame()

        # Build daily shares balance per ticker
        shares_df = pd.DataFrame(0.0, index=date_index, columns=tickers)
        for _, row in tx_df.iterrows():
            date = row['transaction_date'].normalize()
            qty = row['quantity'] if row['action'].lower() == 'buy' else -row['quantity']
            if date in shares_df.index:
                shares_df.loc[date:, row['ticker']] += qty

        # 3. Download Historical Prices safely
        # Note: If single ticker, yfinance formatting differs
        price_data = yf.download(psx_tickers, start=start_date, end=end_date, progress=False)
        
        if isinstance(price_data, (float, int, np.float64)):
            return pd.DataFrame() 

        if price_data.empty:
            return pd.DataFrame()

        # Extract 'Close' prices
        if 'Close' in price_data:
            prices_df = price_data['Close']
        else:
            prices_df = price_data

        # Normalize column names in prices_df to match tickers (strip '.KA')
        if isinstance(prices_df, pd.DataFrame):
            prices_df.columns = [col.replace('.KA', '') for col in prices_df.columns]
        elif isinstance(prices_df, pd.Series):
            # Handle single ticker case where yfinance returns a Series
            prices_df = prices_df.to_frame(name=tickers[0])

        # Align prices to shares index and forward-fill non-trading days
        prices_df = prices_df.reindex(date_index).ffill().bfill()
        
        # Align columns
        common_cols = [col for col in tickers if col in prices_df.columns]
        if not common_cols:
            return pd.DataFrame()

        # 4. Explicit Vectorized Multiplication across columns (axis=1)
        # Multiplying aligned DataFrames and summing row-by-row (horizontal sum)
        daily_stock_values = (shares_df[common_cols] * prices_df[common_cols]).sum(axis=1)

        # 5. Guarantee DataFrame construction
        equity_curve_df = pd.DataFrame({
            'portfolio_value': daily_stock_values
        }, index=date_index)

        # Drop any initial zero-value days before first purchase
        equity_curve_df = equity_curve_df[equity_curve_df['portfolio_value'] > 0]

        return equity_curve_df

    except Exception as e:
        import traceback
        print(f"Error building equity curve: {e}")
        traceback.print_exc()
        return pd.DataFrame()


# ====================================================================
# PSX INVESTMENT, CAPITAL & 3-STAGE DIVIDEND ACCOUNTING ENGINE
# ====================================================================

REAL_KTRADE_CASH_BALANCE: float = 4357.38
REAL_KTRADE_WORKING_CAPITAL: float = 51446.75
REAL_KTRADE_HOLDINGS: List[Dict[str, float]] = [
    {"ticker": "EFERT", "shares": 8.0, "avg_buy_price": 194.99},
    {"ticker": "FABL", "shares": 5.0, "avg_buy_price": 91.40},
    {"ticker": "FFC", "shares": 15.0, "avg_buy_price": 559.49},
    {"ticker": "HUBC", "shares": 35.0, "avg_buy_price": 193.18},
    {"ticker": "LUCK", "shares": 6.0, "avg_buy_price": 391.38},
    {"ticker": "MARI", "shares": 11.0, "avg_buy_price": 631.53},
    {"ticker": "MEBL", "shares": 2.0, "avg_buy_price": 528.61},
    {"ticker": "OGDC", "shares": 25.0, "avg_buy_price": 306.78},
    {"ticker": "SYS", "shares": 109.0, "avg_buy_price": 109.15},
]

DEFAULT_WATCHLIST_DCF_TARGETS: Dict[str, Dict[str, float]] = {
    "EFERT": {"target_price": 232.00, "stop_loss": 178.00, "fair_pe": 8.5},
    "FABL": {"target_price": 112.00, "stop_loss": 82.50, "fair_pe": 5.5},
    "FFC": {"target_price": 645.00, "stop_loss": 510.00, "fair_pe": 9.0},
    "HUBC": {"target_price": 235.00, "stop_loss": 175.00, "fair_pe": 6.5},
    "LUCK": {"target_price": 465.00, "stop_loss": 355.00, "fair_pe": 10.5},
    "MARI": {"target_price": 740.00, "stop_loss": 575.00, "fair_pe": 8.0},
    "MEBL": {"target_price": 615.00, "stop_loss": 482.00, "fair_pe": 7.5},
    "OGDC": {"target_price": 368.00, "stop_loss": 278.00, "fair_pe": 6.5},
    "SYS": {"target_price": 138.00, "stop_loss": 98.50, "fair_pe": 15.0},
}


def record_capital_transaction(tx_type: str, amount: float, notes: str = None, reference_note: str = None) -> dict:
    """
    Records cash moving in/out of KTrade & Bank in capital_ledger (single source of truth):
    - DEPOSIT_TO_KTRADE (Fresh capital deposited to KTrade)
    - WITHDRAWAL_FROM_KTRADE (Cash withdrawn from KTrade to personal bank)
    - DIVIDEND_SENT_TO_KTRADE (Reinvested dividend moved from bank account to KTrade)
    - FEE_ADJUSTMENT (Broker fee / withholding / trade cash flow adjustment)
    """
    import uuid
    type_map = {
        'DEPOSIT_TO_BROKER': 'DEPOSIT_TO_KTRADE',
        'WITHDRAWAL_FROM_BROKER': 'WITHDRAWAL_FROM_KTRADE',
        'DIVIDEND_BANK_TO_BROKER': 'DIVIDEND_SENT_TO_KTRADE',
        'DEPOSIT_TO_KTRADE': 'DEPOSIT_TO_KTRADE',
        'WITHDRAWAL_FROM_KTRADE': 'WITHDRAWAL_FROM_KTRADE',
        'DIVIDEND_SENT_TO_KTRADE': 'DIVIDEND_SENT_TO_KTRADE',
        'FEE_ADJUSTMENT': 'FEE_ADJUSTMENT'
    }
    raw_type = tx_type.strip().upper()
    if raw_type not in type_map:
        raise ValueError(f"Invalid tx_type: {tx_type}. Must be one of {set(type_map.values())}")
    clean_type = type_map[raw_type]

    note_text = notes if notes is not None else reference_note
    amt = float(amount)
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO capital_ledger (tx_type, amount, notes, reference_note, created_at, tx_date)
            VALUES (%s, %s, %s, %s, NOW(), NOW())
            RETURNING id, tx_type, amount, notes, created_at;
        """, (clean_type, amt, note_text, note_text))
        row = cur.fetchone()

        # Best-effort mirror to legacy cash_ledger inside a savepoint so missing legacy tables never break transactions
        if clean_type in {'DEPOSIT_TO_KTRADE', 'DIVIDEND_SENT_TO_KTRADE'}:
            cash_delta = amt
        elif clean_type == 'WITHDRAWAL_FROM_KTRADE':
            cash_delta = -amt
        else:
            cash_delta = amt

        try:
            cur.execute("SAVEPOINT sp_legacy_cash;")
            user_id = os.getenv("DISCORD_USER_ID", "1536522727287164979")
            cur.execute("""
                INSERT INTO cash_ledger (transaction_id, discord_user_id, transaction_type, amount, created_at)
                VALUES (%s, %s, %s, %s, NOW())
            """, (str(uuid.uuid4()), user_id, clean_type, cash_delta))
            cur.execute("RELEASE SAVEPOINT sp_legacy_cash;")
        except Exception:
            cur.execute("ROLLBACK TO SAVEPOINT sp_legacy_cash;")

        conn.commit()
        invalidate_financial_summary_cache()
        return {
            "id": row[0],
            "tx_type": row[1],
            "amount": float(row[2]),
            "notes": row[3],
            "reference_note": row[3],
            "created_at": row[4].isoformat() if row[4] else None,
            "tx_date": row[4].isoformat() if row[4] else None
        }
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

def record_dividend(
    ticker: str,
    shares_held: float = None,
    dps_gross: float = 0.0,
    gross_amount: float = None,
    tax_rate_pct: float = 15.00,
    tax_withheld: float = None,
    net_amount: float = None,
    ex_date: str = None,
    payment_date: str = None,
    status: str = 'CREDITED_TO_BANK',
    notes: str = None
) -> dict:
    """
    Tracks the exact PSX Dividend Lifecycle:
    ENTITLED (Pending Payout) -> CREDITED_TO_BANK (CDC Dividend Credited into Personal Bank Account) -> SENT_TO_KTRADE (Reinvested in KTrade)

    CRITICAL RULE:
    A dividend in 'ENTITLED' or 'CREDITED_TO_BANK' does NOT increase KTrade cash.
    Only when marked 'SENT_TO_KTRADE' does it insert a 'DIVIDEND_SENT_TO_KTRADE' row in capital_ledger and increase KTrade available cash!
    """
    from utils import normalize_ticker
    clean_ticker = normalize_ticker(ticker)
    
    # Auto-lookup shares from portfolio_holdings if not provided
    if shares_held is None or float(shares_held) <= 0:
        conn_lookup = get_db_connection()
        cur_lookup = conn_lookup.cursor()
        try:
            cur_lookup.execute("SELECT shares FROM portfolio_holdings WHERE ticker = %s;", (clean_ticker,))
            res = cur_lookup.fetchone()
            shares = float(res[0]) if res and res[0] else 0.0
        except Exception:
            shares = 0.0
        finally:
            cur_lookup.close()
            conn_lookup.close()
    else:
        shares = float(shares_held)

    dps = float(dps_gross) if dps_gross is not None else 0.0
    tax_rate = float(tax_rate_pct) if tax_rate_pct is not None else 15.00
    
    # Financial math resolution
    if gross_amount is None:
        if shares > 0 and dps > 0:
            gross_amount = round(shares * dps, 2)
        elif net_amount is not None:
            gross_amount = round(float(net_amount) / (1.0 - (tax_rate / 100.0)), 2) if tax_rate < 100 else float(net_amount)
        else:
            gross_amount = 0.0
    else:
        gross_amount = round(float(gross_amount), 2)

    if tax_withheld is None:
        tax_withheld = round(gross_amount * (tax_rate / 100.0), 2)
    else:
        tax_withheld = round(float(tax_withheld), 2)

    if net_amount is None:
        net_amount = round(gross_amount - tax_withheld, 2)
    else:
        net_amount = round(float(net_amount), 2)

    clean_status = status.strip().upper() if status else 'CREDITED_TO_BANK'
    valid_statuses = {'ENTITLED', 'ANNOUNCED', 'CREDITED_TO_BANK', 'SENT_TO_KTRADE'}
    if clean_status not in valid_statuses:
        clean_status = 'CREDITED_TO_BANK'

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO dividend_ledger (
                ticker, shares_held, dps_gross, gross_amount, tax_rate_pct,
                tax_withheld, net_amount, ex_date, payment_date, status, notes, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            RETURNING id, ticker, shares_held, dps_gross, gross_amount, tax_rate_pct,
                      tax_withheld, net_amount, ex_date, payment_date, status, notes, created_at;
        """, (
            clean_ticker, shares, dps, gross_amount, tax_rate,
            tax_withheld, net_amount, ex_date, payment_date, clean_status, notes
        ))
        row = cur.fetchone()
        conn.commit()
        invalidate_financial_summary_cache()
        
        # CRITICAL RULE: Only if status is 'SENT_TO_KTRADE', credit capital_ledger immediately!
        if clean_status == 'SENT_TO_KTRADE':
            record_capital_transaction(
                tx_type='DIVIDEND_SENT_TO_KTRADE',
                amount=net_amount,
                notes=f"Reinvested Dividend #{row[0]}: {clean_ticker} ({shares} shs @ Rs. {dps})"
            )

        return {
            "id": row[0],
            "ticker": row[1],
            "shares_held": float(row[2]),
            "dps_gross": float(row[3]),
            "gross_amount": float(row[4]),
            "tax_rate_pct": float(row[5]),
            "tax_withheld": float(row[6]),
            "net_amount": float(row[7]),
            "ex_date": str(row[8]) if row[8] else None,
            "payment_date": str(row[9]) if row[9] else None,
            "status": row[10],
            "notes": row[11],
            "created_at": row[12].isoformat() if row[12] else None
        }
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

def transfer_dividend_to_ktrade(dividend_id_or_ticker) -> dict:
    """
    Transitions a dividend to SENT_TO_KTRADE, logs DIVIDEND_SENT_TO_KTRADE in capital_ledger,
    and increments KTrade available cash.
    Accepts either an integer ID or ticker string (e.g. 'MEBL', 'HUBC').
    """
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # Determine if target is ID or ticker
        is_id = False
        target_id = None
        target_ticker = None

        if isinstance(dividend_id_or_ticker, int) or (isinstance(dividend_id_or_ticker, str) and dividend_id_or_ticker.isdigit()):
            is_id = True
            target_id = int(dividend_id_or_ticker)
        else:
            target_ticker = str(dividend_id_or_ticker).strip().upper()

        if is_id:
            cur.execute("""
                SELECT id, ticker, shares_held, dps_gross, net_amount, status
                FROM dividend_ledger
                WHERE id = %s
            """, (target_id,))
        else:
            cur.execute("""
                SELECT id, ticker, shares_held, dps_gross, net_amount, status
                FROM dividend_ledger
                WHERE ticker = %s AND status IN ('CREDITED_TO_BANK', 'ENTITLED')
                ORDER BY created_at DESC
                LIMIT 1;
            """, (target_ticker,))

        existing = cur.fetchone()
        if not existing:
            raise ValueError(f"No eligible dividend found for '{dividend_id_or_ticker}' to transfer to KTrade.")

        div_id = existing[0]
        ticker = existing[1]
        shares = float(existing[2])
        dps = float(existing[3])
        net_amount = float(existing[4])
        old_status = existing[5]

        if old_status == 'SENT_TO_KTRADE':
            # Already transferred
            return {
                "id": div_id,
                "ticker": ticker,
                "status": "SENT_TO_KTRADE",
                "net_amount": net_amount,
                "message": "Dividend was already transferred to KTrade."
            }

        cur.execute("""
            UPDATE dividend_ledger
            SET status = 'SENT_TO_KTRADE'
            WHERE id = %s
            RETURNING id, ticker, shares_held, dps_gross, gross_amount, tax_rate_pct,
                      tax_withheld, net_amount, ex_date, payment_date, status, notes, created_at;
        """, (div_id,))
        row = cur.fetchone()
        conn.commit()

        # Log in capital_ledger & increment KTrade cash
        cap_entry = record_capital_transaction(
            tx_type='DIVIDEND_SENT_TO_KTRADE',
            amount=net_amount,
            notes=f"Reinvested Dividend #{div_id}: {ticker} ({shares} shs @ Rs. {dps})"
        )
        print(f"💰 [Dividend Pipeline] Dividend #{div_id} ({ticker}) marked SENT_TO_KTRADE. Added Rs. {net_amount} to KTrade Available Cash.")

        return {
            "id": row[0],
            "ticker": row[1],
            "shares_held": float(row[2]),
            "dps_gross": float(row[3]),
            "gross_amount": float(row[4]),
            "tax_rate_pct": float(row[5]),
            "tax_withheld": float(row[6]),
            "net_amount": float(row[7]),
            "ex_date": str(row[8]) if row[8] else None,
            "payment_date": str(row[9]) if row[9] else None,
            "status": row[10],
            "notes": row[11],
            "created_at": row[12].isoformat() if row[12] else None,
            "capital_transaction": cap_entry
        }
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

def update_dividend_status(dividend_id: int, new_status: str) -> dict:
    """
    Transitions dividend across lifecycle states.
    If new_status is 'SENT_TO_KTRADE', routes directly through transfer_dividend_to_ktrade()!
    """
    clean_status = new_status.strip().upper()
    if clean_status == 'SENT_TO_KTRADE':
        return transfer_dividend_to_ktrade(dividend_id)

    valid_statuses = {'ANNOUNCED', 'ENTITLED', 'CREDITED_TO_BANK', 'SENT_TO_KTRADE'}
    if clean_status not in valid_statuses:
        raise ValueError(f"Invalid dividend status: {new_status}. Must be one of {valid_statuses}")

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE dividend_ledger
            SET status = %s
            WHERE id = %s
            RETURNING id, ticker, shares_held, dps_gross, gross_amount, tax_rate_pct,
                      tax_withheld, net_amount, ex_date, payment_date, status, notes, created_at;
        """, (clean_status, dividend_id))
        row = cur.fetchone()
        if not row:
            raise ValueError(f"Dividend #{dividend_id} not found.")
        conn.commit()
        invalidate_financial_summary_cache()

        return {
            "id": row[0],
            "ticker": row[1],
            "shares_held": float(row[2]),
            "dps_gross": float(row[3]),
            "gross_amount": float(row[4]),
            "tax_rate_pct": float(row[5]),
            "tax_withheld": float(row[6]),
            "net_amount": float(row[7]),
            "ex_date": str(row[8]) if row[8] else None,
            "payment_date": str(row[9]) if row[9] else None,
            "status": row[10],
            "notes": row[11],
            "created_at": row[12].isoformat() if row[12] else None
        }
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

def record_trade_execution(
    ticker: str,
    side: str,
    quantity: float,
    price: float,
    fees: float = 0.00,
    notes: str = None,
    fees_and_tax: float = None
) -> dict:
    """
    Logs BUY / SELL trade in trade_history, updates portfolio_holdings weighted average cost,
    and reflects trade cash flow in capital_ledger.
    """
    from utils import normalize_ticker
    clean_ticker = normalize_ticker(ticker)
    clean_side = side.strip().upper()
    if clean_side not in {'BUY', 'SELL'}:
        raise ValueError("Trade side must be 'BUY' or 'SELL'.")

    qty = float(quantity)
    prc = float(price)
    resolved_fees = fees_and_tax if fees_and_tax is not None else fees
    f_val = float(resolved_fees) if resolved_fees is not None else 0.00
    gross = round(qty * prc, 2)
    net_total = round(gross + f_val if clean_side == 'BUY' else gross - f_val, 2)

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # 1. Insert into trade_history
        cur.execute("""
            INSERT INTO trade_history (ticker, side, quantity, price, fees, fees_and_tax, net_total, executed_at, notes)
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), %s)
            RETURNING id, ticker, side, quantity, price, fees, net_total, executed_at, notes;
        """, (clean_ticker, clean_side, qty, prc, f_val, f_val, net_total, notes))
        t_row = cur.fetchone()

        # 2. Update portfolio_holdings
        cur.execute("""
            SELECT id, shares, avg_buy_price FROM portfolio_holdings WHERE ticker = %s
        """, (clean_ticker,))
        existing_h = cur.fetchone()

        if clean_side == 'BUY':
            if existing_h:
                old_shares = float(existing_h[1])
                old_avg = float(existing_h[2])
                new_shares = old_shares + qty
                new_avg = round(((old_shares * old_avg) + (qty * prc)) / new_shares, 4) if new_shares > 0 else prc
                cur.execute("""
                    UPDATE portfolio_holdings
                    SET shares = %s,
                        avg_buy_price = %s,
                        last_price = COALESCE(NULLIF(last_price, 0), %s),
                        updated_at = NOW()
                    WHERE id = %s
                """, (new_shares, new_avg, prc, existing_h[0]))
            else:
                cur.execute("""
                    INSERT INTO portfolio_holdings (ticker, shares, avg_buy_price, last_price, updated_at)
                    VALUES (%s, %s, %s, %s, NOW())
                """, (clean_ticker, qty, prc, prc))

            # Cash flow deduction in unified capital_ledger
            cash_note = f"Trade Execution: BUY {qty:g} {clean_ticker} @ {prc}"
            cur.execute("""
                INSERT INTO capital_ledger (tx_type, amount, notes, reference_note, created_at, tx_date)
                VALUES ('FEE_ADJUSTMENT', %s, %s, %s, NOW(), NOW());
            """, (-net_total, cash_note, cash_note))

        elif clean_side == 'SELL':
            if existing_h:
                old_shares = float(existing_h[1])
                new_shares = max(0.0, old_shares - qty)
                cur.execute("""
                    UPDATE portfolio_holdings
                    SET shares = %s, updated_at = NOW()
                    WHERE id = %s
                """, (new_shares, existing_h[0]))

            # Cash flow credit in unified capital_ledger
            cash_note = f"Trade Execution: SELL {qty:g} {clean_ticker} @ {prc}"
            cur.execute("""
                INSERT INTO capital_ledger (tx_type, amount, notes, reference_note, created_at, tx_date)
                VALUES ('FEE_ADJUSTMENT', %s, %s, %s, NOW(), NOW());
            """, (net_total, cash_note, cash_note))

        conn.commit()
        invalidate_financial_summary_cache()
        return {
            "id": t_row[0],
            "ticker": t_row[1],
            "side": t_row[2],
            "quantity": float(t_row[3]),
            "price": float(t_row[4]),
            "fees": float(t_row[5]),
            "net_total": float(t_row[6]),
            "executed_at": t_row[7].isoformat() if t_row[7] else None,
            "notes": t_row[8]
        }
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()

# Alias for backward compatibility
record_trade = record_trade_execution


def sync_full_portfolio_state(
    cash_balance: float,
    holdings: list[dict],
    total_working_capital: float = None
) -> dict:
    """
    Atomically resets mock/stale ledger rows across portfolio_holdings, trade_history,
    capital_ledger, and dividend_ledger, sets exact KTrade holdings (ticker, shares, avg_buy_price),
    and records exact cash balance so ktrade_available_cash == cash_balance.
    """
    from utils import normalize_ticker
    import uuid

    clean_cash = round(float(cash_balance or 0.0), 2)
    sanitized_holdings = []
    total_cost_basis = 0.0

    for item in (holdings or []):
        if not isinstance(item, dict):
            continue
        raw_sym = item.get("ticker") or item.get("symbol")
        if not raw_sym:
            continue
        sym = normalize_ticker(str(raw_sym))
        shs = float(item.get("shares") if item.get("shares") is not None else item.get("quantity", 0))
        avg_p = float(
            item.get("avg_buy_price")
            if item.get("avg_buy_price") is not None
            else item.get("avg_price", item.get("price", 0))
        )
        if sym and shs > 0 and avg_p > 0:
            sanitized_holdings.append({
                "ticker": sym,
                "shares": shs,
                "avg_buy_price": round(avg_p, 4),
            })
            total_cost_basis += round(shs * avg_p, 2)

    total_cost_basis = round(total_cost_basis, 2)
    if total_working_capital is not None and float(total_working_capital) > 0:
        working_cap = round(float(total_working_capital), 2)
    else:
        working_cap = round(total_cost_basis + clean_cash, 2)

    deployed_adjustment = round(clean_cash - working_cap, 2)

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # Preserve any recently scraped spot prices so cached view stays accurate immediately
        existing_spot_map = {}
        try:
            cur.execute("SAVEPOINT sp_read_spot;")
            cur.execute("SELECT ticker, last_price, last_change_pct, price_updated_at FROM portfolio_holdings;")
            for r in cur.fetchall():
                if r[0] and r[1] and float(r[1]) > 0:
                    existing_spot_map[str(r[0]).upper()] = (float(r[1]), float(r[2] or 0.0), r[3])
            cur.execute("RELEASE SAVEPOINT sp_read_spot;")
        except Exception:
            cur.execute("ROLLBACK TO SAVEPOINT sp_read_spot;")

        # 1. Atomically wipe stale/mock rows from all 4 core tables (+ legacy cash_ledger if present)
        cur.execute("DELETE FROM portfolio_holdings;")
        cur.execute("DELETE FROM trade_history;")
        cur.execute("DELETE FROM capital_ledger;")
        cur.execute("DELETE FROM dividend_ledger;")

        try:
            cur.execute("SAVEPOINT sp_clear_legacy_cash;")
            cur.execute("DELETE FROM cash_ledger;")
            cur.execute("RELEASE SAVEPOINT sp_clear_legacy_cash;")
        except Exception:
            cur.execute("ROLLBACK TO SAVEPOINT sp_clear_legacy_cash;")

        # 2. Insert exact KTrade holdings + baseline trade_history + watchlist DCF defaults
        for h in sanitized_holdings:
            sym = h["ticker"]
            shs = h["shares"]
            avg_p = h["avg_buy_price"]
            prev_spot = existing_spot_map.get(sym)
            init_spot = prev_spot[0] if prev_spot else avg_p
            init_chg = prev_spot[1] if prev_spot else 0.0
            init_ts = prev_spot[2] if prev_spot else None
            gross_cost = round(shs * avg_p, 2)

            cur.execute("""
                INSERT INTO portfolio_holdings (
                    ticker, shares, avg_buy_price, last_price, last_change_pct, price_updated_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, COALESCE(%s, NOW()), NOW())
                ON CONFLICT (ticker) DO UPDATE
                SET shares = EXCLUDED.shares,
                    avg_buy_price = EXCLUDED.avg_buy_price,
                    last_price = EXCLUDED.last_price,
                    last_change_pct = EXCLUDED.last_change_pct,
                    price_updated_at = EXCLUDED.price_updated_at,
                    updated_at = NOW();
            """, (sym, shs, avg_p, init_spot, init_chg, init_ts))

            cur.execute("""
                INSERT INTO trade_history (
                    ticker, side, quantity, price, fees, fees_and_tax, net_total, executed_at, notes
                )
                VALUES (%s, 'BUY', %s, %s, 0.00, 0.00, %s, NOW(), 'KTrade Custody Baseline Sync');
            """, (sym, shs, avg_p, gross_cost))

            dcf_defaults = DEFAULT_WATCHLIST_DCF_TARGETS.get(sym)
            default_tp = dcf_defaults["target_price"] if dcf_defaults else round(avg_p * 1.18, 2)
            default_sl = dcf_defaults["stop_loss"] if dcf_defaults else round(avg_p * 0.91, 2)
            default_fpe = dcf_defaults["fair_pe"] if dcf_defaults else 8.5
            try:
                cur.execute("SAVEPOINT sp_watchlist_seed;")
                cur.execute("""
                    INSERT INTO watchlist_targets (ticker, target_price, stop_loss, historical_fair_pe)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (ticker) DO NOTHING;
                """, (sym, default_tp, default_sl, default_fpe))
                cur.execute("RELEASE SAVEPOINT sp_watchlist_seed;")
            except Exception:
                cur.execute("ROLLBACK TO SAVEPOINT sp_watchlist_seed;")

        # 3. Record working capital & deployed adjustment so ktrade_available_cash == clean_cash exactly
        cur.execute("""
            INSERT INTO capital_ledger (tx_type, amount, notes, reference_note, created_at, tx_date)
            VALUES ('DEPOSIT_TO_KTRADE', %s, %s, %s, NOW(), NOW());
        """, (
            working_cap,
            "KTrade Working Capital Sync",
            "KTrade Working Capital Sync",
        ))
        if abs(deployed_adjustment) > 0.0001:
            cur.execute("""
                INSERT INTO capital_ledger (tx_type, amount, notes, reference_note, created_at, tx_date)
                VALUES ('FEE_ADJUSTMENT', %s, %s, %s, NOW(), NOW());
            """, (
                deployed_adjustment,
                "Deployed Equity Cost Basis & Settlement Sync",
                "Deployed Equity Cost Basis & Settlement Sync",
            ))

        # Mirror exact liquid cash into legacy cash_ledger if table exists
        try:
            cur.execute("SAVEPOINT sp_seed_legacy_cash;")
            user_id = os.getenv("DISCORD_USER_ID", "1536522727287164979")
            cur.execute("""
                INSERT INTO cash_ledger (transaction_id, discord_user_id, transaction_type, amount, created_at)
                VALUES (%s, %s, 'SYNC_PORTFOLIO', %s, NOW());
            """, (str(uuid.uuid4()), user_id, clean_cash))
            cur.execute("RELEASE SAVEPOINT sp_seed_legacy_cash;")
        except Exception:
            cur.execute("ROLLBACK TO SAVEPOINT sp_seed_legacy_cash;")

        # Mark sync checkpoint so startup knows real KTrade book is active
        try:
            cur.execute("SAVEPOINT sp_sync_ckpt;")
            cur.execute("""
                INSERT INTO agent_checkpoints (key, value, updated_at)
                VALUES ('ktrade_real_book_synced_v1', %s, NOW())
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW();
            """, (str(int(datetime.datetime.now().timestamp())),))
            cur.execute("RELEASE SAVEPOINT sp_sync_ckpt;")
        except Exception:
            cur.execute("ROLLBACK TO SAVEPOINT sp_sync_ckpt;")

        conn.commit()
        invalidate_financial_summary_cache()
        return {
            "status": "success",
            "holdings_synced": len(sanitized_holdings),
            "ktrade_available_cash": clean_cash,
            "fresh_capital_invested": working_cap,
            "portfolio_cost_basis": total_cost_basis,
            "tickers": [h["ticker"] for h in sanitized_holdings],
        }
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()


def ensure_real_ktrade_portfolio_seeded() -> bool:
    """
    Detects empty or mock portfolio rows on boot and seeds the user's real 9-position KTrade book
    (Cash: 4,357.38 PKR, Working Capital: 51,446.75 PKR) deterministically.
    """
    real_tickers = {h["ticker"] for h in REAL_KTRADE_HOLDINGS}
    known_mock_tickers = {"AAPL", "TSLA", "MSFT", "TEST", "MOCK", "DEMO", "XYZ"}

    conn = get_db_connection()
    cur = conn.cursor()
    needs_seed = False
    try:
        cur.execute("SELECT value FROM agent_checkpoints WHERE key = 'ktrade_real_book_synced_v1';")
        already_synced = cur.fetchone() is not None

        cur.execute("SELECT ticker, shares FROM portfolio_holdings WHERE shares > 0;")
        rows = cur.fetchall()
        current_tickers = {str(r[0]).upper() for r in rows if r[0]}

        if not current_tickers:
            needs_seed = True
        elif current_tickers & known_mock_tickers:
            needs_seed = True
        elif not already_synced and current_tickers != real_tickers:
            needs_seed = True
    except Exception as e:
        print(f"⚠️ [Portfolio Seed Check] {e}", flush=True)
        return False
    finally:
        cur.close()
        conn.close()

    if needs_seed:
        print("🔄 [KTrade Sync] Mock or empty portfolio detected — seeding verified 9-position KTrade book...", flush=True)
        sync_full_portfolio_state(
            cash_balance=REAL_KTRADE_CASH_BALANCE,
            holdings=REAL_KTRADE_HOLDINGS,
            total_working_capital=REAL_KTRADE_WORKING_CAPITAL,
        )
        return True
    return False


def refresh_portfolio_live_prices() -> dict:
    """
    Background sweep helper: scrapes live PSX spot prices & daily % changes in parallel
    for all active holdings and persists last_price, last_change_pct, price_updated_at in portfolio_holdings.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT ticker, avg_buy_price FROM portfolio_holdings WHERE shares > 0;")
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    if not rows:
        return {"updated": 0, "prices": {}}

    def _scrape_one(sym: str, fallback_p: float):
        try:
            cp, chg = get_live_price_and_change(sym)
            if cp and cp > 0:
                return float(cp), float(chg or 0.0), True
        except Exception:
            pass
        return float(fallback_p), 0.0, False

    scraped: Dict[str, tuple[float, float, bool]] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(rows), 10)) as executor:
        fut_map = {executor.submit(_scrape_one, str(r[0]).upper(), float(r[1])): str(r[0]).upper() for r in rows}
        for fut in concurrent.futures.as_completed(fut_map):
            sym = fut_map[fut]
            try:
                scraped[sym] = fut.result()
            except Exception:
                scraped[sym] = (0.0, 0.0, False)

    w_conn = get_db_connection()
    w_cur = w_conn.cursor()
    updated = 0
    try:
        for sym, (cp, chg, ok) in scraped.items():
            if cp > 0:
                w_cur.execute("""
                    UPDATE portfolio_holdings
                    SET last_price = %s,
                        last_change_pct = %s,
                        price_updated_at = NOW()
                    WHERE UPPER(ticker) = %s;
                """, (cp, chg, sym))
                if ok:
                    updated += 1
        w_conn.commit()
        invalidate_financial_summary_cache()
    except Exception:
        w_conn.rollback()
    finally:
        w_cur.close()
        w_conn.close()

    return {
        "updated": updated,
        "prices": {k: {"price": v[0], "change_pct": v[1]} for k, v in scraped.items()},
    }


def get_complete_financial_summary(use_cached_prices: bool = False) -> dict:
    """
    Computes complete PSX investment summary, capital accounting, and mark-to-market valuations.
    - Single source of truth: capital_ledger (cash + working capital), portfolio_holdings, dividend_ledger.
    - When use_cached_prices=True: uses last_price stored in portfolio_holdings (<150ms response, 0 HTTP calls).
    - When use_cached_prices=False: scrapes live PSX prices in parallel and updates portfolio_holdings.last_price.
    """
    global _CACHED_FIN_SUMMARY, _CACHED_FIN_SUMMARY_TS
    if use_cached_prices and _CACHED_FIN_SUMMARY is not None and (_time.time() - _CACHED_FIN_SUMMARY_TS) < 20.0:
        return _CACHED_FIN_SUMMARY

    from learning_engine import PSX_SECTOR_MAP

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # 1. Unified Capital & Available Cash Summary from capital_ledger
        cur.execute("""
            SELECT
                COALESCE(SUM(CASE
                    WHEN tx_type IN ('DEPOSIT_TO_KTRADE', 'DEPOSIT_TO_BROKER') THEN amount
                    WHEN tx_type IN ('WITHDRAWAL_FROM_KTRADE', 'WITHDRAWAL_FROM_BROKER') THEN -amount
                    ELSE 0
                END), 0) AS fresh_capital,
                COALESCE(SUM(CASE
                    WHEN tx_type IN ('DIVIDEND_SENT_TO_KTRADE', 'DIVIDEND_BANK_TO_BROKER') THEN amount
                    ELSE 0
                END), 0) AS reinvested_divs,
                COALESCE(SUM(CASE
                    WHEN tx_type IN ('DEPOSIT_TO_KTRADE', 'DEPOSIT_TO_BROKER', 'DIVIDEND_SENT_TO_KTRADE', 'DIVIDEND_BANK_TO_BROKER') THEN amount
                    WHEN tx_type IN ('WITHDRAWAL_FROM_KTRADE', 'WITHDRAWAL_FROM_BROKER') THEN -amount
                    WHEN tx_type = 'FEE_ADJUSTMENT' THEN amount
                    ELSE 0
                END), 0) AS available_cash
            FROM capital_ledger;
        """)
        cap_row = cur.fetchone()
        fresh_capital = float(cap_row[0]) if cap_row else 0.0
        reinvested_divs = float(cap_row[1]) if cap_row else 0.0
        available_cash = float(cap_row[2]) if cap_row else 0.0

        # 2. Dividend Ledger Summary
        cur.execute("""
            SELECT
                COALESCE(SUM(gross_amount), 0) AS total_gross,
                COALESCE(SUM(tax_withheld), 0) AS total_tax,
                COALESCE(SUM(CASE WHEN status = 'CREDITED_TO_BANK' THEN net_amount ELSE 0 END), 0) AS in_bank,
                COALESCE(SUM(CASE WHEN status = 'SENT_TO_KTRADE' THEN net_amount ELSE 0 END), 0) AS reinvested,
                COALESCE(SUM(CASE WHEN status IN ('ANNOUNCED', 'ENTITLED') THEN net_amount ELSE 0 END), 0) AS pending
            FROM dividend_ledger;
        """)
        div_row = cur.fetchone()
        total_gross_div = float(div_row[0]) if div_row else 0.0
        total_tax_wht = float(div_row[1]) if div_row else 0.0
        net_div_bank = float(div_row[2]) if div_row else 0.0
        net_div_reinvested = float(div_row[3]) if div_row else 0.0
        pending_div = float(div_row[4]) if div_row else 0.0

        # 3. Portfolio Holdings + Watchlist Valuation Targets (single query)
        cur.execute("""
            SELECT
                p.ticker,
                p.shares,
                p.avg_buy_price,
                p.dividend_yield,
                p.research_sentiment,
                p.last_price,
                p.last_change_pct,
                p.price_updated_at,
                w.target_price,
                w.stop_loss,
                w.historical_fair_pe,
                w.current_trailing_pe
            FROM portfolio_holdings p
            LEFT JOIN watchlist_targets w ON UPPER(p.ticker) = UPPER(w.ticker)
            WHERE p.shares > 0
            ORDER BY (p.shares * COALESCE(NULLIF(p.last_price, 0), p.avg_buy_price)) DESC;
        """)
        holdings_rows = cur.fetchall()

        # 4. Fetch top active_playbook rules for per-stock HUD mapping
        playbook_rules = []
        try:
            cur.execute("SAVEPOINT sp_summary_playbook;")
            cur.execute("""
                SELECT source, rule, COALESCE(confidence_weight, 0.70)
                FROM active_playbook
                ORDER BY COALESCE(confidence_weight, 0.70) DESC, created_at DESC
                LIMIT 12;
            """)
            playbook_rules = [(str(r[0] or ""), str(r[1] or ""), float(r[2] or 0.70)) for r in cur.fetchall()]
            cur.execute("RELEASE SAVEPOINT sp_summary_playbook;")
        except Exception:
            cur.execute("ROLLBACK TO SAVEPOINT sp_summary_playbook;")

        # Load KMI-30 Shariah status
        kmi_set = set()
        kmi_cache = os.path.join(os.path.dirname(__file__), "kmi30_cache.json")
        if os.path.exists(kmi_cache):
            try:
                with open(kmi_cache, "r", encoding="utf-8") as f:
                    kmi_data = json.load(f)
                    if isinstance(kmi_data, list):
                        kmi_set = set(kmi_data)
                    elif isinstance(kmi_data, dict):
                        kmi_set = set(kmi_data.get("tickers", []))
            except Exception:
                pass
        if not kmi_set:
            kmi_set = {"SYS", "HUBC", "LUCK", "FFC", "EFERT", "MEBL", "MARI", "OGDC", "PPL", "POL", "FABL"}

        price_map: Dict[str, tuple[float, float]] = {}
        if use_cached_prices:
            for row in holdings_rows:
                sym = str(row[0]).upper()
                avg_p = float(row[2])
                cached_p = float(row[5]) if row[5] is not None and float(row[5]) > 0 else avg_p
                cached_chg = float(row[6]) if row[6] is not None else 0.0
                price_map[sym] = (cached_p, cached_chg)
        else:
            def _fetch_price_item(sym, default_p):
                try:
                    cp, chg = get_live_price_and_change(sym)
                    if cp and cp > 0:
                        return float(cp), float(chg or 0.0), True
                except Exception:
                    pass
                return float(default_p), 0.0, False

            scraped_updates = []
            if holdings_rows:
                with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(holdings_rows), 10)) as executor:
                    future_map = {
                        executor.submit(
                            _fetch_price_item,
                            str(r[0]).upper(),
                            float(r[5]) if r[5] is not None and float(r[5]) > 0 else float(r[2]),
                        ): str(r[0]).upper()
                        for r in holdings_rows
                    }
                    for fut in concurrent.futures.as_completed(future_map):
                        ticker_name = future_map[fut]
                        try:
                            cp, chg, was_live = fut.result()
                            price_map[ticker_name] = (cp, chg)
                            if was_live and cp > 0:
                                scraped_updates.append((cp, chg, ticker_name))
                        except Exception:
                            price_map[ticker_name] = (0.0, 0.0)

            if scraped_updates:
                try:
                    cur.execute("SAVEPOINT sp_persist_spot;")
                    for cp, chg, sym in scraped_updates:
                        cur.execute("""
                            UPDATE portfolio_holdings
                            SET last_price = %s, last_change_pct = %s, price_updated_at = NOW()
                            WHERE UPPER(ticker) = %s;
                        """, (cp, chg, sym))
                    cur.execute("RELEASE SAVEPOINT sp_persist_spot;")
                    conn.commit()
                except Exception:
                    cur.execute("ROLLBACK TO SAVEPOINT sp_persist_spot;")

        portfolio_market_val = 0.0
        portfolio_cost = 0.0
        holdings_detail = []
        sector_totals: Dict[str, float] = {}

        def _match_playbook_rule(sym: str, sector_name: str) -> str:
            sym_u = sym.upper()
            for _, r_txt, _ in playbook_rules:
                if sym_u in r_txt.upper() or sector_name.split()[0].upper() in r_txt.upper():
                    return r_txt
            if playbook_rules:
                return playbook_rules[0][1]
            return "Require >= 20% DCF Margin of Safety before deploying incremental KTrade cash; enforce 30% sector cap."

        for row in holdings_rows:
            tick = str(row[0]).upper()
            shares = float(row[1])
            avg_price = float(row[2])
            div_yield = float(row[3]) if row[3] is not None else None
            sentiment = row[4] or "NEUTRAL"
            price_updated_at = row[7].isoformat() if row[7] else None

            defaults = DEFAULT_WATCHLIST_DCF_TARGETS.get(tick, {})
            dcf_fair_val = float(row[8]) if row[8] is not None and float(row[8]) > 0 else float(defaults.get("target_price") or round(avg_price * 1.18, 2))
            stop_loss_val = float(row[9]) if row[9] is not None and float(row[9]) > 0 else float(defaults.get("stop_loss") or round(avg_price * 0.91, 2))
            fair_pe = float(row[10]) if row[10] is not None else defaults.get("fair_pe")
            trailing_pe = float(row[11]) if row[11] is not None else None

            curr_price, change_pct = price_map.get(tick, (avg_price, 0.0))
            if curr_price <= 0:
                curr_price = avg_price

            mv = round(shares * curr_price, 2)
            cb = round(shares * avg_price, 2)
            pnl = round(mv - cb, 2)
            pnl_pct = round((pnl / cb * 100), 2) if cb > 0 else 0.0

            mos_pct = round(((dcf_fair_val - curr_price) / dcf_fair_val) * 100.0, 2) if dcf_fair_val > 0 else 0.0
            stop_dist_pct = round(((curr_price - stop_loss_val) / curr_price) * 100.0, 2) if curr_price > 0 and stop_loss_val > 0 else 0.0
            sector = PSX_SECTOR_MAP.get(tick, "PSX Core Equity")

            portfolio_market_val += mv
            portfolio_cost += cb
            sector_totals[sector] = sector_totals.get(sector, 0.0) + mv

            holdings_detail.append({
                "ticker": tick,
                "sector": sector,
                "shares": shares,
                "avg_price": avg_price,
                "current_price": curr_price,
                "change_pct": change_pct,
                "market_value": mv,
                "cost_basis": cb,
                "unrealized_pnl": pnl,
                "unrealized_pnl_pct": pnl_pct,
                "dividend_yield": div_yield,
                "sentiment": sentiment,
                "is_kmi30": tick in kmi_set,
                "dcf_fair_value": dcf_fair_val,
                "margin_of_safety_pct": mos_pct,
                "stop_loss": stop_loss_val,
                "stop_distance_pct": stop_dist_pct,
                "fair_pe": fair_pe,
                "trailing_pe": trailing_pe,
                "price_updated_at": price_updated_at,
                "playbook_rule": _match_playbook_rule(tick, sector),
            })

        total_equity_pool = portfolio_market_val + max(0.0, available_cash)
        sector_cap_limit = 30.0

        for h in holdings_detail:
            h["weight_pct"] = round((h["market_value"] / portfolio_market_val * 100), 2) if portfolio_market_val > 0 else 0.0
            sec_val = sector_totals.get(h["sector"], h["market_value"])
            sec_wt = round((sec_val / total_equity_pool * 100.0), 2) if total_equity_pool > 0 else h["weight_pct"]
            h["sector_weight_pct"] = sec_wt
            h["sector_cap_pct"] = sector_cap_limit
            h["sector_cap_utilization_pct"] = round(min(150.0, (sec_wt / sector_cap_limit) * 100.0), 1)

            if h["stop_loss"] > 0 and h["current_price"] <= h["stop_loss"]:
                verdict = "STOP_RISK"
            elif h["weight_pct"] > 30.0 and h["margin_of_safety_pct"] < 10.0:
                verdict = "TRIM"
            elif h["margin_of_safety_pct"] >= 18.0:
                verdict = "ACCUMULATE"
            elif h["margin_of_safety_pct"] <= -8.0 and h["ticker"] not in {"FFC", "EFERT", "MEBL", "HUBC"}:
                verdict = "TRIM"
            else:
                verdict = "HOLD"
            h["verdict"] = verdict

        # Sort holdings by market_value DESC
        holdings_detail.sort(key=lambda x: x["market_value"], reverse=True)

        unrealized_pnl = round(portfolio_market_val - portfolio_cost, 2)
        unrealized_pnl_pct = round((unrealized_pnl / portfolio_cost * 100), 2) if portfolio_cost > 0 else 0.0
        net_worth = round(portfolio_market_val + available_cash, 2)
        true_total_return = round((net_worth + net_div_bank) - fresh_capital, 2)
        true_roi_pct = round((true_total_return / fresh_capital * 100), 2) if fresh_capital > 0 else 0.0

        result_payload = {
            "financial_kpis": {
                "total_net_worth": net_worth,
                "fresh_capital_invested": fresh_capital,
                "fresh_capital_deposited": fresh_capital,
                "ktrade_available_cash": round(available_cash, 2),
                "ktrade_cash_balance": round(available_cash, 2),
                "portfolio_market_value": round(portfolio_market_val, 2),
                "portfolio_cost_basis": round(portfolio_cost, 2),
                "total_cost_basis": round(portfolio_cost, 2),
                "unrealized_pnl": unrealized_pnl,
                "unrealized_pnl_pct": unrealized_pnl_pct,
                "total_gross_dividends": total_gross_div,
                "total_wht_tax_paid": total_tax_wht,
                "net_dividends_in_bank": net_div_bank,
                "dividends_in_bank": net_div_bank,
                "net_dividends_reinvested": net_div_reinvested,
                "dividends_sent_to_ktrade": net_div_reinvested,
                "pending_dividends": pending_div,
                "dividends_pending": pending_div,
                "true_total_return": true_total_return,
                "true_roi_pct": true_roi_pct,
                "price_mode": "CACHED" if use_cached_prices else "LIVE",
            },
            "holdings": holdings_detail,
            "holdings_count": len(holdings_detail),
        }
        _CACHED_FIN_SUMMARY = {
            **result_payload,
            "financial_kpis": {**result_payload["financial_kpis"], "price_mode": "CACHED"},
        }
        _CACHED_FIN_SUMMARY_TS = _time.time()
        return result_payload
    finally:
        cur.close()
        conn.close()

def get_all_dividends(limit: int = 50) -> list[dict]:
    """Retrieves all rows from dividend_ledger ordered by created_at DESC."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT id, ticker, shares_held, dps_gross, gross_amount, tax_rate_pct,
                   tax_withheld, net_amount, ex_date, payment_date, status, notes, created_at
            FROM dividend_ledger
            ORDER BY created_at DESC
            LIMIT %s;
        """, (limit,))
        rows = cur.fetchall()
        return [
            {
                "id": r[0],
                "ticker": r[1],
                "shares_held": float(r[2]),
                "dps_gross": float(r[3]),
                "gross_amount": float(r[4]),
                "tax_rate_pct": float(r[5]),
                "tax_withheld": float(r[6]),
                "net_amount": float(r[7]),
                "ex_date": str(r[8]) if r[8] else None,
                "payment_date": str(r[9]) if r[9] else None,
                "status": r[10],
                "notes": r[11],
                "created_at": r[12].isoformat() if r[12] else None
            }
            for r in rows
        ]
    finally:
        cur.close()
        conn.close()

def get_capital_ledger_entries(limit: int = 50) -> list[dict]:
    """Retrieves recent cash transactions from capital_ledger."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT id, tx_type, amount, reference_note, tx_date
            FROM capital_ledger
            ORDER BY tx_date DESC
            LIMIT %s;
        """, (limit,))
        rows = cur.fetchall()
        return [
            {
                "id": r[0],
                "tx_type": r[1],
                "amount": float(r[2]),
                "reference_note": r[3],
                "tx_date": r[4].isoformat() if r[4] else None
            }
            for r in rows
        ]
    finally:
        cur.close()
        conn.close()

def get_trade_history_entries(limit: int = 50) -> list[dict]:
    """Retrieves executed trade audit trail from trade_history."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT id, ticker, side, quantity, price, fees_and_tax, net_total, executed_at, notes
            FROM trade_history
            ORDER BY executed_at DESC
            LIMIT %s;
        """, (limit,))
        rows = cur.fetchall()
        return [
            {
                "id": r[0],
                "ticker": r[1],
                "side": r[2],
                "quantity": float(r[3]),
                "price": float(r[4]),
                "fees_and_tax": float(r[5]),
                "net_total": float(r[6]),
                "executed_at": r[7].isoformat() if r[7] else None,
                "notes": r[8]
            }
            for r in rows
        ]
    finally:
        cur.close()
        conn.close()

def save_desk_manager_thought(thought_text: str):
    """
    Saves Desk Manager synthesis memorandum into Supabase agent_checkpoints
    and mirrors to local memory_cache/proactive_thought.txt.
    Immune to cloud container ephemeral disk wipes.
    """
    if not thought_text:
        return
    # 1. Mirror to local disk
    try:
        os.makedirs("memory_cache", exist_ok=True)
        with open("memory_cache/proactive_thought.txt", "w", encoding="utf-8") as f:
            f.write(thought_text)
    except Exception as e:
        print(f"⚠️ [Memory Cache] Failed writing local proactive_thought.txt: {e}")

    # 2. Persist to Supabase agent_checkpoints (jsonb format)
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO agent_checkpoints (key, value, updated_at)
            VALUES ('desk_manager_proactive_thought', %s, NOW())
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW();
        """, (json.dumps({"thought": thought_text, "saved_at": datetime.datetime.now().isoformat()}),))
        conn.commit()
        cur.close()
        conn.close()
        print("💾 [Supabase] Desk Manager memorandum successfully persisted to agent_checkpoints.")
    except Exception as e:
        print(f"❌ [Supabase] Failed persisting Desk Manager thought: {e}")

def get_desk_manager_thought() -> str:
    """
    Retrieves the latest Desk Manager synthesis memorandum.
    Checks Supabase agent_checkpoints first (cloud safe), then falls back to local disk.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT value FROM agent_checkpoints
            WHERE key IN ('desk_manager_proactive_thought', 'proactive_thought')
            ORDER BY updated_at DESC
            LIMIT 1;
        """)
        res = cur.fetchone()
        cur.close()
        conn.close()
        if res and res[0]:
            val = res[0]
            if isinstance(val, dict):
                return val.get("thought", "")
            elif isinstance(val, str):
                try:
                    parsed = json.loads(val)
                    if isinstance(parsed, dict) and "thought" in parsed:
                        return parsed["thought"]
                    return str(parsed)
                except Exception:
                    return val
            return str(val)
    except Exception as e:
        print(f"⚠️ [Supabase] Could not fetch desk manager thought: {e}")

    # Fallback to local file if available
    try:
        if os.path.exists("memory_cache/proactive_thought.txt"):
            with open("memory_cache/proactive_thought.txt", "r", encoding="utf-8") as f:
                return f.read()
    except Exception:
        pass

    return "No Desk Manager synthesis memorandum generated yet."


def publish_agent_event(category: str, title: str, content: str, metadata: dict = None) -> bool:
    """
    Unified agent feed publisher. Automatically enriches every event with Co-Investor
    teaching metadata (why_it_matters, quant_concept, portfolio_impact, confidence_weight).
    Categories: 'DESK_MANAGER', 'KTRADE_WHATSAPP', 'YOUTUBE_MENTOR', 'MACRO_NEWS', 'SYSTEM_ALERT'
    """
    try:
        enriched_meta = dict(metadata) if isinstance(metadata, dict) else {}
        try:
            from learning_engine import enrich_event_teaching_metadata
            enriched_meta = enrich_event_teaching_metadata(category, title, content, enriched_meta)
        except Exception as enrich_err:
            print(f"⚠️ [Agent Feed] Teaching metadata enrich fallback: {enrich_err}")

        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO agent_feed (category, title, content, metadata, created_at)
            VALUES (%s, %s, %s, %s, NOW())
            RETURNING id;
        """, (category, title, content, json.dumps(enriched_meta)))
        row = cur.fetchone()
        conn.commit()
        cur.close()
        conn.close()
        event_id = row[0] if row else None
        print(f"📢 [Agent Feed] Published [{category}] #{event_id}: {title}")
        return True
    except Exception as e:
        print(f"❌ [Agent Feed] Failed publishing event [{category}] {title}: {e}")
        return False


def get_agent_feed(limit: int = 50, category: str = None) -> list[dict]:
    """Retrieves recent events from agent_feed table, ensuring Co-Investor teaching metadata is present."""
    from learning_engine import enrich_event_teaching_metadata

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        if category:
            cur.execute("""
                SELECT id, category, title, content, metadata, created_at
                FROM agent_feed
                WHERE category = %s
                ORDER BY created_at DESC
                LIMIT %s;
            """, (category, limit))
        else:
            cur.execute("""
                SELECT id, category, title, content, metadata, created_at
                FROM agent_feed
                ORDER BY created_at DESC
                LIMIT %s;
            """, (limit,))
        rows = cur.fetchall()
        feed_out = []
        for r in rows:
            raw_meta = r[4] if isinstance(r[4], dict) else json.loads(r[4] or "{}")
            if not raw_meta.get("why_it_matters") or not raw_meta.get("quant_concept"):
                raw_meta = enrich_event_teaching_metadata(r[1], r[2], r[3], raw_meta)
            feed_out.append({
                "id": r[0],
                "category": r[1],
                "title": r[2],
                "content": r[3],
                "metadata": raw_meta,
                "created_at": r[5].isoformat() if r[5] else None
            })
        return feed_out
    except Exception as e:
        print(f"❌ [Agent Feed] Error fetching feed: {e}")
        return []
    finally:
        cur.close()
        conn.close()

def save_chat_message(role: str, content: str, actions_taken: list = None) -> int:
    """Saves a user or assistant message to Supabase chat_messages."""
    if actions_taken is None:
        actions_taken = []
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO chat_messages (role, content, actions_taken, created_at)
            VALUES (%s, %s, %s, NOW())
            RETURNING id;
        """, (role, content, json.dumps(actions_taken)))
        row = cur.fetchone()
        conn.commit()
        msg_id = row[0] if row else None
        return msg_id
    finally:
        cur.close()
        conn.close()

def get_chat_history(limit: int = 50) -> list[dict]:
    """Retrieves recent conversation history from chat_messages."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT id, role, content, actions_taken, created_at
            FROM chat_messages
            ORDER BY created_at ASC
            LIMIT %s;
        """, (limit,))
        rows = cur.fetchall()
        return [
            {
                "id": r[0],
                "role": r[1],
                "content": r[2],
                "actions_taken": r[3] if isinstance(r[3], list) else json.loads(r[3] or "[]"),
                "created_at": r[4].isoformat() if r[4] else None
            }
            for r in rows
        ]
    except Exception as e:
        print(f"❌ [Chat History] Error fetching history: {e}")
        return []
    finally:
        cur.close()
        conn.close()



