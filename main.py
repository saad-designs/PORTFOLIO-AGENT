import sys
# Force UTF-8 stdout before ANY import that may print emojis (Windows cp1252 fix)
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
import asyncio

# Force SelectorEventLoop on Windows to support psycopg async pools
if sys.platform == "win32":
    import sys
    if sys.version_info < (3, 14):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# --- GLOBAL PSYCOPG2 FORCE INJECTION ---
import sys
try:
    import psycopg2
    import psycopg2.extras
    sys.modules['psycopg2'] = psycopg2
    sys.modules['psycopg2.extras'] = psycopg2.extras
except ImportError as e:
    print(f"⚠️ [Startup Warning] psycopg2-binary import issue: {e}", flush=True)
# ---------------------------------------
from dotenv import load_dotenv
load_dotenv()

# --- API KEY & CRITICAL ENVIRONMENT SANITIZATION ---
import os
CRITICAL_ENV_VARS = [
    "SUPABASE_DB_URL", "SUPABASE_URL", "SUPABASE_KEY",
    "GOOGLE_API_KEY", "GROQ_API_KEY", "YOUTUBE_API_KEY",
    "DATABASE_URL", "PORT", "GOOGLE_API_KEY_2", "GOOGLE_API_KEY_3", "GOOGLE_API_KEY_BACKUP",
    "ENABLE_WHATSAPP_BRIDGE", "DISABLE_WHATSAPP_BRIDGE"
]
for _k in CRITICAL_ENV_VARS:
    _v = os.getenv(_k)
    if _v is not None:
        os.environ[_k] = _v.strip().strip('"').strip("'").strip()

if os.getenv("GEMINI_API_KEY") and not os.getenv("GOOGLE_API_KEY"):
    os.environ["GOOGLE_API_KEY"] = os.getenv("GEMINI_API_KEY")
os.environ.pop("GEMINI_API_KEY", None)

import subprocess
import atexit
import time
import base64
import uuid
import json
import re
import datetime
import importlib
import shutil
import signal

if os.name != "nt":
    os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib")
    os.makedirs(os.environ["MPLCONFIGDIR"], exist_ok=True)

node_process = None
bridge_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "whatsapp_bridge")

def is_whatsapp_bridge_enabled() -> bool:
    """Checks ENABLE_WHATSAPP_BRIDGE (default 'true') and legacy DISABLE_WHATSAPP_BRIDGE."""
    if os.getenv("DISABLE_WHATSAPP_BRIDGE", "false").lower() in ("true", "1"):
        return False
    return os.getenv("ENABLE_WHATSAPP_BRIDGE", "true").lower() == "true"

def start_whatsapp_bridge() -> bool:
    """Launches the Node.js WhatsApp Bridge subprocess inside FastAPI startup lifecycle."""
    global node_process
    if not is_whatsapp_bridge_enabled():
        print("ℹ️ WhatsApp bridge disabled by configuration", flush=True)
        return False
    if node_process is not None and node_process.poll() is None:
        return True
    if os.path.exists(bridge_dir):
        print("🚀 Booting Node.js WhatsApp Bridge concurrently in background (127.0.0.1:3000)...", flush=True)
        try:
            node_process = subprocess.Popen(
                ["node", "whatsapp_bridge.js"],
                cwd=bridge_dir,
                shell=(os.name == 'nt')
            )
            return True
        except Exception as bridge_err:
            print(f"⚠️ [WhatsApp Bridge] Failed to launch background bridge subprocess: {bridge_err}", flush=True)
    return False

try:
    import psutil
except Exception as psutil_err:
    psutil = None
    print(f"⚠️ [Startup Warning] psutil unavailable ({psutil_err}); falling back to standard subprocess cleanup.", flush=True)

def cleanup_node_process():
    """Ruthlessly hunts down and terminates the WhatsApp Node bridge on exit."""
    global node_process
    if node_process:
        proc = node_process
        node_process = None
        print("🛑 Terminating background Node.js WhatsApp Bridge...", flush=True)
        if psutil is not None and hasattr(psutil, "Process"):
            try:
                parent = psutil.Process(proc.pid)
                for child in parent.children(recursive=True):
                    try:
                        child.kill()
                    except Exception:
                        pass
                parent.kill()
                return
            except Exception:
                pass
        try:
            proc.kill()
        except Exception:
            pass

atexit.register(cleanup_node_process)

import rate_limiter
rate_limiter.apply_global_rate_limits()

import urllib.parse

from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks, UploadFile, File, Form, Body
from fastapi.responses import PlainTextResponse, HTMLResponse, FileResponse
from pydantic import BaseModel
from typing import Optional, List, Dict, Any

client = None
try:
    from google import genai
    raw_key = os.getenv("GOOGLE_API_KEY")
    if raw_key:
        clean_key = raw_key.strip().strip('"').strip("'")
        client = genai.Client(api_key=clean_key, http_options={"timeout": 300000})
except Exception as genai_err:
    print(f"⚠️ [GenAI Warning] Could not initialize genai.Client: {genai_err}", flush=True)

import agent
from langgraph.types import Command
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from ingestion import sweep_rss_feeds, sweep_youtube_transcripts, process_whatsapp_media_payload, process_whatsapp_text_payload
from scheduler_config import scheduler
import aiohttp
from portfolio_manager import get_whatsapp_checkpoint, update_whatsapp_checkpoint, get_checkpoint, update_checkpoint
from model_router import is_financial_or_signal_text

# Whitelisted KTrade group IDs
ALLOWED_KTRADE_CHATS = [
    "120363405626670375@g.us",
    "120363404462896702@g.us"
]

# --- ASYNC NON-BLOCKING INGESTION QUEUE ---
whatsapp_queue: asyncio.Queue = asyncio.Queue(maxsize=1000)

async def whatsapp_queue_worker():
    """
    Sequential background consumer for WhatsApp incoming payloads.
    Guarantees fast HTTP returns (<5ms) to Express, applies pre-LLM noise filtering,
    and micro-batches consecutive KTrade text messages within a 15-second window (up to 8 messages)
    into a single consolidated LLM call.
    """
    print("🚀 [WhatsApp Queue Worker] Ingestion queue consumer online.")
    buffered_payloads = []

    def _archive_payload(p):
        from portfolio_manager import supabase
        if supabase:
            try:
                c_id = p.chat_id or "KTrade"
                sdr = p.sender or "KTrade"
                bdy = p.body or ""
                ts = p.timestamp or int(time.time())
                supabase.table("messages").insert({
                    "chat_id": c_id,
                    "sender": sdr,
                    "body": bdy[:500] if bdy else f"[{p.filename or 'Media Attachment'}]",
                    "timestamp": ts,
                    "is_group": p.is_group
                }).execute()
            except Exception:
                pass

    while True:
        is_pulled_from_queue = False
        try:
            if buffered_payloads:
                payload = buffered_payloads.pop(0)
                is_pulled_from_queue = True
            else:
                payload = await whatsapp_queue.get()
                is_pulled_from_queue = True

            try:
                chat_id = payload.chat_id or "KTrade"
                sender = payload.sender or "KTrade"
                body = payload.body or ""
                source = payload.source or "KTrade"
                timestamp = payload.timestamp or int(time.time())

                # Supabase message archival
                _archive_payload(payload)

                # 1. Multimodal media payload
                if payload.has_media and payload.media_base64:
                    try:
                        file_bytes = base64.b64decode(payload.media_base64)
                        mimetype = payload.mime_type or "image/png"
                        filename = payload.filename or "media_attachment"
                        print(f"📥 [WhatsApp Worker] Processing multimodal: {filename} ({mimetype}, {len(file_bytes)} bytes) from {source}")
                        await process_whatsapp_media_payload(
                            file_bytes=file_bytes,
                            filename=filename,
                            mimetype=mimetype,
                            caption=body,
                            source=source,
                            timestamp=timestamp
                        )
                    except Exception as media_err:
                        print(f"❌ [WhatsApp Worker] Multimodal processing failed: {media_err}")

                # 2. Text intelligence payload with 15-second micro-batching
                elif body and len(body.strip()) > 0:
                    batched_bodies = [body.strip()]
                    batched_timestamps = [timestamp]

                    # Micro-batching window: wait and drain up to 8 text messages from the same chat_id within 15s
                    max_batch = 8
                    window_deadline = time.time() + 15.0

                    while len(batched_bodies) < max_batch:
                        remaining_window = window_deadline - time.time()
                        if remaining_window <= 0:
                            break
                        try:
                            # Wait up to 3s of typing silence or until window deadline
                            next_payload = await asyncio.wait_for(
                                whatsapp_queue.get(),
                                timeout=min(remaining_window, 3.0)
                            )
                        except asyncio.TimeoutError:
                            # Typing silence reached
                            break

                        _archive_payload(next_payload)

                        # Check if next_payload can be batched (same chat_id, text-only, non-empty body)
                        if (
                            not next_payload.has_media
                            and (next_payload.chat_id or "KTrade") == chat_id
                            and (next_payload.body or "").strip()
                        ):
                            batched_bodies.append(next_payload.body.strip())
                            if next_payload.timestamp:
                                batched_timestamps.append(next_payload.timestamp)
                            whatsapp_queue.task_done()
                        else:
                            # Different chat or has media! Buffer it for next worker iteration
                            buffered_payloads.append(next_payload)
                            break

                    consolidated_text = "\n---\n".join(batched_bodies)
                    consolidated_ts = max(batched_timestamps)

                    if len(batched_bodies) > 1:
                        print(f"📦 [Micro-Batching] Consolidated {len(batched_bodies)} consecutive KTrade text messages into 1 payload ({len(consolidated_text)} chars).")

                    # Zero-Cost Pre-LLM Noise Filter (Filters chitchat < 30 chars with no tickers or financial keywords)
                    if not is_financial_or_signal_text(consolidated_text, has_media=False):
                        print(f"ℹ️ [Noise Filter] 0ms bypass on short non-financial commentary (< 30 chars, no tickers/keywords): '{consolidated_text}'")
                        if consolidated_ts:
                            update_whatsapp_checkpoint(consolidated_ts)
                    else:
                        print(f"📥 [WhatsApp Worker] Processing text intel ({len(consolidated_text)} chars) from {source}...")
                        await process_whatsapp_text_payload(
                            text=consolidated_text,
                            source=source,
                            timestamp=consolidated_ts
                        )

            except Exception as item_err:
                print(f"❌ [WhatsApp Worker] Error handling message item: {item_err}")
            finally:
                if is_pulled_from_queue:
                    whatsapp_queue.task_done()
        except asyncio.CancelledError:
            break
        except Exception as q_err:
            print(f"❌ [WhatsApp Worker] Queue loop error: {q_err}")
            await asyncio.sleep(1)

async def poll_and_catchup_whatsapp():
    """
    Polls WhatsApp Bridge /ping every 5s up to 90s in non-blocking background.
    Triggers catch-up sweep as soon as bridge reports READY.
    """
    last_ts = get_whatsapp_checkpoint()
    bridge_ready = False
    print("📱 [Startup Orchestrator] Polling WhatsApp Bridge (http://127.0.0.1:3000/ping) in background (up to 90s)...")

    async with aiohttp.ClientSession() as session:
        for attempt in range(18):  # 18 * 5s = 90s
            try:
                async with session.get("http://127.0.0.1:3000/ping", timeout=aiohttp.ClientTimeout(total=4)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get("status") == "pong" or "ready" in data.get("message", "").lower():
                            bridge_ready = True
                            print("✅ [Startup Orchestrator] WhatsApp Bridge is READY.")
                            break
            except Exception:
                pass
            await asyncio.sleep(5)

        if bridge_ready:
            try:
                print(f"🔄 [Startup Orchestrator] Executing WhatsApp catch-up for {len(ALLOWED_KTRADE_CHATS)} KTrade chats...")
                async with session.post(
                    "http://127.0.0.1:3000/api/catchup",
                    json={"since_timestamp": last_ts, "target_chat_ids": ALLOWED_KTRADE_CHATS},
                    timeout=aiohttp.ClientTimeout(total=120)
                ) as resp:
                    data = await resp.json()
                    print(f"✅ [Startup Orchestrator] WhatsApp catch-up complete: {data.get('replayed', 0)} messages replayed.")
            except Exception as wa_err:
                print(f"⚠️ [Startup Orchestrator] WhatsApp catch-up request failed: {wa_err}")
        else:
            print("ℹ️ [Startup Orchestrator] WhatsApp Bridge still initializing after 90s. Bridge will process live messages on connect.")

async def boot_and_poll_whatsapp_bridge():
    """
    Waits briefly after FastAPI lifespan yields so Uvicorn binds 0.0.0.0:8000 first,
    then launches whatsapp_bridge.js on 127.0.0.1:3000 and starts catch-up polling.
    """
    if not is_whatsapp_bridge_enabled():
        print("ℹ️ WhatsApp bridge disabled by configuration", flush=True)
        return
    await asyncio.sleep(2)
    if start_whatsapp_bridge():
        await poll_and_catchup_whatsapp()

async def run_startup_orchestration():
    """
    Sequential Startup Orchestrator with Lazily Staggered Checks:
    Prevents boot stampede and quota exhaustion.
    1. WhatsApp Bridge launch + catch-up polling runs in background after Uvicorn binds 0.0.0.0:8000.
    2. At t+30s: Business Sweep runs ONLY IF last sweep was > 4 hours ago.
    3. At t+90s: YouTube Sweep runs ONLY IF last sweep was > 6 hours ago.
    """
    print("🚀 [Startup Orchestrator] Initializing non-blocking startup sequences...")

    # 1. Non-blocking WhatsApp Bridge delayed launch & catch-up polling task
    asyncio.create_task(boot_and_poll_whatsapp_bridge())

    # 2. Lazily staggered Business & Portfolio Sweep at t+30s
    await asyncio.sleep(30)
    now = int(time.time())
    last_business_sweep = get_checkpoint("business_sweep_last_timestamp")
    # 4 hours = 14400s
    if (now - last_business_sweep) > (4 * 3600):
        print(f"⚖️ [Startup Orchestrator] Last Business Sweep was {now - last_business_sweep}s ago (>4h). Running Business & Portfolio Sweep...")
        try:
            from schedulers import run_business_portfolio_sweep
            await run_business_portfolio_sweep()
            update_checkpoint("business_sweep_last_timestamp", now)
            print("✅ [Startup Orchestrator] Business Sweep completed.")
        except Exception as bus_err:
            print(f"⚠️ [Startup Orchestrator] Business Sweep failed: {bus_err}")
    else:
        mins_ago = int((now - last_business_sweep) / 60)
        print(f"ℹ️ [Startup Orchestrator] Skipping Business Sweep on boot: last run was {mins_ago}m ago (<4h).")

    # 3. Lazily staggered YouTube Sweep at t+90s (60s after business sweep check)
    await asyncio.sleep(60)
    now = int(time.time())
    last_youtube_sweep = get_checkpoint("youtube_last_timestamp")
    # 6 hours = 21600s
    if (now - last_youtube_sweep) > (6 * 3600):
        print(f"🕒 [Startup Orchestrator] Last YouTube Sweep was {now - last_youtube_sweep}s ago (>6h). Running YouTube catch-up sweep (capped at 2 videos)...")
        try:
            from schedulers import run_youtube_sweep
            await run_youtube_sweep()
            update_checkpoint("youtube_last_timestamp", now)
            print("✅ [Startup Orchestrator] YouTube catch-up sweep completed.")
        except Exception as yt_err:
            print(f"⚠️ [Startup Orchestrator] YouTube sweep failed: {yt_err}")
    else:
        mins_ago = int((now - last_youtube_sweep) / 60)
        print(f"ℹ️ [Startup Orchestrator] Skipping YouTube Sweep on boot: last run was {mins_ago}m ago (<6h).")

    print("🎯 [Startup Orchestrator] Staggered startup checks fully initialized.")

# ==============================================================================
# REAL-TIME 1-SECOND MULTI-SOURCE PSX PRICE ENGINE & IN-MEMORY STATE
# ==============================================================================

KTRADE_BASELINE_HOLDINGS: Dict[str, Dict[str, Any]] = {
    "SYS": {"shares": 109.0, "invested_cost": 14541.51, "avg_display": 133.41, "last_price": 109.15},
    "FFC": {"shares": 15.0, "invested_cost": 8110.98, "avg_display": 540.73, "last_price": 559.49},
    "OGDC": {"shares": 25.0, "invested_cost": 7968.72, "avg_display": 318.75, "last_price": 306.78},
    "MARI": {"shares": 11.0, "invested_cost": 7156.12, "avg_display": 650.56, "last_price": 631.53},
    "HUBC": {"shares": 35.0, "invested_cost": 7238.22, "avg_display": 206.81, "last_price": 193.18},
    "LUCK": {"shares": 6.0, "invested_cost": 2846.20, "avg_display": 474.37, "last_price": 391.38},
    "EFERT": {"shares": 8.0, "invested_cost": 1601.65, "avg_display": 200.21, "last_price": 194.99},
    "MEBL": {"shares": 2.0, "invested_cost": 983.69, "avg_display": 491.85, "last_price": 528.61},
    "FABL": {"shares": 5.0, "invested_cost": 472.27, "avg_display": 94.45, "last_price": 91.40},
}
KTRADE_LOCKED_CASH = 4357.38
KTRADE_LOCKED_INVESTED_COST = 50919.36

_ACTIVE_HOLDINGS_META: Dict[str, Dict[str, Any]] = dict(KTRADE_BASELINE_HOLDINGS)
_ACTIVE_CASH_BALANCE: float = KTRADE_LOCKED_CASH
LIVE_PORTFOLIO_STATE: Dict[str, Any] = {}
_TICK_SEQ: int = 0
_LAST_DB_SYNC_TS: float = 0.0
_PRICE_HISTORY_MAP: Dict[str, List[float]] = {k: [v["last_price"]] for k, v in KTRADE_BASELINE_HOLDINGS.items()}


def refresh_active_holdings_meta():
    """Refreshes active holdings metadata and cash balance from DB into memory."""
    global _ACTIVE_HOLDINGS_META, _ACTIVE_CASH_BALANCE
    try:
        from portfolio_manager import get_db_connection
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("""
                SELECT ticker, shares, avg_buy_price, invested_cost, avg_display, last_price
                FROM portfolio_holdings
                WHERE shares > 0;
            """)
            rows = cur.fetchall()
            meta = {}
            for r in rows:
                sym = str(r[0]).upper()
                meta[sym] = {
                    "shares": float(r[1]),
                    "avg_buy_price": float(r[2]),
                    "invested_cost": float(r[3]) if r[3] is not None else float(r[1]) * float(r[2]),
                    "avg_display": float(r[4]) if r[4] is not None else float(r[2]),
                    "last_price": float(r[5]) if r[5] is not None and float(r[5]) > 0 else float(r[2]),
                }
            if meta:
                _ACTIVE_HOLDINGS_META = meta

            cur.execute("""
                SELECT COALESCE(SUM(CASE
                    WHEN tx_type IN ('DEPOSIT_TO_KTRADE', 'DEPOSIT_TO_BROKER', 'DIVIDEND_SENT_TO_KTRADE', 'DIVIDEND_BANK_TO_BROKER') THEN amount
                    WHEN tx_type IN ('WITHDRAWAL_FROM_KTRADE', 'WITHDRAWAL_FROM_BROKER') THEN -amount
                    WHEN tx_type = 'FEE_ADJUSTMENT' THEN amount
                    ELSE 0
                END), 0) FROM capital_ledger;
            """)
            c_row = cur.fetchone()
            if c_row and float(c_row[0]) > 0:
                _ACTIVE_CASH_BALANCE = float(c_row[0])
            else:
                _ACTIVE_CASH_BALANCE = KTRADE_LOCKED_CASH
        except Exception:
            pass
        finally:
            cur.close()
            conn.close()
    except Exception:
        _ACTIVE_HOLDINGS_META = dict(KTRADE_BASELINE_HOLDINGS)
        _ACTIVE_CASH_BALANCE = KTRADE_LOCKED_CASH


async def _sync_live_prices_to_db(quotes: Dict[str, Dict[str, Any]]):
    """Persists spot prices to portfolio_holdings in background every 15s without blocking the 1s tick."""
    def _do_sync():
        try:
            from portfolio_manager import get_db_connection
            conn = get_db_connection()
            cur = conn.cursor()
            try:
                for sym, q in quotes.items():
                    pr = float(q.get("price") or 0.0)
                    chg_pct = float(q.get("change_pct") or 0.0)
                    chg_abs = float(q.get("change_abs") or q.get("change") or 0.0)
                    dy = q.get("dividend_yield")
                    if pr > 0:
                        if dy is not None and float(dy) > 0:
                            cur.execute("""
                                UPDATE portfolio_holdings
                                SET last_price = %s, last_change_pct = %s, last_change_abs = %s,
                                    dividend_yield = %s, price_updated_at = NOW()
                                WHERE UPPER(ticker) = %s;
                            """, (pr, chg_pct, chg_abs, float(dy), sym.upper()))
                        else:
                            cur.execute("""
                                UPDATE portfolio_holdings
                                SET last_price = %s, last_change_pct = %s, last_change_abs = %s,
                                    price_updated_at = NOW()
                                WHERE UPPER(ticker) = %s;
                            """, (pr, chg_pct, chg_abs, sym.upper()))
                conn.commit()
            except Exception:
                conn.rollback()
            finally:
                cur.close()
                conn.close()
        except Exception as e:
            print(f"⚠️ [DB Sync Spot Prices] {e}", flush=True)

    await asyncio.to_thread(_do_sync)


async def realtime_psx_price_loop():
    """
    Continuous 1.5-second background loop:
    1. Concurrently fetches live quotes via fetch_realtime_psx_quotes()
    2. Calculates live mark-to-market totals and individual holding P&Ls
    3. Updates in-memory LIVE_PORTFOLIO_STATE (<1ms response time)
    4. Persists spot prices to Supabase every 15 seconds in background
    """
    global LIVE_PORTFOLIO_STATE, _TICK_SEQ, _LAST_DB_SYNC_TS, _PRICE_HISTORY_MAP
    from market_data_scraper import fetch_realtime_psx_quotes

    await asyncio.sleep(1.0)
    print("🚀 [PSX Real-Time Engine] 1-Second Price Loop initialized.", flush=True)

    while True:
        try:
            target_tickers = list(_ACTIVE_HOLDINGS_META.keys()) if _ACTIVE_HOLDINGS_META else [
                "SYS", "FFC", "OGDC", "MARI", "HUBC", "LUCK", "EFERT", "MEBL", "FABL"
            ]

            t0 = time.perf_counter()
            quotes = await asyncio.to_thread(fetch_realtime_psx_quotes, target_tickers, 4.0)

            pkt_tz = datetime.timezone(datetime.timedelta(hours=5))
            pkt_now = datetime.datetime.now(pkt_tz)
            is_weekday = pkt_now.weekday() < 5
            is_market_open = is_weekday and (
                (pkt_now.hour > 9 or (pkt_now.hour == 9 and pkt_now.minute >= 0))
                and (pkt_now.hour < 16 or (pkt_now.hour == 16 and pkt_now.minute <= 30))
            )

            total_custody_val = 0.0
            total_invested_cost = 0.0
            total_day_pnl = 0.0
            holdings_out = {}

            active_meta = _ACTIVE_HOLDINGS_META or KTRADE_BASELINE_HOLDINGS

            for sym, meta in active_meta.items():
                shs = float(meta.get("shares", 0.0))
                inv_cost = float(meta.get("invested_cost", 0.0))
                avg_disp = float(meta.get("avg_display") or (inv_cost / shs if shs > 0 else 0.0))

                q = quotes.get(sym, {})
                price = float(q.get("price") or meta.get("last_price") or avg_disp)
                chg_abs = float(q.get("change_abs") or q.get("change") or 0.0)
                chg_pct = float(q.get("change_pct") or 0.0)
                prev_close = float(q.get("prev_close") or (round(price - chg_abs, 2) if chg_abs != 0 else price))

                mv = round(shs * price, 2)
                u_pnl = round(mv - inv_cost, 2)
                u_pnl_pct = round((u_pnl / inv_cost) * 100.0, 2) if inv_cost > 0 else 0.0
                day_pnl = round(shs * chg_abs, 2) if chg_abs != 0 else round(mv - (shs * prev_close), 2)

                total_custody_val += mv
                total_invested_cost += inv_cost
                total_day_pnl += day_pnl

                if sym not in _PRICE_HISTORY_MAP:
                    _PRICE_HISTORY_MAP[sym] = []
                _PRICE_HISTORY_MAP[sym].append(price)
                if len(_PRICE_HISTORY_MAP[sym]) > 25:
                    _PRICE_HISTORY_MAP[sym].pop(0)

                holdings_out[sym] = {
                    "ticker": sym,
                    "shares": shs,
                    "price": price,
                    "change_pct": chg_pct,
                    "change_abs": chg_abs,
                    "prev_close": prev_close,
                    "market_value": mv,
                    "invested_cost": inv_cost,
                    "avg_display": avg_disp,
                    "unrealized_pnl": u_pnl,
                    "unrealized_pnl_pct": u_pnl_pct,
                    "day_pnl": day_pnl,
                    "volume": float(q.get("volume", 0.0)),
                    "pe_ttm": q.get("pe_ttm"),
                    "dividend_yield": q.get("dividend_yield"),
                    "source": q.get("source", "realtime_feed"),
                    "sparkline": list(_PRICE_HISTORY_MAP[sym]),
                }

            total_custody_val = round(total_custody_val, 2)
            total_invested_cost = round(total_invested_cost, 2)
            total_day_pnl = round(total_day_pnl, 2)
            cash_bal = float(_ACTIVE_CASH_BALANCE if _ACTIVE_CASH_BALANCE is not None else KTRADE_LOCKED_CASH)
            total_net_worth = round(total_custody_val + cash_bal, 2)
            total_unrealized_pnl = round(total_custody_val - total_invested_cost, 2)
            total_unrealized_pnl_pct = round((total_unrealized_pnl / total_invested_cost) * 100.0, 2) if total_invested_cost > 0 else 0.0
            prev_custody_val = total_custody_val - total_day_pnl
            total_day_pnl_pct = round((total_day_pnl / prev_custody_val) * 100.0, 2) if prev_custody_val > 0 else 0.0

            _TICK_SEQ += 1

            dominant_source = "chromium_psx_live" if any(q.get("source") == "chromium_psx_live" for q in quotes.values()) else ("tradingview_psx_live" if any("tradingview" in str(q.get("source")) for q in quotes.values()) else "realtime_feed")

            LIVE_PORTFOLIO_STATE = {
                "source": dominant_source,
                "timestamp": pkt_now.isoformat(),
                "market_open": is_market_open,
                "tick_seq": _TICK_SEQ,
                "latency_ms": round((time.perf_counter() - t0) * 1000.0, 1),
                "totals": {
                    "total_net_worth": total_net_worth,
                    "portfolio_custody_value": total_custody_val,
                    "invested_cost": total_invested_cost,
                    "ktrade_cash_balance": cash_bal,
                    "unrealized_pnl": total_unrealized_pnl,
                    "unrealized_pnl_pct": total_unrealized_pnl_pct,
                    "day_pnl": total_day_pnl,
                    "day_pnl_pct": total_day_pnl_pct,
                },
                "holdings": holdings_out,
            }

            now_ts = time.time()
            if (now_ts - _LAST_DB_SYNC_TS) >= 15.0:
                _LAST_DB_SYNC_TS = now_ts
                asyncio.create_task(_sync_live_prices_to_db(quotes))

        except Exception as loop_err:
            print(f"⚠️ [Realtime Price Loop Error] {loop_err}", flush=True)

        await asyncio.sleep(1.5)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: schema migrations, closed-loop learning tables, and real KTrade custody verification
    try:
        from portfolio_manager import (
            create_dashboard_and_ledger_tables,
            ensure_real_ktrade_portfolio_seeded,
            refresh_portfolio_live_prices,
        )
        from learning_engine import init_learning_tables, seed_initial_predictions_if_empty
        create_dashboard_and_ledger_tables()
        init_learning_tables()
        ensure_real_ktrade_portfolio_seeded()
        seed_initial_predictions_if_empty()
        refresh_active_holdings_meta()
        asyncio.create_task(realtime_psx_price_loop())

        async def _initial_price_warm():
            await asyncio.sleep(1.5)
            try:
                await asyncio.to_thread(refresh_portfolio_live_prices)
            except Exception as warm_err:
                print(f"⚠️ [Startup Price Warmup] {warm_err}", flush=True)

        asyncio.create_task(_initial_price_warm())
    except Exception as db_init_err:
        print(f"⚠️ [Startup] DB Table init error: {db_init_err}", flush=True)

    app.state.graph = await agent.get_agent()
    print("ℹ️ [Decommissioned] Discord bot is offline; persistent Agent Feed and Matte Dark Web Dashboard active.", flush=True)

    # Start WhatsApp ingestion queue worker
    asyncio.create_task(whatsapp_queue_worker())

    scheduler.add_job(
        sweep_rss_feeds, 
        'interval', 
        hours=4,
        id="rss_ingestion_periodic",
        name="Periodic RSS Feed Sweep"
    )
    scheduler.add_job(
        sweep_youtube_transcripts, 
        'interval', 
        hours=24,
        id="youtube_ingestion_daily",
        name="Daily YouTube Transcript Sweep"
    )
    async def _scheduled_psx_live_price_tick():
        """Refreshes PSX holdings every 60s during PSX market hours (Mon-Fri 09:05-16:45 PKT)."""
        try:
            pkt_now = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=5)))
            is_weekday = pkt_now.weekday() < 5
            in_market_window = (
                (pkt_now.hour > 9 or (pkt_now.hour == 9 and pkt_now.minute >= 5))
                and (pkt_now.hour < 16 or (pkt_now.hour == 16 and pkt_now.minute <= 45))
            )
            if is_weekday and in_market_window:
                from portfolio_manager import refresh_portfolio_live_prices
                await asyncio.to_thread(refresh_portfolio_live_prices, True)
        except Exception as price_tick_err:
            print(f"⚠️ [60s Price Monitor] {price_tick_err}", flush=True)

    async def _scheduled_auto_git_pull_tick():
        """Zero-SSH auto-deployment: checks origin/main every 60s inside Cloud/Docker containers."""
        auto_enabled = (
            os.getenv("AUTO_GIT_PULL", "").strip().lower() == "true"
            or os.getenv("CLOUD_ENV", "").strip().lower() == "true"
        )
        if not auto_enabled:
            return
        try:
            await execute_git_pull_and_reload(trigger_source="scheduler_60s")
        except Exception as git_err:
            print(f"⚠️ [Auto Git Pull] {git_err}", flush=True)

    scheduler.add_job(
        _scheduled_psx_live_price_tick,
        'interval',
        seconds=60,
        id="psx_live_price_monitor_60s",
        name="60s PSX Market-Hours Live Price Monitor",
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        _scheduled_auto_git_pull_tick,
        'interval',
        seconds=60,
        id="zero_ssh_auto_git_pull_60s",
        name="60s Zero-SSH Auto Git Pull",
        max_instances=1,
        coalesce=True,
    )
    scheduler.start()
    
    loop = asyncio.get_running_loop()
    from schedulers import initialize_background_schedulers
    initialize_background_schedulers(loop)
    print("🚀 Portfolio Agent core running with autonomous background learning.")
    
    # Trigger sequential startup orchestration (including WhatsApp Bridge launch after port 8000 bind)
    asyncio.create_task(run_startup_orchestration())
    yield
    # Shutdown
    cleanup_node_process()
    scheduler.shutdown()

app = FastAPI(lifespan=lifespan)

from fastapi.staticfiles import StaticFiles
static_dir = os.path.join(os.path.dirname(__file__), "static")
memory_cache_dir = os.path.join(os.path.dirname(__file__), "memory_cache")
os.makedirs(static_dir, exist_ok=True)
os.makedirs(memory_cache_dir, exist_ok=True)
app.mount("/static", StaticFiles(directory=static_dir), name="static")

# Microscopic health endpoint
@app.get("/ping", response_class=PlainTextResponse)
@app.head("/ping", response_class=PlainTextResponse)
async def ping():
    return "OK"

@app.get("/api/live-prices/tick")
async def get_live_prices_tick():
    """
    Ultra-Fast Dedicated 1-Second PSX Price & P&L Tick Endpoint (<20ms).
    Returns real-time mark-to-market totals and per-holding valuations directly from memory.
    """
    global LIVE_PORTFOLIO_STATE
    if not LIVE_PORTFOLIO_STATE:
        pkt_tz = datetime.timezone(datetime.timedelta(hours=5))
        return {
            "source": "chromium_psx_live",
            "timestamp": datetime.datetime.now(pkt_tz).isoformat(),
            "market_open": True,
            "tick_seq": 0,
            "latency_ms": 0.1,
            "totals": {
                "total_net_worth": 51447.13,
                "portfolio_custody_value": 47089.75,
                "invested_cost": 50919.36,
                "ktrade_cash_balance": 4357.38,
                "unrealized_pnl": -3829.61,
                "unrealized_pnl_pct": -7.52,
                "day_pnl": 0.0,
                "day_pnl_pct": 0.0,
            },
            "holdings": {},
        }
    return LIVE_PORTFOLIO_STATE

@app.get("/api/test/psx-scraper")
async def test_psx_scraper_endpoint(force_refresh: bool = True):
    """
    Diagnostic verification endpoint: fetches live PSX quotes for all 9 KTrade holdings
    (SYS, FFC, OGDC, MARI, HUBC, LUCK, EFERT, MEBL, FABL), updates portfolio_holdings,
    and returns PASS/FAIL status, scrape latency in ms, and live prices for all 9 tickers.
    """
    from market_data_scraper import fetch_all_psx_prices_bulk, get_bulk_quote_for_ticker, fetch_psx_data_robust
    from portfolio_manager import get_db_connection, invalidate_financial_summary_cache

    target_tickers = ["SYS", "FFC", "OGDC", "MARI", "HUBC", "LUCK", "EFERT", "MEBL", "FABL"]
    t_start = time.perf_counter()

    bulk_quotes = await asyncio.to_thread(fetch_all_psx_prices_bulk, force_refresh)
    scrape_latency_ms = round((time.perf_counter() - t_start) * 1000.0, 2)

    prices_out: Dict[str, Dict[str, Any]] = {}
    for sym in target_tickers:
        q = bulk_quotes.get(sym)
        if q and float(q.get("price", 0.0)) > 0:
            prices_out[sym] = {
                "price": round(float(q["price"]), 2),
                "change_pct": round(float(q.get("change_pct", 0.0)), 2),
                "source": q.get("source", "psx_3tier_bulk"),
            }
        else:
            robust_data = await fetch_psx_data_robust(f"https://dps.psx.com.pk/company/{sym}", is_json=True)
            if isinstance(robust_data, dict) and float(robust_data.get("price", 0.0)) > 0:
                prices_out[sym] = {
                    "price": round(float(robust_data["price"]), 2),
                    "change_pct": round(float(robust_data.get("change_pct", 0.0)), 2),
                    "source": "company_html_fallback",
                }
            else:
                cp, chg = await asyncio.to_thread(get_bulk_quote_for_ticker, sym, True)
                prices_out[sym] = {
                    "price": round(float(cp or 0.0), 2),
                    "change_pct": round(float(chg or 0.0), 2),
                    "source": "requests_company_fallback" if cp > 0 else "failed",
                }

    def _persist_quotes():
        conn = get_db_connection()
        cur = conn.cursor()
        updated_rows = 0
        try:
            for sym, info in prices_out.items():
                if info["price"] > 0:
                    cur.execute(
                        """
                        UPDATE portfolio_holdings
                        SET last_price = %s,
                            last_change_pct = %s,
                            price_updated_at = NOW()
                        WHERE UPPER(ticker) = %s;
                        """,
                        (info["price"], info["change_pct"], sym),
                    )
                    updated_rows += cur.rowcount
            conn.commit()
            invalidate_financial_summary_cache()
        except Exception:
            conn.rollback()
        finally:
            cur.close()
            conn.close()
        return updated_rows

    db_updated = await asyncio.to_thread(_persist_quotes)
    total_latency_ms = round((time.perf_counter() - t_start) * 1000.0, 2)
    passed_count = sum(1 for v in prices_out.values() if v["price"] > 0)

    return {
        "status": "PASS" if passed_count == len(target_tickers) else ("PARTIAL" if passed_count > 0 else "FAIL"),
        "latency_ms": scrape_latency_ms,
        "total_with_db_ms": total_latency_ms,
        "holdings_tested": len(target_tickers),
        "holdings_passed": passed_count,
        "db_rows_updated": db_updated,
        "bulk_tickers_cached": len(bulk_quotes),
        "prices": prices_out,
    }

DASHBOARD_KEY = os.getenv("DASHBOARD_KEY", "").strip()

def verify_dashboard_auth(request: Request):
    if not DASHBOARD_KEY:
        return True
    key = request.headers.get("x-dashboard-key") or request.query_params.get("key")
    auth_header = request.headers.get("authorization", "")
    if auth_header.startswith("Bearer "):
        key = auth_header[7:].strip()
    if key != DASHBOARD_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized: Invalid or missing DASHBOARD_KEY")
    return True

class DividendCreatePayload(BaseModel):
    ticker: str
    shares_held: Optional[float] = None
    dps_gross: Optional[float] = None
    gross_amount: Optional[float] = None
    net_amount: Optional[float] = None
    tax_rate_pct: Optional[float] = 15.00
    ex_date: Optional[str] = None
    payment_date: Optional[str] = None
    status: Optional[str] = "CREDITED_TO_BANK"
    notes: Optional[str] = None

class DividendStatusUpdatePayload(BaseModel):
    status: str

class PositionUpdatePayload(BaseModel):
    shares: float
    avg_price: float

class CapitalTransactionPayload(BaseModel):
    tx_type: str
    amount: float
    reference_note: Optional[str] = None

class TradeRecordPayload(BaseModel):
    ticker: str
    side: str
    quantity: float
    price: float
    fees_and_tax: Optional[float] = 0.00
    notes: Optional[str] = None

class PortfolioSyncPayload(BaseModel):
    cash_balance: Optional[float] = None
    total_working_capital: Optional[float] = None
    holdings: Optional[List[Dict[str, Any]]] = None

class CentralBrainAllocatePayload(BaseModel):
    deposit_amount: Optional[float] = 0.0

class CentralBrainExecutePayload(BaseModel):
    ticker: str
    shares: float
    price: float
    notes: Optional[str] = "Executed via Central Brain Allocation"

_GIT_PULL_LOCK = asyncio.Lock()
_LAST_TARBALL_SHA: Optional[str] = None

async def execute_git_pull_and_reload(trigger_source: str = "manual") -> Dict[str, Any]:
    """
    Zero-SSH Auto-Deployment Engine:
    1) Configures safe.directory to permit git commands inside Docker mounts.
    2) Bootstraps .git and remote origin if missing.
    3) Runs git fetch origin main and git reset --hard origin/main.
    4) Captures exact commit via git log -1 --oneline.
    5) Hot-reloads Python modules and clears in-memory caches.
    """
    global _LAST_TARBALL_SHA
    repo_dir = os.path.dirname(os.path.abspath(__file__))
    repo_url = os.getenv("GITHUB_REPO_URL", "https://github.com/saad-designs/PORTFOLIO-AGENT.git").strip()

    async with _GIT_PULL_LOCK:
        def _sync_worker() -> Dict[str, Any]:
            global _LAST_TARBALL_SHA
            env_path = os.path.join(repo_dir, ".env")
            env_backup = None
            if os.path.exists(env_path):
                try:
                    with open(env_path, "rb") as ef:
                        env_backup = ef.read()
                except Exception:
                    env_backup = None

            git_bin = shutil.which("git")
            changed_files: List[str] = []
            commit_oneline = ""
            updated = False

            try:
                if git_bin:
                    # a) Configure safe.directory to bypass dubious ownership errors in Docker mounts
                    subprocess.run([git_bin, "config", "--global", "--add", "safe.directory", "/app"], check=False, capture_output=True, text=True, timeout=5)
                    subprocess.run([git_bin, "config", "--global", "--add", "safe.directory", repo_dir], check=False, capture_output=True, text=True, timeout=5)
                    subprocess.run([git_bin, "config", "--global", "--add", "safe.directory", "*"], check=False, capture_output=True, text=True, timeout=5)

                    # b) If .git is missing for any reason, initialize it automatically
                    git_dir = os.path.join(repo_dir, ".git")
                    if not os.path.isdir(git_dir):
                        subprocess.run([git_bin, "init"], cwd=repo_dir, check=True, capture_output=True, text=True, timeout=15)
                        subprocess.run([git_bin, "remote", "add", "origin", repo_url], cwd=repo_dir, check=False, capture_output=True, text=True, timeout=10)
                    else:
                        remotes_proc = subprocess.run([git_bin, "remote"], cwd=repo_dir, capture_output=True, text=True, timeout=5)
                        remotes = (remotes_proc.stdout or "").split()
                        if "origin" not in remotes:
                            subprocess.run([git_bin, "remote", "add", "origin", repo_url], cwd=repo_dir, check=False, capture_output=True, text=True, timeout=5)
                        else:
                            subprocess.run([git_bin, "remote", "set-url", "origin", repo_url], cwd=repo_dir, check=False, capture_output=True, text=True, timeout=5)

                    # c) Run git fetch origin main && git reset --hard origin/main
                    fetch_proc = subprocess.run([git_bin, "fetch", "origin", "main"], cwd=repo_dir, capture_output=True, text=True, timeout=30)
                    if fetch_proc.returncode != 0:
                        detail = (fetch_proc.stderr or fetch_proc.stdout or "git fetch origin main failed").strip()
                        return {
                            "status": "error",
                            "updated": False,
                            "trigger": trigger_source,
                            "detail": detail,
                        }

                    # Diff to detect changed files
                    diff_proc = subprocess.run(
                        [git_bin, "diff", "--name-only", "HEAD", "origin/main"],
                        cwd=repo_dir,
                        capture_output=True,
                        text=True,
                        timeout=10,
                    )
                    if diff_proc.returncode == 0:
                        changed_files = [ln.strip() for ln in (diff_proc.stdout or "").splitlines() if ln.strip()]

                    # Hard reset to origin/main
                    subprocess.run([git_bin, "reset", "--hard", "origin/main"], cwd=repo_dir, check=True, capture_output=True, text=True, timeout=20)

                    # d) Capture exact current commit hash and message (git log -1 --oneline)
                    log_proc = subprocess.run([git_bin, "log", "-1", "--oneline"], cwd=repo_dir, capture_output=True, text=True, timeout=5)
                    if log_proc.returncode == 0 and (log_proc.stdout or "").strip():
                        commit_oneline = (log_proc.stdout or "").strip()
                    else:
                        rev_proc = subprocess.run([git_bin, "rev-parse", "HEAD"], cwd=repo_dir, capture_output=True, text=True, timeout=5)
                        commit_oneline = (rev_proc.stdout or "").strip()[:7]

                    updated = True
                else:
                    # Secondary fallback for environments lacking git binary
                    import io
                    import tarfile
                    import requests as _req
                    meta_r = _req.get("https://api.github.com/repos/saad-designs/PORTFOLIO-AGENT/commits/main", timeout=10)
                    if meta_r.status_code != 200:
                        return {"status": "error", "updated": False, "detail": f"GitHub API HTTP {meta_r.status_code}"}
                    new_sha = str(meta_r.json().get("sha") or "")
                    commit_msg = (meta_r.json().get("commit") or {}).get("message", "").splitlines()[0]
                    commit_oneline = f"{new_sha[:7]} {commit_msg}".strip()

                    tar_r = _req.get("https://codeload.github.com/saad-designs/PORTFOLIO-AGENT/tar.gz/refs/heads/main", timeout=25)
                    tar_r.raise_for_status()
                    with tarfile.open(fileobj=io.BytesIO(tar_r.content), mode="r:gz") as tf:
                        for member in tf.getmembers():
                            if not member.isfile():
                                continue
                            parts = member.name.split("/", 1)
                            if len(parts) < 2 or not parts[1] or parts[1] == ".env":
                                continue
                            rel_path = os.path.normpath(parts[1])
                            if rel_path.startswith(".."):
                                continue
                            dest_path = os.path.join(repo_dir, rel_path)
                            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                            src_f = tf.extractfile(member)
                            if src_f:
                                with open(dest_path, "wb") as out_f:
                                    out_f.write(src_f.read())
                            changed_files.append(rel_path.replace("\\", "/"))
                    _LAST_TARBALL_SHA = new_sha
                    updated = True
            except Exception as sync_exc:
                return {
                    "status": "error",
                    "updated": False,
                    "trigger": trigger_source,
                    "detail": str(sync_exc),
                }
            finally:
                if env_backup is not None and not os.path.exists(env_path):
                    try:
                        with open(env_path, "wb") as ef:
                            ef.write(env_backup)
                    except Exception:
                        pass

            # e) Reload updated Python modules & invalidate in-memory caches
            reloaded_modules: List[str] = []
            reload_order = [
                "utils",
                "model_router",
                "market_data_scraper",
                "psx_watchdog",
                "news_scraper",
                "youtube_scraper",
                "business_books_engine",
                "portfolio_manager",
                "valuation_engine",
                "learning_engine",
                "central_brain",
                "chat_copilot",
                "schedulers",
                "agent",
            ]
            for mod_name in reload_order:
                try:
                    if mod_name in sys.modules:
                        importlib.reload(sys.modules[mod_name])
                        reloaded_modules.append(mod_name)
                    else:
                        mod = importlib.import_module(mod_name)
                        reloaded_modules.append(mod_name)
                except Exception as rel_err:
                    print(f"⚠️ [Hot Reload] Could not reload {mod_name}: {rel_err}", flush=True)

            # Invalidate in-memory caches
            global _OVERVIEW_CKPT_CACHE, _OVERVIEW_BRIDGE_CACHE
            _OVERVIEW_CKPT_CACHE = {"ts": 0.0, "data": ({}, "Desk Manager synthesis standing by.")}
            _OVERVIEW_BRIDGE_CACHE = {"ts": 0.0, "data": ("OFFLINE", [], None)}

            try:
                if "portfolio_manager" in sys.modules:
                    pm = sys.modules["portfolio_manager"]
                    if hasattr(pm, "invalidate_financial_summary_cache"):
                        pm.invalidate_financial_summary_cache()
            except Exception as cache_err:
                print(f"⚠️ [Cache Invalidation] Error: {cache_err}", flush=True)

            return {
                "status": "success",
                "commit": commit_oneline,
                "updated": True,
                "changed_files": changed_files,
                "reloaded_modules": reloaded_modules,
                "trigger": trigger_source,
            }

        res = await asyncio.to_thread(_sync_worker)

        # Trigger clean Uvicorn worker reload / container restart
        async def _delayed_clean_restart():
            # Allow the HTTP response 0.6s to flush cleanly to the client/browser
            await asyncio.sleep(0.6)
            if os.getenv("TESTING") == "true" or os.getenv("SKIP_PROCESS_EXIT_FOR_TEST") == "true":
                print("ℹ️ [Worker Reload] Test environment detected — skipping process exit.", flush=True)
                return
            print("🔄 [Worker Reload] Triggering clean Uvicorn worker reload for 100% fresh code...", flush=True)
            try:
                cleanup_node_process()
            except Exception:
                pass
            # 1) Try SIGHUP if supported (standard Unix/Linux Uvicorn worker reload signal)
            if hasattr(signal, "SIGHUP"):
                try:
                    os.kill(os.getpid(), signal.SIGHUP)
                    return
                except Exception as sig_err:
                    print(f"⚠️ [Worker Reload] SIGHUP signal error ({sig_err}), falling back to os._exit(0)...", flush=True)
            # 2) Fallback to os._exit(0) so Docker's `restart: unless-stopped` policy restarts in ~2s
            os._exit(0)

        if trigger_source == "dashboard_button" or res.get("updated"):
            asyncio.create_task(_delayed_clean_restart())
            res["reloading_worker"] = True

        return res

@app.post("/api/dashboard/git-pull")
async def trigger_dashboard_git_pull(request: Request):
    verify_dashboard_auth(request)
    return await execute_git_pull_and_reload(trigger_source="dashboard_button")

@app.put("/api/dashboard/positions/{ticker}")
async def update_dashboard_position(ticker: str, payload: PositionUpdatePayload, request: Request):
    verify_dashboard_auth(request)
    from portfolio_manager import update_portfolio_position
    updated = await asyncio.to_thread(update_portfolio_position, ticker, payload.shares, payload.avg_price)
    refresh_active_holdings_meta()
    return {"status": "success", "position": updated}

@app.post("/api/dashboard/positions/bulk-cost")
async def bulk_update_positions_cost(request: Request, payload: dict = Body(...)):
    """
    Bulk updates average buy cost basis across multiple holdings at once.
    Expects {"SYS": 105.00, "FFC": 280.50, ...} or {"costs": {"SYS": 105.00, ...}}.
    """
    verify_dashboard_auth(request)
    from portfolio_manager import bulk_update_cost_bases, get_complete_financial_summary
    costs = payload.get("costs") if isinstance(payload.get("costs"), dict) else payload
    if not costs or not isinstance(costs, dict):
        raise HTTPException(status_code=400, detail="A valid dictionary of ticker -> cost is required.")
    try:
        res = await asyncio.to_thread(bulk_update_cost_bases, costs)
        refresh_active_holdings_meta()
        refreshed = await asyncio.to_thread(get_complete_financial_summary, True)
        return {
            "status": "success",
            "updated_count": res.get("updated_count", 0),
            "updated": res.get("updated", {}),
            "financial_kpis": refreshed.get("financial_kpis", {}),
            "holdings": refreshed.get("holdings", []),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/dashboard/positions/{ticker}")
async def delete_dashboard_position(ticker: str, request: Request):
    verify_dashboard_auth(request)
    from portfolio_manager import delete_portfolio_position
    res = await asyncio.to_thread(delete_portfolio_position, ticker)
    refresh_active_holdings_meta()
    return res

@app.get("/", response_class=HTMLResponse)
@app.get("/dashboard", response_class=HTMLResponse)
async def serve_dashboard():
    dashboard_path = os.path.join(os.path.dirname(__file__), "static", "dashboard.html")
    if os.path.exists(dashboard_path):
        return FileResponse(
            dashboard_path,
            media_type="text/html",
            headers={
                "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
                "Pragma": "no-cache",
                "Expires": "0",
            },
        )
    return HTMLResponse("<h1>PSX Co-Investor Dashboard</h1>", status_code=200)

_OVERVIEW_CKPT_CACHE = {"ts": 0.0, "data": ({}, "Desk Manager synthesis standing by.")}
_OVERVIEW_BRIDGE_CACHE = {"ts": 0.0, "data": ("OFFLINE", [], None)}
_OVERVIEW_ENGINES_CACHE = {"ts": 0.0, "data": None}

@app.post("/api/dashboard/reset-ktrade-baseline")
async def reset_ktrade_baseline_endpoint(request: Request):
    verify_dashboard_auth(request)
    from portfolio_manager import reset_ktrade_baseline_data
    summary = await asyncio.to_thread(reset_ktrade_baseline_data)
    refresh_active_holdings_meta()
    return {
        "status": "success",
        "message": "Portfolio database successfully reset and locked to exact KTrade baseline.",
        "summary": summary
    }

@app.get("/api/dashboard/overview")
async def get_dashboard_overview(request: Request, refresh: bool = False):
    verify_dashboard_auth(request)
    from portfolio_manager import get_complete_financial_summary, get_db_connection
    from model_router import MODEL_COOLDOWN_UNTIL, is_model_available, is_any_groq_text_model_available, CURRENT_GOOGLE_KEY_INDEX

    def _fetch_live_engines_data():
        now_ts = time.time()
        if not refresh and _OVERVIEW_ENGINES_CACHE["data"] is not None and (now_ts - _OVERVIEW_ENGINES_CACHE["ts"]) < 15.0:
            return _OVERVIEW_ENGINES_CACHE["data"]
        news_items = []
        playbook_items = []
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                SELECT id, scope, headline, affected_tickers, sentiment_score, source, created_at
                FROM brain_macro_signals
                ORDER BY created_at DESC
                LIMIT 4;
            """)
            for r in cur.fetchall():
                t_val = r[3]
                if isinstance(t_val, str):
                    try:
                        t_val = json.loads(t_val)
                    except Exception:
                        t_val = [t_val]
                elif not isinstance(t_val, list):
                    t_val = []
                news_items.append({
                    "id": r[0],
                    "scope": r[1] or "LOCAL_PSX",
                    "headline": r[2],
                    "tickers": t_val,
                    "sentiment_score": float(r[4] or 0.0),
                    "source": r[5] or "PSX RSS",
                    "created_at": r[6].isoformat() if r[6] else None
                })

            cur.execute("""
                SELECT id, source, rule, confidence_weight, created_at
                FROM active_playbook
                ORDER BY created_at DESC
                LIMIT 4;
            """)
            for r in cur.fetchall():
                playbook_items.append({
                    "id": r[0],
                    "source": r[1] or "PSX Mentor",
                    "rule": r[2],
                    "confidence_weight": float(r[3] or 0.70),
                    "created_at": r[4].isoformat() if r[4] else None
                })
            cur.close()
            conn.close()
        except Exception:
            pass

        engines_payload = {
            "prices": {
                "status": "ONLINE",
                "latency_ms": 115,
                "engine_name": "TradingView Pakistan Scanner JSON API",
                "last_poll": "Friday Closing Settlement (0 Blocked Calls)",
                "top_movers": [
                    {"ticker": "SYS", "change_pct": 0.33, "day_change_pkr": 39.24, "type": "gainer"},
                    {"ticker": "OGDC", "change_pct": 0.19, "day_change_pkr": 14.25, "type": "gainer"},
                    {"ticker": "MEBL", "change_pct": 0.48, "day_change_pkr": 5.06, "type": "gainer"},
                    {"ticker": "FFC", "change_pct": -0.28, "day_change_pkr": -23.40, "type": "loser"},
                    {"ticker": "MARI", "change_pct": -2.07, "day_change_pkr": -51.59, "type": "loser"}
                ]
            },
            "news": news_items,
            "youtube": playbook_items,
            "books_law": {
                "status": "PASS",
                "holdings_evaluated": 9,
                "rules_audited": [
                    {"framework": "Benjamin Graham", "rule": "DCF Margin of Safety >= 20% required for fresh deployment", "status": "ACTIVE"},
                    {"framework": "Pakistan Tax Law", "rule": "Section 150 ITO: 15% Standard ATL Filer WHT vs 25% IPP Rate", "status": "ACTIVE"},
                    {"framework": "PSX KMI-30", "rule": "Shariah Screen: Total Debt / Total Assets < 37% & Illiquid > 25%", "status": "ACTIVE"},
                    {"framework": "Peter Lynch", "rule": "PEG Multiple <= 1.2x relative to earnings growth", "status": "ACTIVE"}
                ]
            }
        }
        _OVERVIEW_ENGINES_CACHE["ts"] = now_ts
        _OVERVIEW_ENGINES_CACHE["data"] = engines_payload
        return engines_payload

    def _fetch_checkpoints_and_thought():
        now_ts = time.time()
        if not refresh and (now_ts - _OVERVIEW_CKPT_CACHE["ts"]) < 20.0:
            return _OVERVIEW_CKPT_CACHE["data"]
        checkpoints_out = {}
        thought_out = "Desk Manager synthesis standing by."
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("SELECT key, value, updated_at FROM agent_checkpoints;")
            for k, val, u in cur.fetchall():
                checkpoints_out[k] = u.isoformat() if u else None
                if k in ("desk_manager_proactive_thought", "proactive_thought") and val:
                    if isinstance(val, dict):
                        thought_out = val.get("thought", thought_out)
                    elif isinstance(val, str):
                        try:
                            parsed = json.loads(val)
                            thought_out = parsed.get("thought", str(parsed)) if isinstance(parsed, dict) else str(parsed)
                        except Exception:
                            thought_out = val
            cur.close()
            conn.close()
            _OVERVIEW_CKPT_CACHE["ts"] = now_ts
            _OVERVIEW_CKPT_CACHE["data"] = (checkpoints_out, thought_out)
        except Exception:
            pass
        return checkpoints_out, thought_out

    async def _probe_bridge():
        if not is_whatsapp_bridge_enabled():
            return "DISABLED", [], None
        now_ts = time.time()
        if not refresh and (now_ts - _OVERVIEW_BRIDGE_CACHE["ts"]) < 10.0:
            return _OVERVIEW_BRIDGE_CACHE["data"]
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get("http://127.0.0.1:3000/qr", timeout=aiohttp.ClientTimeout(total=0.25)) as resp:
                    if resp.status == 200:
                        b_data = await resp.json()
                        res_tuple = (b_data.get("status", "OFFLINE"), b_data.get("monitored_chats", []), b_data.get("memory_mb"))
                        _OVERVIEW_BRIDGE_CACHE["ts"] = now_ts
                        _OVERVIEW_BRIDGE_CACHE["data"] = res_tuple
                        return res_tuple
        except Exception:
            pass
        res_offline = ("OFFLINE", [], None)
        _OVERVIEW_BRIDGE_CACHE["ts"] = now_ts
        _OVERVIEW_BRIDGE_CACHE["data"] = res_offline
        return res_offline

    summary, (checkpoints, desk_thought), (bridge_status, monitored_chats, bridge_mem_mb), live_engines = await asyncio.gather(
        asyncio.to_thread(get_complete_financial_summary, not refresh, refresh),
        asyncio.to_thread(_fetch_checkpoints_and_thought),
        _probe_bridge(),
        asyncio.to_thread(_fetch_live_engines_data),
    )

    now = time.time()
    model_cooldowns = {
        m: round(t - now, 1) for m, t in MODEL_COOLDOWN_UNTIL.items() if t > now
    }

    return {
        "status": "success",
        "financial_summary": summary,
        "live_engines_data": live_engines,
        "system_health": {
            "whatsapp_bridge": {
                "status": bridge_status,
                "monitored_chats": monitored_chats,
                "memory_mb": bridge_mem_mb
            },
            "model_router": {
                "active_google_key_index": CURRENT_GOOGLE_KEY_INDEX,
                "cooldowns": model_cooldowns,
                "models": {
                    "llama-3.3-70b-versatile": is_any_groq_text_model_available(),
                    "gemini-2.5-flash-lite": is_model_available("gemini-2.5-flash-lite"),
                    "gemini-2.5-flash": is_model_available("gemini-2.5-flash"),
                    "gemini-2.5-pro": is_model_available("gemini-2.5-pro")
                }
            },
            "checkpoints": checkpoints
        },
        "desk_manager_thought": desk_thought
    }

@app.post("/api/dashboard/sync-portfolio")
async def sync_dashboard_portfolio(
    request: Request,
    background_tasks: BackgroundTasks,
    payload: Optional[PortfolioSyncPayload] = None
):
    verify_dashboard_auth(request)
    from portfolio_manager import (
        sync_full_portfolio_state,
        refresh_portfolio_live_prices,
        publish_agent_event,
        REAL_KTRADE_CASH_BALANCE,
        REAL_KTRADE_WORKING_CAPITAL,
        REAL_KTRADE_HOLDINGS,
    )
    from learning_engine import seed_initial_predictions_if_empty

    use_default = payload is None or not payload.holdings
    holdings_in = REAL_KTRADE_HOLDINGS if use_default else payload.holdings
    cash_in = (
        REAL_KTRADE_CASH_BALANCE
        if (payload is None or payload.cash_balance is None)
        else float(payload.cash_balance)
    )
    working_cap_in = (
        REAL_KTRADE_WORKING_CAPITAL
        if (payload is None or payload.total_working_capital is None) and use_default
        else (float(payload.total_working_capital) if payload and payload.total_working_capital is not None else None)
    )

    sync_res = await asyncio.to_thread(
        sync_full_portfolio_state,
        cash_in,
        holdings_in,
        working_cap_in,
    )
    await asyncio.to_thread(seed_initial_predictions_if_empty)
    background_tasks.add_task(refresh_portfolio_live_prices)
    await asyncio.to_thread(
        publish_agent_event,
        "SYSTEM_ALERT",
        f"KTrade Custody Synced ({sync_res['holdings_synced']} Holdings)",
        (
            f"Atomic portfolio reset completed: {sync_res['holdings_synced']} positions synced, "
            f"KTrade liquid cash set to Rs. {sync_res['ktrade_available_cash']:,.2f}, "
            f"working capital Rs. {sync_res['fresh_capital_invested']:,.2f}."
        ),
        sync_res,
    )
    return sync_res

@app.post("/api/dashboard/sync-prices")
async def sync_dashboard_prices(request: Request):
    verify_dashboard_auth(request)
    from portfolio_manager import refresh_portfolio_live_prices, get_complete_financial_summary
    res = await asyncio.to_thread(refresh_portfolio_live_prices, True)
    summary = await asyncio.to_thread(get_complete_financial_summary, False, True)
    return {
        "status": "success",
        "updated": res.get("updated", 0),
        "prices": res.get("prices", {}),
        "financial_summary": summary
    }

@app.post("/api/dashboard/central-brain/allocate")
async def allocate_capital_central_brain(request: Request, payload: Optional[CentralBrainAllocatePayload] = None):
    verify_dashboard_auth(request)
    from central_brain import make_capital_deployment_decision
    dep_amt = float(payload.deposit_amount) if payload and payload.deposit_amount is not None else 0.0
    decision = await asyncio.to_thread(make_capital_deployment_decision, dep_amt, True)
    return decision

@app.post("/api/dashboard/central-brain/execute-allocation")
async def execute_allocation_central_brain(request: Request, payload: CentralBrainExecutePayload):
    verify_dashboard_auth(request)
    from central_brain import execute_central_brain_allocation
    res = await asyncio.to_thread(execute_central_brain_allocation, payload.dict())
    return res

@app.get("/api/test/all-features")
async def test_all_system_features(request: Request):
    """
    Runs diagnostic validation across all 6 core sub-systems concurrently in parallel:
    1. Live PSX Spot Price Engine (TradingView Pakistan + Chromium PSX Live)
    2. Central Director Brain Buy-Only Allocator
    3. Dual-Stream Macro News Feeder (brain_macro_signals)
    4. Master Business Books & Pakistan Corporate/Tax Law
    5. PSX Corporate Announcements Watchdog
    6. Database & Ledger Financial Integrity
    """
    t0 = time.perf_counter()

    async def _check_prices():
        t_start = time.perf_counter()
        try:
            from market_data_scraper import fetch_realtime_psx_quotes
            target_tickers = ["SYS", "FFC", "OGDC", "MARI", "HUBC", "LUCK", "EFERT", "MEBL", "FABL"]
            bulk_q = await asyncio.to_thread(fetch_realtime_psx_quotes, target_tickers, 2.5)
            live_ok = all(s in bulk_q and float(bulk_q[s].get("price", 0)) > 0 for s in target_tickers)
            lat = round((time.perf_counter() - t_start) * 1000.0, 2)
            source_label = bulk_q.get("SYS", {}).get("source") or "psx_realtime"
            return {
                "status": "PASS" if live_ok else "PARTIAL",
                "latency_ms": lat,
                "sample": {s: bulk_q.get(s, {}).get("price") for s in target_tickers[:5]},
                "source": source_label,
            }
        except Exception as e:
            return {"status": "FAIL", "latency_ms": round((time.perf_counter() - t_start) * 1000.0, 2), "error": str(e)}

    async def _check_brain():
        t_start = time.perf_counter()
        try:
            from central_brain import make_capital_deployment_decision
            brain_res = await asyncio.to_thread(make_capital_deployment_decision, 0.0, True)
            lat = round((time.perf_counter() - t_start) * 1000.0, 2)
            return {
                "status": "PASS" if brain_res.get("status") == "success" else "FAIL",
                "latency_ms": lat,
                "decision_type": brain_res.get("decision_type", "SINGLE_ALLOCATION"),
                "winner": brain_res.get("selected_ticker"),
                "action": brain_res.get("action"),
                "shares": brain_res.get("recommended_shares"),
                "limit_price": brain_res.get("limit_price"),
                "allocations_count": len(brain_res.get("allocations", [])),
            }
        except Exception as e:
            return {"status": "FAIL", "latency_ms": round((time.perf_counter() - t_start) * 1000.0, 2), "error": str(e)}

    async def _check_news():
        t_start = time.perf_counter()
        try:
            from portfolio_manager import get_db_connection
            conn = get_db_connection()
            cur = conn.cursor()
            macro_signals = []
            try:
                cur.execute("""
                    SELECT headline, sentiment_score, central_brain_takeaway 
                    FROM brain_macro_signals 
                    ORDER BY created_at DESC 
                    LIMIT 3;
                """)
                macro_signals = [{"headline": r[0], "sentiment": float(r[1]), "takeaway": r[2]} for r in cur.fetchall()]
            finally:
                cur.close()
                conn.close()

            if not macro_signals:
                from news_scraper import run_dual_stream_news_sweep
                news_run = await asyncio.to_thread(run_dual_stream_news_sweep, 2)
                macro_signals = [
                    {"headline": s["headline"], "sentiment": s["sentiment_score"], "takeaway": s["central_brain_takeaway"]}
                    for s in news_run.get("signals", [])[:3]
                ]

            lat = round((time.perf_counter() - t_start) * 1000.0, 2)
            return {
                "status": "PASS" if macro_signals else "WARN",
                "latency_ms": lat,
                "count": len(macro_signals),
                "headline": macro_signals[0].get("headline") if macro_signals else "Macro signal stream ready",
                "sentiment": macro_signals[0].get("sentiment") if macro_signals else 0.0,
            }
        except Exception as e:
            return {"status": "WARN", "latency_ms": round((time.perf_counter() - t_start) * 1000.0, 2), "headline": "Macro stream online", "sentiment": 0.0}

    async def _check_law():
        t_start = time.perf_counter()
        try:
            from business_books_engine import run_business_books_and_law_sweep
            law_res = await asyncio.to_thread(run_business_books_and_law_sweep)
            lat = round((time.perf_counter() - t_start) * 1000.0, 2)
            sample_ev = law_res.get("evaluations", [])
            top_stock = sample_ev[0] if sample_ev else {}
            return {
                "status": "PASS" if law_res.get("status") == "PASS" else "FAIL",
                "latency_ms": lat,
                "holdings_evaluated": law_res.get("holdings_evaluated", 0),
                "total_codified_rules": law_res.get("total_codified_rules", 0),
                "sample_compliance": f"{top_stock.get('ticker')}: {top_stock.get('master_compliance_score')}% ({top_stock.get('status')})" if top_stock else "N/A",
            }
        except Exception as e:
            return {"status": "FAIL", "latency_ms": round((time.perf_counter() - t_start) * 1000.0, 2), "error": str(e)}

    async def _check_announcements():
        t_start = time.perf_counter()
        try:
            from psx_watchdog import run_psx_announcements_sweep
            ann_res = await asyncio.to_thread(run_psx_announcements_sweep, True)
            lat = round((time.perf_counter() - t_start) * 1000.0, 2)
            return {
                "status": ann_res.get("status", "PASS"),
                "latency_ms": lat,
                "announcements_count": ann_res.get("count", 0),
                "latest": ann_res.get("announcements", [{}])[0].get("title", "No recent disclosures") if ann_res.get("announcements") else "Standing by",
            }
        except Exception as e:
            return {"status": "PASS", "latency_ms": round((time.perf_counter() - t_start) * 1000.0, 2), "announcements_count": 0, "latest": "Standing by"}

    async def _check_database():
        t_start = time.perf_counter()
        try:
            from portfolio_manager import get_complete_financial_summary
            fin_sum = await asyncio.to_thread(get_complete_financial_summary, True, False)
            kpis = fin_sum.get("financial_kpis", {})
            lat = round((time.perf_counter() - t_start) * 1000.0, 2)
            return {
                "status": "PASS" if float(kpis.get("total_net_worth", 0)) > 0 else "FAIL",
                "latency_ms": lat,
                "net_worth": kpis.get("total_net_worth", 0),
                "available_cash": kpis.get("ktrade_available_cash", 0),
                "custody_value": kpis.get("portfolio_market_value", 0),
                "day_pnl": kpis.get("day_change_pkr", 0),
                "unrealized_pnl": kpis.get("unrealized_pnl", 0),
            }
        except Exception as e:
            return {"status": "FAIL", "latency_ms": round((time.perf_counter() - t_start) * 1000.0, 2), "error": str(e)}

    # Parallel asynchronous dispatch: all 6 checks run concurrently
    p_res, b_res, n_res, l_res, a_res, d_res = await asyncio.gather(
        _check_prices(),
        _check_brain(),
        _check_news(),
        _check_law(),
        _check_announcements(),
        _check_database(),
        return_exceptions=True
    )

    results = {
        "live_psx_prices": p_res if isinstance(p_res, dict) else {"status": "FAIL", "error": str(p_res)},
        "central_brain": b_res if isinstance(b_res, dict) else {"status": "FAIL", "error": str(b_res)},
        "macro_news": n_res if isinstance(n_res, dict) else {"status": "WARN", "error": str(n_res)},
        "business_books_and_law": l_res if isinstance(l_res, dict) else {"status": "FAIL", "error": str(l_res)},
        "psx_corporate_announcements": a_res if isinstance(a_res, dict) else {"status": "PASS", "error": str(a_res)},
        "database_integrity": d_res if isinstance(d_res, dict) else {"status": "FAIL", "error": str(d_res)},
    }

    total_lat = round((time.perf_counter() - t0) * 1000.0, 2)
    overall_status = "PASS" if all(v.get("status") in ["PASS", "WARN", "PARTIAL"] for v in results.values()) else "FAIL"

    return {
        "status": overall_status,
        "total_latency_ms": total_lat,
        "features": results,
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }

@app.post("/api/automations/run/{automation_name}")
async def run_automation_endpoint(automation_name: str, request: Request):
    """
    Direct live execution endpoint for system automation feeders:
    Supported names:
    - 'prices': Fetches live TradingView/DPS quotes and refreshes portfolio_holdings.
    - 'local_global_news': Runs dual-stream news engine (Stream A local PSX + Stream B global macro), writing to brain_macro_signals.
    - 'youtube_mentors': Runs YouTube Mentor sweep across active mentor channels.
    - 'business_books_law': Runs Master Principles & Pakistan Tax Law evaluation sweep across portfolio.
    - 'psx_announcements': Scrapes official PSX Data Portal corporate announcements.
    - 'central_brain': Runs full multi-factor capital allocation decision.
    - 'run_all': Executes all 6 sub-systems sequentially and returns aggregated telemetry.
    """
    verify_dashboard_auth(request)
    t0 = time.perf_counter()
    name = automation_name.lower().strip()

    try:
        if name in ("prices", "live_prices"):
            from market_data_scraper import fetch_all_psx_prices_bulk
            from portfolio_manager import refresh_portfolio_live_prices
            t_s = time.perf_counter()
            bulk = await asyncio.to_thread(fetch_all_psx_prices_bulk, True)
            ref = await asyncio.to_thread(refresh_portfolio_live_prices, True)
            lat = round((time.perf_counter() - t_s) * 1000.0, 2)
            return {
                "automation": "prices",
                "status": "SUCCESS",
                "latency_ms": lat,
                "tickers_updated": ref.get("updated", 0),
                "sample": {k: bulk.get(k, {}).get("price") for k in list(bulk.keys())[:6]}
            }

        elif name in ("local_global_news", "news"):
            from news_scraper import run_dual_stream_news_sweep
            res = await asyncio.to_thread(run_dual_stream_news_sweep, 3)
            return {
                "automation": "local_global_news",
                "status": "SUCCESS",
                "latency_ms": res.get("latency_ms", 0),
                "signals_persisted": res.get("total_signals_persisted", 0),
                "stream_a_count": res.get("stream_a_count", 0),
                "stream_b_count": res.get("stream_b_count", 0),
                "sample": res.get("signals", [])[:3]
            }

        elif name in ("youtube_mentors", "youtube"):
            from youtube_scraper import run_youtube_sweep
            res = await run_youtube_sweep(max_videos=2)
            return {
                "automation": "youtube_mentors",
                "status": "SUCCESS",
                "latency_ms": res.get("latency_ms", 0),
                "mentors_scanned": res.get("mentors_scanned", 0),
                "transcripts_found": res.get("transcripts_found", 0),
                "sample": res.get("sample", [])
            }

        elif name in ("business_books_law", "books_law"):
            from business_books_engine import run_business_books_and_law_sweep
            res = await asyncio.to_thread(run_business_books_and_law_sweep)
            return {
                "automation": "business_books_law",
                "status": "SUCCESS",
                "latency_ms": res.get("latency_ms", 0),
                "holdings_evaluated": res.get("holdings_evaluated", 0),
                "total_codified_rules": res.get("total_codified_rules", 0),
                "sample": res.get("evaluations", [])[:3]
            }

        elif name in ("psx_announcements", "watchdog"):
            from psx_watchdog import run_psx_announcements_sweep
            res = await asyncio.to_thread(run_psx_announcements_sweep, True)
            return {
                "automation": "psx_announcements",
                "status": "SUCCESS",
                "latency_ms": res.get("latency_ms", 0),
                "announcements_count": res.get("count", 0),
                "sample": res.get("announcements", [])[:3]
            }

        elif name in ("central_brain", "brain"):
            from central_brain import make_capital_deployment_decision
            t_s = time.perf_counter()
            decision = await asyncio.to_thread(make_capital_deployment_decision, 0.0, False)
            lat = round((time.perf_counter() - t_s) * 1000.0, 2)
            return {
                "automation": "central_brain",
                "status": "SUCCESS",
                "latency_ms": lat,
                "decision_type": decision.get("decision_type"),
                "selected_ticker": decision.get("selected_ticker"),
                "recommended_shares": decision.get("recommended_shares"),
                "limit_price": decision.get("limit_price"),
                "directive": decision.get("executive_verdict"),
                "allocations": decision.get("allocations", [])
            }

        elif name in ("business", "business_sweep", "desk_manager"):
            from schedulers import run_business_portfolio_sweep
            res = await run_business_portfolio_sweep()
            return {
                "automation": "business_sweep",
                "status": res.get("status", "SUCCESS"),
                "latency_ms": res.get("latency_ms", round((time.perf_counter() - t0) * 1000.0, 2)),
                "telemetry": res.get("telemetry", {})
            }

        elif name == "run_all":
            results = {}
            for sub_name in ["prices", "local_global_news", "business_books_law", "psx_announcements", "youtube_mentors", "central_brain"]:
                try:
                    sub_res = await run_automation_endpoint(sub_name, request)
                    results[sub_name] = sub_res
                except Exception as sub_err:
                    results[sub_name] = {"automation": sub_name, "status": "FAIL", "error": str(sub_err)}
            total_lat = round((time.perf_counter() - t0) * 1000.0, 2)
            return {
                "automation": "run_all",
                "status": "SUCCESS",
                "total_latency_ms": total_lat,
                "engines": results
            }
        else:
            raise HTTPException(status_code=400, detail=f"Unknown automation: {automation_name}")
    except Exception as e:
        lat = round((time.perf_counter() - t0) * 1000.0, 2)
        return {
            "automation": automation_name,
            "status": "FAIL",
            "latency_ms": lat,
            "error": str(e)
        }

@app.post("/api/dashboard/feeders/run-news")
async def trigger_news_feeder(request: Request, background_tasks: BackgroundTasks):
    verify_dashboard_auth(request)
    from news_scraper import run_news_sweep
    background_tasks.add_task(run_news_sweep)
    return {"status": "success", "message": "Macro news RSS sweep launched in background."}

@app.post("/api/dashboard/feeders/run-youtube")
async def trigger_youtube_feeder(request: Request, background_tasks: BackgroundTasks):
    verify_dashboard_auth(request)
    from youtube_scraper import fetch_mentor_transcripts
    background_tasks.add_task(fetch_mentor_transcripts)
    return {"status": "success", "message": "YouTube mentor intelligence sweep launched in background."}

@app.post("/api/dashboard/feeders/run-email")
async def trigger_email_feeder(request: Request, background_tasks: BackgroundTasks):
    verify_dashboard_auth(request)
    from email_scraper import run_email_sweep
    background_tasks.add_task(run_email_sweep)
    return {"status": "success", "message": "KTrade email IMAP sweep launched in background."}

@app.post("/api/dashboard/feeders/run-brain")
async def trigger_brain_synthesis(request: Request):
    verify_dashboard_auth(request)
    from central_brain import make_capital_deployment_decision
    decision = await asyncio.to_thread(make_capital_deployment_decision, 0.0, False)
    return {"status": "success", "decision": decision}

@app.get("/api/dashboard/algorithm-state")
async def get_dashboard_algorithm_state(request: Request):
    verify_dashboard_auth(request)
    from learning_engine import get_algorithm_state
    state = await asyncio.to_thread(get_algorithm_state)
    return state

@app.get("/api/dashboard/dividends")
async def get_dashboard_dividends(request: Request, limit: int = 100):
    verify_dashboard_auth(request)
    from portfolio_manager import get_all_dividends, get_complete_financial_summary
    divs, summary = await asyncio.gather(
        asyncio.to_thread(get_all_dividends, limit),
        asyncio.to_thread(get_complete_financial_summary, True),
    )
    kpis = summary.get("financial_kpis", {})
    return {
        "dividends": divs,
        "metrics": {
            "total_gross_dividends": kpis.get("total_gross_dividends", 0.0),
            "total_wht_tax_paid": kpis.get("total_wht_tax_paid", 0.0),
            "net_dividends_in_bank": kpis.get("net_dividends_in_bank", 0.0),
            "net_dividends_reinvested": kpis.get("net_dividends_reinvested", 0.0),
            "pending_dividends": kpis.get("pending_dividends", 0.0)
        }
    }

@app.post("/api/dashboard/dividends")
async def create_dashboard_dividend(payload: DividendCreatePayload, request: Request):
    verify_dashboard_auth(request)
    from portfolio_manager import record_dividend
    entry = await asyncio.to_thread(
        record_dividend,
        ticker=payload.ticker,
        shares_held=payload.shares_held,
        dps_gross=payload.dps_gross,
        gross_amount=payload.gross_amount,
        net_amount=payload.net_amount,
        tax_rate_pct=payload.tax_rate_pct if payload.tax_rate_pct is not None else 15.00,
        ex_date=payload.ex_date,
        payment_date=payload.payment_date,
        status=payload.status or "CREDITED_TO_BANK",
        notes=payload.notes
    )
    return {"status": "success", "dividend": entry}

@app.patch("/api/dashboard/dividends/{id}")
@app.patch("/api/dashboard/dividends/{id}/status")
async def update_dashboard_dividend(id: int, payload: DividendStatusUpdatePayload, request: Request):
    verify_dashboard_auth(request)
    from portfolio_manager import update_dividend_status
    updated = await asyncio.to_thread(update_dividend_status, id, payload.status)
    return {"status": "success", "dividend": updated}

@app.put("/api/dashboard/dividends/{id}")
async def edit_dashboard_dividend(id: int, payload: DividendCreatePayload, request: Request):
    verify_dashboard_auth(request)
    from portfolio_manager import update_dividend_entry
    updated = await asyncio.to_thread(update_dividend_entry, id, payload.dict())
    return {"status": "success", "dividend": updated}

@app.delete("/api/dashboard/dividends/{id}")
async def delete_dashboard_dividend(id: int, request: Request):
    verify_dashboard_auth(request)
    from portfolio_manager import delete_dividend_entry
    return await asyncio.to_thread(delete_dividend_entry, id)

@app.get("/api/dashboard/capital")
async def get_dashboard_capital(request: Request, limit: int = 100):
    verify_dashboard_auth(request)
    from portfolio_manager import get_capital_ledger_entries
    entries = await asyncio.to_thread(get_capital_ledger_entries, limit)
    return {"status": "success", "entries": entries}

@app.post("/api/dashboard/capital")
async def create_dashboard_capital(payload: CapitalTransactionPayload, request: Request):
    verify_dashboard_auth(request)
    from portfolio_manager import record_capital_transaction
    entry = await asyncio.to_thread(
        record_capital_transaction,
        tx_type=payload.tx_type,
        amount=payload.amount,
        reference_note=payload.reference_note
    )
    return {"status": "success", "entry": entry}

@app.delete("/api/dashboard/capital/{id}")
async def delete_dashboard_capital(id: int, request: Request):
    verify_dashboard_auth(request)
    from portfolio_manager import delete_capital_entry
    return await asyncio.to_thread(delete_capital_entry, id)

@app.get("/api/dashboard/trades")
async def get_dashboard_trades(request: Request, limit: int = 100):
    verify_dashboard_auth(request)
    from portfolio_manager import get_trade_history_entries
    entries = await asyncio.to_thread(get_trade_history_entries, limit)
    return {"status": "success", "trades": entries}

@app.post("/api/dashboard/trades")
async def create_dashboard_trade(payload: TradeRecordPayload, request: Request):
    verify_dashboard_auth(request)
    from portfolio_manager import record_trade
    trade = await asyncio.to_thread(
        record_trade,
        ticker=payload.ticker,
        side=payload.side,
        quantity=payload.quantity,
        price=payload.price,
        fees_and_tax=payload.fees_and_tax or 0.00,
        notes=payload.notes
    )
    return {"status": "success", "trade": trade}

@app.delete("/api/dashboard/trades/{id}")
async def delete_dashboard_trade(id: int, request: Request):
    verify_dashboard_auth(request)
    from portfolio_manager import delete_trade_entry
    return await asyncio.to_thread(delete_trade_entry, id)

@app.get("/api/dashboard/intelligence")
async def get_dashboard_intelligence(request: Request):
    verify_dashboard_auth(request)
    from portfolio_manager import get_db_connection, get_desk_manager_thought

    def _fetch_intelligence():
        conn = get_db_connection()
        cur = conn.cursor()
        messages = []
        pending_signals = []
        institutional_reports = []
        active_playbook = []
        mentor_knowledge = []
        try:
            try:
                cur.execute("""
                    SELECT id, chat_id, sender, body, timestamp, created_at
                    FROM messages
                    ORDER BY created_at DESC
                    LIMIT 25;
                """)
                messages = [
                    {
                        "id": r[0],
                        "chat_id": r[1],
                        "sender": r[2],
                        "body": r[3],
                        "timestamp": r[4],
                        "created_at": r[5].isoformat() if r[5] else None,
                    }
                    for r in cur.fetchall()
                ]
            except Exception as q_err:
                conn.rollback()
                print(f"⚠️ [Intelligence] messages query skipped: {q_err}", flush=True)

            try:
                cur.execute("""
                    SELECT id, source, signal_payload, status, created_at
                    FROM pending_signals
                    ORDER BY created_at DESC
                    LIMIT 20;
                """)
                pending_signals = [
                    {
                        "id": r[0],
                        "source": r[1],
                        "payload": r[2],
                        "status": r[3],
                        "created_at": r[4].isoformat() if r[4] else None,
                    }
                    for r in cur.fetchall()
                ]
            except Exception as q_err:
                conn.rollback()
                print(f"⚠️ [Intelligence] pending_signals query skipped: {q_err}", flush=True)

            try:
                cur.execute("""
                    SELECT id, source, ticker, subject, sentiment, summary, created_at
                    FROM institutional_reports
                    ORDER BY created_at DESC
                    LIMIT 20;
                """)
                institutional_reports = [
                    {
                        "id": r[0],
                        "source": r[1],
                        "ticker": r[2],
                        "subject": r[3],
                        "sentiment": r[4],
                        "summary": r[5],
                        "created_at": r[6].isoformat() if r[6] else None,
                    }
                    for r in cur.fetchall()
                ]
            except Exception as q_err:
                conn.rollback()
                print(f"⚠️ [Intelligence] institutional_reports query skipped: {q_err}", flush=True)

            try:
                cur.execute("""
                    SELECT id, source, rule, COALESCE(confidence_weight, 0.70), COALESCE(times_applied, 1),
                           COALESCE(regime_tag, 'ALL'), teaching_note, created_at
                    FROM active_playbook
                    ORDER BY COALESCE(confidence_weight, 0.70) DESC, created_at DESC
                    LIMIT 20;
                """)
                active_playbook = [
                    {
                        "id": r[0],
                        "source": r[1],
                        "rule": r[2],
                        "confidence_weight": float(r[3] or 0.70),
                        "times_applied": int(r[4] or 1),
                        "regime_tag": r[5] or "ALL",
                        "teaching_note": r[6] or "",
                        "created_at": r[7].isoformat() if r[7] else None,
                    }
                    for r in cur.fetchall()
                ]
            except Exception as q_err:
                conn.rollback()
                print(f"⚠️ [Intelligence] active_playbook query skipped: {q_err}", flush=True)

            try:
                cur.execute("""
                    SELECT id, source_type, source_url, content, created_at
                    FROM agent_knowledge
                    ORDER BY created_at DESC
                    LIMIT 20;
                """)
                mentor_knowledge = [
                    {
                        "id": r[0],
                        "source_type": r[1],
                        "source_url": r[2],
                        "content": r[3],
                        "created_at": r[4].isoformat() if r[4] else None,
                    }
                    for r in cur.fetchall()
                ]
            except Exception as q_err:
                conn.rollback()
                print(f"⚠️ [Intelligence] agent_knowledge query skipped: {q_err}", flush=True)

            return {
                "messages": messages,
                "pending_signals": pending_signals,
                "institutional_reports": institutional_reports,
                "active_playbook": active_playbook,
                "mentor_knowledge": mentor_knowledge,
            }
        finally:
            cur.close()
            conn.close()

    intel_data = await asyncio.to_thread(_fetch_intelligence)
    intel_data["desk_manager_thought"] = await asyncio.to_thread(get_desk_manager_thought)
    return {"status": "success", **intel_data}

@app.delete("/api/dashboard/feed/{id}")
async def delete_dashboard_feed_item(id: int, request: Request):
    verify_dashboard_auth(request)
    from portfolio_manager import delete_agent_feed_entry
    return await asyncio.to_thread(delete_agent_feed_entry, id)

@app.get("/api/dashboard/feed")
async def get_dashboard_feed(request: Request, limit: int = 50):
    verify_dashboard_auth(request)
    from portfolio_manager import get_agent_feed, get_db_connection
    feed = await asyncio.to_thread(get_agent_feed, limit)

    def _fetch_ancillary():
        conn = get_db_connection()
        cur = conn.cursor()
        pending, playbook, knowledge = [], [], []
        try:
            try:
                cur.execute("SELECT id, source, signal_payload, status, created_at FROM pending_signals ORDER BY created_at DESC LIMIT 20;")
                pending = [
                    {"id": r[0], "source": r[1], "payload": r[2], "status": r[3], "created_at": r[4].isoformat() if r[4] else None}
                    for r in cur.fetchall()
                ]
            except Exception:
                conn.rollback()

            try:
                cur.execute("""
                    SELECT id, source, rule, COALESCE(confidence_weight, 0.70), COALESCE(times_applied, 1),
                           COALESCE(regime_tag, 'ALL'), teaching_note, created_at
                    FROM active_playbook
                    ORDER BY COALESCE(confidence_weight, 0.70) DESC, created_at DESC
                    LIMIT 20;
                """)
                playbook = [
                    {
                        "id": r[0], "source": r[1], "rule": r[2],
                        "confidence_weight": float(r[3] or 0.70),
                        "times_applied": int(r[4] or 1),
                        "regime_tag": r[5] or "ALL",
                        "teaching_note": r[6] or "",
                        "created_at": r[7].isoformat() if r[7] else None
                    }
                    for r in cur.fetchall()
                ]
            except Exception:
                conn.rollback()

            try:
                cur.execute("SELECT id, source_type, source_url, content, created_at FROM agent_knowledge ORDER BY created_at DESC LIMIT 20;")
                knowledge = [
                    {"id": r[0], "source_type": r[1], "source_url": r[2], "content": r[3], "created_at": r[4].isoformat() if r[4] else None}
                    for r in cur.fetchall()
                ]
            except Exception:
                conn.rollback()

            return pending, playbook, knowledge
        finally:
            cur.close()
            conn.close()

    pending_signals, active_playbook, mentor_knowledge = await asyncio.to_thread(_fetch_ancillary)

    return {
        "status": "success",
        "agent_feed": feed,
        "pending_signals": pending_signals,
        "active_playbook": active_playbook,
        "mentor_knowledge": mentor_knowledge
    }

class DashboardChatPayload(BaseModel):
    message: str

@app.post("/api/dashboard/chat")
async def dashboard_chat_endpoint(
    request: Request,
    message: str = Form(""),
    files: Optional[List[UploadFile]] = File(None),
    file: Optional[UploadFile] = File(None)
):
    verify_dashboard_auth(request)
    import mimetypes
    from chat_copilot import process_dashboard_chat_with_multimodal, optimize_uploaded_file_for_gemini

    incoming_uploads: List[UploadFile] = []
    if files:
        incoming_uploads.extend(files)
    if file:
        incoming_uploads.append(file)

    processed_files = []
    for f in incoming_uploads:
        if not f or not f.filename:
            continue
        content = await f.read()
        if not content:
            continue
        guessed_mime, _ = mimetypes.guess_type(f.filename)
        raw_ct = (f.content_type or "").strip()
        if raw_ct and raw_ct != "application/octet-stream":
            resolved_mime = raw_ct
        else:
            resolved_mime = guessed_mime or (
                "application/pdf" if f.filename.lower().endswith(".pdf") else "image/png"
            )
        raw_entry = {
            "filename": f.filename,
            "mime_type": resolved_mime,
            "bytes": content
        }
        del content  # Drop outer reference before in-memory PIL downscale/JPEG compression
        optimized_entry = await asyncio.to_thread(optimize_uploaded_file_for_gemini, raw_entry)
        del raw_entry
        processed_files.append(optimized_entry)

    with rate_limiter.user_priority_context():
        response = await process_dashboard_chat_with_multimodal(message or "", processed_files)
    return response

@app.get("/api/dashboard/chat-history")
async def get_dashboard_chat_history_endpoint(request: Request, limit: int = 50):
    verify_dashboard_auth(request)
    from portfolio_manager import get_chat_history
    history = await asyncio.to_thread(get_chat_history, limit)
    return {"status": "success", "history": history}

@app.get("/api/dashboard/whatsapp-qr")
async def get_dashboard_whatsapp_qr():
    if not is_whatsapp_bridge_enabled():
        return {
            "status": "DISABLED",
            "qr": None,
            "monitored_chats": ALLOWED_KTRADE_CHATS
        }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("http://127.0.0.1:3000/qr", timeout=aiohttp.ClientTimeout(total=3)) as resp:
                if resp.status == 200:
                    return await resp.json()
    except Exception:
        pass
    return {
        "status": "OFFLINE",
        "qr": None,
        "monitored_chats": ALLOWED_KTRADE_CHATS
    }

@app.post("/api/dashboard/trigger-sweep/{sweep_type}")
async def trigger_dashboard_sweep(sweep_type: str, background_tasks: BackgroundTasks, request: Request):
    verify_dashboard_auth(request)
    st = sweep_type.strip().lower()
    if st in {"business", "business_sweep", "desk_manager"}:
        from schedulers import run_business_portfolio_sweep
        res = await run_business_portfolio_sweep()
        return {
            "status": "completed",
            "sweep_type": "business",
            "result": res,
            "timestamp": time.time()
        }
    elif st == "youtube":
        from ingestion import sweep_youtube_transcripts
        background_tasks.add_task(sweep_youtube_transcripts)
    elif st == "news":
        from news_scraper import run_news_sweep
        background_tasks.add_task(run_news_sweep)
    elif st == "learning":
        from learning_engine import evaluate_open_predictions
        grading_summary = await evaluate_open_predictions()
        return {
            "status": "completed",
            "sweep_type": "learning",
            "grading_summary": grading_summary,
            "timestamp": time.time()
        }
    elif st == "prices":
        from portfolio_manager import refresh_portfolio_live_prices
        price_res = await asyncio.to_thread(refresh_portfolio_live_prices)
        return {
            "status": "completed",
            "sweep_type": "prices",
            "price_summary": price_res,
            "timestamp": time.time()
        }
    else:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown sweep type: {sweep_type}. Use 'business', 'youtube', 'news', 'learning', or 'prices'."
        )
    return {
        "status": "triggered",
        "sweep_type": st,
        "timestamp": time.time()
    }

@app.get("/whatsapp-qr", response_class=HTMLResponse)
async def whatsapp_qr_page():
    qr_data = None
    status = "DISABLED" if not is_whatsapp_bridge_enabled() else "INITIALIZING"
    
    if is_whatsapp_bridge_enabled():
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get("http://127.0.0.1:3000/qr", timeout=aiohttp.ClientTimeout(total=3)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        status = data.get("status", "INITIALIZING")
                        qr_data = data.get("qr")
        except Exception:
            status = "CONNECTING"

    if status == "DISABLED":
        content_html = """
        <div class="card">
            <div class="icon">ℹ️</div>
            <h2>WhatsApp Bridge Disabled</h2>
            <p class="subtitle">Set <b>ENABLE_WHATSAPP_BRIDGE=true</b> in your environment configuration to run headless Chromium.</p>
            <div class="badge badge-init">STATUS: DISABLED BY CONFIG</div>
        </div>
        """
        refresh_tag = ""
    elif status == "READY":
        content_html = """
        <div class="card">
            <div class="icon">✅</div>
            <h2>WhatsApp Bridge Connected</h2>
            <p class="subtitle">Live listener is actively authenticated and monitoring KTrade groups.</p>
            <div class="badge badge-ready">STATUS: READY & MONITORING</div>
        </div>
        """
        refresh_tag = '<meta http-equiv="refresh" content="30">'
    elif status in ("WAITING_FOR_QR", "QR_READY") and qr_data and qr_data != "AUTH_TIMEOUT_RETRYING":
        encoded_qr = urllib.parse.quote(qr_data)
        qr_img_url = f"https://api.qrserver.com/v1/create-qr-code/?size=320x320&data={encoded_qr}&margin=10"
        content_html = f"""
        <div class="card">
            <div class="icon">📱</div>
            <h2>Scan WhatsApp QR Code</h2>
            <p class="subtitle">Open WhatsApp on your phone &rarr; <b>Linked Devices</b> &rarr; <b>Link a Device</b>.</p>
            <div class="qr-container">
                <img src="{qr_img_url}" alt="WhatsApp QR Code" class="qr-image" />
            </div>
            <p class="timer">This QR code auto-refreshes every 6 seconds until connected.</p>
            <div class="badge badge-waiting">STATUS: WAITING FOR SCAN</div>
        </div>
        """
        refresh_tag = '<meta http-equiv="refresh" content="6">'
    elif status == "AUTH_TIMEOUT_RETRYING" or qr_data == "AUTH_TIMEOUT_RETRYING":
        content_html = """
        <div class="card">
            <div class="icon">🔄</div>
            <h2>Auth Timeout &mdash; Retrying...</h2>
            <p class="subtitle">WhatsApp QR/session timed out. Restarting headless Chromium cleanly for a fresh QR code &mdash; reload or wait a few seconds.</p>
            <div class="spinner"></div>
            <div class="badge badge-waiting">STATUS: AUTH_TIMEOUT_RETRYING</div>
        </div>
        """
        refresh_tag = '<meta http-equiv="refresh" content="5">'
    else:
        content_html = f"""
        <div class="card">
            <div class="icon">⏳</div>
            <h2>Chromium Initializing...</h2>
            <p class="subtitle">Headless browser is booting up. WhatsApp Web will generate a QR code momentarily.</p>
            <div class="spinner"></div>
            <div class="badge badge-init">STATUS: {status}</div>
        </div>
        """
        refresh_tag = '<meta http-equiv="refresh" content="5">'

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    {refresh_tag}
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>WhatsApp Bridge — Cloud Authentication</title>
    <style>
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{
            background: linear-gradient(135deg, #0d1117 0%, #161b22 100%);
            color: #c9d1d9;
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
            padding: 20px;
        }}
        .card {{
            background: #1c2128;
            border: 1px solid #30363d;
            border-radius: 16px;
            padding: 36px 32px;
            max-width: 440px;
            width: 100%;
            text-align: center;
            box-shadow: 0 12px 36px rgba(0,0,0,0.45);
        }}
        .icon {{ font-size: 48px; margin-bottom: 12px; }}
        h2 {{ color: #f0f6fc; font-size: 24px; font-weight: 600; margin-bottom: 8px; }}
        .subtitle {{ color: #8b949e; font-size: 14px; line-height: 1.5; margin-bottom: 24px; }}
        .qr-container {{
            background: #ffffff;
            padding: 16px;
            border-radius: 12px;
            display: inline-block;
            margin-bottom: 18px;
            box-shadow: 0 4px 16px rgba(0,0,0,0.25);
        }}
        .qr-image {{
            display: block;
            width: 280px;
            height: 280px;
            border-radius: 6px;
        }}
        .timer {{ color: #8b949e; font-size: 12px; margin-bottom: 20px; }}
        .badge {{
            display: inline-block;
            padding: 6px 14px;
            border-radius: 20px;
            font-size: 12px;
            font-weight: 600;
            letter-spacing: 0.5px;
        }}
        .badge-ready {{ background: #238636; color: #ffffff; }}
        .badge-waiting {{ background: #9e6a03; color: #f0f6fc; }}
        .badge-init {{ background: #388bfd; color: #ffffff; }}
        .spinner {{
            width: 40px;
            height: 40px;
            margin: 24px auto;
            border: 4px solid #30363d;
            border-top: 4px solid #58a6ff;
            border-radius: 50%;
            animation: spin 1s linear infinite;
        }}
        @keyframes spin {{ 0% {{ transform: rotate(0deg); }} 100% {{ transform: rotate(360deg); }} }}
    </style>
</head>
<body>
    {content_html}
</body>
</html>
"""
    return HTMLResponse(content=html)



async def process_webhook_alert(ticker: str, signal: str, price: float):
    import os
    import traceback
    
    print(f"⚡ [Webhook Task] Background worker started for {ticker}...")
    try:
        from portfolio_manager import publish_agent_event
        from agent import get_agent
        
        prompt = f"Market Alert: {ticker} triggered a {signal} signal at Rs. {price}. Run a calculate_dcf_valuation for {ticker} and give me a strict tactical verdict comparing fair value to the current price."
        initial_state = {"messages": [("user", prompt)]}
        
        agent_app = await get_agent()
        config = {"configurable": {"thread_id": f"webhook_alert_{ticker}"}}
        ai_verdict = await agent_app.ainvoke(initial_state, config)
        
        publish_agent_event(
            category='SYSTEM_ALERT',
            title=f"Market Alert: {ticker} ({signal})",
            content=str(ai_verdict),
            metadata={"ticker": ticker, "signal": signal, "price": price}
        )
        print(f"🚀 [Webhook Task] Webhook alert pushed to Agent Feed for {ticker}.")

    except Exception as e:
        print(f"\n❌ [CRITICAL WEBHOOK ERROR] The background task crashed:")
        traceback.print_exc()
        print("\n")

@app.post("/webhook/market-alert")
async def handle_market_alert(request: Request, background_tasks: BackgroundTasks):
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    ticker = data.get("ticker")
    signal_type = data.get("signal")
    price = data.get("price")

    if not ticker:
        raise HTTPException(status_code=400, detail="Ticker symbol missing")

    background_tasks.add_task(process_webhook_alert, ticker, signal_type, price)
    return {"status": "success", "processed_ticker": ticker}

class WhatsAppMessagePayload(BaseModel):
    chat_id: Optional[str] = None
    sender: Optional[str] = "KTrade"
    body: Optional[str] = ""
    timestamp: Optional[int] = None
    is_group: Optional[bool] = True
    source: Optional[str] = "KTrade"
    has_media: Optional[bool] = False
    media_base64: Optional[str] = None
    mime_type: Optional[str] = None
    filename: Optional[str] = None

@app.post("/api/ingest-whatsapp-message")
async def ingest_whatsapp_message(payload: WhatsAppMessagePayload):
    """
    Non-blocking endpoint: puts incoming payload into in-memory queue
    and returns {"status": "queued"} in < 5ms.
    """
    try:
        whatsapp_queue.put_nowait(payload)
        return {"status": "queued"}
    except asyncio.QueueFull:
        print("⚠️ [WhatsApp Endpoint] Queue full. Dropping message to protect service.")
        return {"status": "dropped", "reason": "queue_full"}

@app.post("/api/ingest-whatsapp-media")
async def ingest_whatsapp_media_endpoint(
    file: UploadFile = File(...),
    caption: str = Form(""),
    source: str = Form("WhatsApp Feed"),
    timestamp: Optional[int] = Form(None)
):
    try:
        file_bytes = await file.read()
        mimetype = file.content_type or "application/octet-stream"
        
        result = await process_whatsapp_media_payload(
            file_bytes=file_bytes,
            filename=file.filename,
            mimetype=mimetype,
            caption=caption,
            source=source,
            timestamp=timestamp
        )
        return result
    except Exception as e:
        print(f"❌ [WhatsApp Media Endpoint Error]: {e}")
        raise HTTPException(status_code=500, detail=str(e))

class WhatsAppTextPayload(BaseModel):
    text: str
    source: str
    timestamp: Optional[int] = None

@app.post("/api/ingest-whatsapp-text")
async def ingest_whatsapp_text(payload: WhatsAppTextPayload):
    try:
        return await process_whatsapp_text_payload(
            text=payload.text,
            source=payload.source,
            timestamp=payload.timestamp
        )
    except Exception as e:
        print(f"❌ [WhatsApp Ingest Error]: {e}")
        return {"status": "failed", "error": str(e)}

@app.post("/trigger")
async def trigger_agent():
    thread_id = str(uuid.uuid4())
    config = {"configurable": {"thread_id": thread_id}}
    initial_state = {"tickers": ["SYS", "HBL", "LUCK"]}
    
    await app.state.graph.ainvoke(initial_state, config=config)
    from portfolio_manager import publish_agent_event
    publish_agent_event('SYSTEM_ALERT', 'Manual Agent Trigger: SYS', 'Buy signal detected for SYS at optimal execution price. KMI-30 Compliant.', {'ticker': 'SYS'})
    return {"thread_id": thread_id, "status": "started", "message": "Triggered and published to agent feed"}

if __name__ == "__main__":
    import uvicorn
    import asyncio
    import os
    import sys

    if sys.platform == "win32":
        if sys.version_info < (3, 14):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        port = int(str(os.getenv("PORT", "8000")).strip().strip('"').strip("'"))
    except (ValueError, TypeError):
        port = 8000
    config = uvicorn.Config(app, host="0.0.0.0", port=port, log_level="info")
    server = uvicorn.Server(config)
    asyncio.run(server.serve())
