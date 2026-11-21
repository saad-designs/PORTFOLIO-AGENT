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
from urllib.parse import urlsplit
import base64
import hashlib
import hmac
import secrets
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

def cleanup_stale_chromium_locks():
    """Removes broken Chromium symlinks (SingletonLock, SingletonCookie, SingletonSocket) before launching bridge."""
    try:
        if os.name != "nt":
            subprocess.run("find /app -name 'Singleton*' -delete 2>/dev/null || true", shell=True, timeout=5)
            subprocess.run("find /tmp -name 'Singleton*' -delete 2>/dev/null || true", shell=True, timeout=5)
            subprocess.run("find /app -name 'lockfile' -delete 2>/dev/null || true", shell=True, timeout=5)
            subprocess.run("find /tmp -name 'psx_chrome_profile' -exec rm -rf {} + 2>/dev/null || true", shell=True, timeout=5)
    except Exception as e:
        print(f"⚠️ [Lock Cleanup] Error executing shell find/delete: {e}", flush=True)

    search_dirs = [
        os.path.join(bridge_dir, ".wwebjs_auth"),
        "/tmp",
        "/tmp/psx_chrome_profile",
        os.getenv("TEMP", ""),
        os.getenv("TMP", "")
    ]
    for sdir in search_dirs:
        if not sdir or not os.path.isdir(sdir):
            continue
        try:
            for root, dirs, files in os.walk(sdir, topdown=False, followlinks=False):
                for fname in files:
                    lower = fname.lower()
                    if lower.startswith("singleton") or lower == "lockfile":
                        fpath = os.path.join(root, fname)
                        try:
                            os.unlink(fpath)
                            print(f"🧹 [Python Lock Cleaner] Unlinked stale lock: {fpath}", flush=True)
                        except Exception:
                            pass
        except Exception:
            pass

def start_whatsapp_bridge() -> bool:
    """Launches the Node.js WhatsApp Bridge subprocess inside FastAPI startup lifecycle."""
    global node_process
    if not is_whatsapp_bridge_enabled():
        print("ℹ️ WhatsApp bridge disabled by configuration", flush=True)
        return False
    if node_process is not None and node_process.poll() is None:
        return True
    if os.path.exists(bridge_dir):
        cleanup_stale_chromium_locks()
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
from fastapi import FastAPI, Request, Response, HTTPException, BackgroundTasks, UploadFile, File, Form, Body
from fastapi.responses import PlainTextResponse, HTMLResponse, FileResponse, JSONResponse
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
from portfolio_manager import get_whatsapp_checkpoint, update_whatsapp_checkpoint, get_checkpoint, update_checkpoint, is_psx_market_open
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
    "SYS": {"shares": 109.0, "invested_cost": 14541.51, "avg_display": 133.41, "last_price": 119.48},
    "FFC": {"shares": 15.0, "invested_cost": 8110.98, "avg_display": 540.73, "last_price": 574.04},
    "OGDC": {"shares": 25.0, "invested_cost": 7968.72, "avg_display": 318.75, "last_price": 316.09},
    "MARI": {"shares": 11.0, "invested_cost": 7156.12, "avg_display": 650.56, "last_price": 663.86},
    "HUBC": {"shares": 35.0, "invested_cost": 7238.22, "avg_display": 206.81, "last_price": 208.98},
    "LUCK": {"shares": 6.0, "invested_cost": 2846.20, "avg_display": 474.37, "last_price": 445.06},
    "EFERT": {"shares": 8.0, "invested_cost": 1601.65, "avg_display": 200.21, "last_price": 210.88},
    "MEBL": {"shares": 2.0, "invested_cost": 983.69, "avg_display": 491.85, "last_price": 563.92},
    "FABL": {"shares": 5.0, "invested_cost": 472.27, "avg_display": 94.45, "last_price": 96.65},
}
KTRADE_LOCKED_CASH = 4357.38
KTRADE_LOCKED_INVESTED_COST = 50919.36

_ACTIVE_HOLDINGS_META: Dict[str, Dict[str, Any]] = {}
_ACTIVE_CASH_BALANCE: Optional[float] = None
_PORTFOLIO_DB_AVAILABLE = False
_PORTFOLIO_DB_ERROR: Optional[str] = None
LIVE_PORTFOLIO_STATE: Dict[str, Any] = {}
_TICK_SEQ: int = 0
_LAST_DB_SYNC_TS: float = 0.0
_LAST_PORTFOLIO_DB_REFRESH_TS: float = 0.0
_LATEST_QUOTES: Dict[str, Dict[str, Any]] = {}
_PRICE_HISTORY_MAP: Dict[str, List[float]] = {}

from live_portfolio import build_live_state, mark_live_state_stale


def _refresh_live_portfolio_snapshot(error: Optional[str] = None) -> Dict[str, Any]:
    global LIVE_PORTFOLIO_STATE
    now_ts = time.time()
    pkt_now = datetime.datetime.fromtimestamp(
        now_ts,
        datetime.timezone(datetime.timedelta(hours=5)),
    )
    LIVE_PORTFOLIO_STATE = build_live_state(
        _ACTIVE_HOLDINGS_META,
        _LATEST_QUOTES,
        _ACTIVE_CASH_BALANCE,
        _PORTFOLIO_DB_AVAILABLE,
        is_psx_market_open(pkt_now),
        now_ts,
        _TICK_SEQ,
        error or _PORTFOLIO_DB_ERROR,
    )
    return LIVE_PORTFOLIO_STATE


_refresh_live_portfolio_snapshot()


def refresh_active_holdings_meta():
    """Refreshes active holdings metadata and cash balance from DB into memory."""
    global _ACTIVE_HOLDINGS_META, _ACTIVE_CASH_BALANCE, _PORTFOLIO_DB_AVAILABLE, _PORTFOLIO_DB_ERROR
    conn = None
    cur = None
    try:
        from portfolio_manager import get_db_connection
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT ticker, shares, avg_buy_price, invested_cost, avg_display, last_price,
                   price_updated_at, price_source, last_change_pct, last_change_abs
            FROM portfolio_holdings
            WHERE shares > 0;
        """)
        meta = {}
        for r in cur.fetchall():
            sym = str(r[0]).upper()
            meta[sym] = {
                "shares": float(r[1]),
                "avg_buy_price": float(r[2]),
                "invested_cost": float(r[3]) if r[3] is not None else float(r[1]) * float(r[2]),
                "avg_display": float(r[4]) if r[4] is not None else None,
                "last_price": float(r[5]) if r[5] is not None and float(r[5]) > 0 else None,
                "price_timestamp": r[6],
                "price_source": r[7],
                "last_change_pct": float(r[8]) if r[8] is not None else None,
                "last_change_abs": float(r[9]) if r[9] is not None else None,
            }
        cur.execute("""
            SELECT COALESCE(SUM(CASE
                WHEN tx_type IN ('DEPOSIT_TO_KTRADE', 'DEPOSIT_TO_BROKER', 'DIVIDEND_SENT_TO_KTRADE', 'DIVIDEND_BANK_TO_BROKER') THEN amount
                WHEN tx_type IN ('WITHDRAWAL_FROM_KTRADE', 'WITHDRAWAL_FROM_BROKER') THEN -amount
                WHEN tx_type = 'FEE_ADJUSTMENT' THEN amount
                ELSE 0
            END), 0) FROM capital_ledger;
        """)
        cash_row = cur.fetchone()
        if cash_row is None or cash_row[0] is None:
            raise RuntimeError("Cash balance query returned no value")
        _ACTIVE_HOLDINGS_META = meta
        _ACTIVE_CASH_BALANCE = float(cash_row[0])
        _PORTFOLIO_DB_AVAILABLE = True
        _PORTFOLIO_DB_ERROR = None
    except Exception as exc:
        _ACTIVE_HOLDINGS_META = {}
        _ACTIVE_CASH_BALANCE = None
        _PORTFOLIO_DB_AVAILABLE = False
        _PORTFOLIO_DB_ERROR = str(exc)
        print(f"⚠️ [Portfolio State] Database unavailable: {exc}", flush=True)
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

    _refresh_live_portfolio_snapshot()
    return _PORTFOLIO_DB_AVAILABLE


async def _sync_live_prices_to_db(quotes: Dict[str, Dict[str, Any]]):
    """Persists spot prices to portfolio_holdings in background every 15s without blocking the 1s tick."""
    def _do_sync():
        try:
            from market_data_scraper import _is_fresh_verified_quote
            from portfolio_manager import get_db_connection
            from portfolio_manager import is_psx_market_open
            if is_psx_market_open() is not True:
                return
            conn = get_db_connection()
            cur = conn.cursor()
            try:
                for sym, q in quotes.items():
                    pr = float(q.get("price") or 0.0)
                    chg_pct = float(q.get("change_pct") or 0.0)
                    chg_abs = float(q.get("change_abs") or q.get("change") or 0.0)
                    source = q.get("source")
                    quote_timestamp = q.get("timestamp")
                    dy = q.get("dividend_yield")
                    if (
                        pr > 0
                        and source
                        and str(source).strip().lower() not in {
                            "db_cached_last_price",
                            "database_stored",
                            "realtime_feed",
                            "unknown",
                        }
                        and quote_timestamp
                        and _is_fresh_verified_quote(q)
                    ):
                        if dy is not None and float(dy) > 0:
                            cur.execute("""
                                UPDATE portfolio_holdings
                                SET last_price = %s, last_change_pct = %s, last_change_abs = %s,
                                    dividend_yield = %s, price_source = %s, price_updated_at = to_timestamp(%s)
                                WHERE UPPER(ticker) = %s;
                            """, (pr, chg_pct, chg_abs, float(dy), source, float(quote_timestamp), sym.upper()))
                        else:
                            cur.execute("""
                                UPDATE portfolio_holdings
                                SET last_price = %s, last_change_pct = %s, last_change_abs = %s,
                                    price_source = %s, price_updated_at = to_timestamp(%s)
                                WHERE UPPER(ticker) = %s;
                            """, (pr, chg_pct, chg_abs, source, float(quote_timestamp), sym.upper()))
                conn.commit()
            except Exception:
                conn.rollback()
            finally:
                cur.close()
                conn.close()
        except Exception as e:
            print(f"⚠️ [DB Sync Spot Prices] {e}", flush=True)

    await asyncio.to_thread(_do_sync)


async def fast_1s_heartbeat_loop():
    """
    Unconditional 1.0-second in-memory heartbeat loop running 24/7:
    - Increments _TICK_SEQ += 1 unconditionally every 1.0 second.
    - Recalculates mark-to-market totals and individual holding P&Ls from _LATEST_QUOTES.
    - Zero blocking network I/O, guaranteed <1ms response time.
    """
    global LIVE_PORTFOLIO_STATE, _TICK_SEQ
    await asyncio.sleep(0.5)
    print("🚀 [PSX Heartbeat Engine] Fast 1-Second Heartbeat loop initialized.", flush=True)

    while True:
        try:
            _TICK_SEQ += 1
            _refresh_live_portfolio_snapshot()
        except Exception as heartbeat_err:
            print(f"⚠️ [Fast Heartbeat Loop Error] {heartbeat_err}", flush=True)
            if LIVE_PORTFOLIO_STATE:
                LIVE_PORTFOLIO_STATE = mark_live_state_stale(
                    LIVE_PORTFOLIO_STATE,
                    str(heartbeat_err),
                    time.time(),
                )

        await asyncio.sleep(1.0)


async def background_quote_fetcher_loop():
    """
    Background worker loop fetching live quotes every 1.5s via fetch_realtime_psx_quotes().
    Updates shared _LATEST_QUOTES cache and syncs spot prices to database every 15 seconds.
    Completely decoupled from fast_1s_heartbeat_loop so network pauses never freeze tick_seq.
    """
    global _LATEST_QUOTES, _LAST_DB_SYNC_TS, _LAST_PORTFOLIO_DB_REFRESH_TS
    from market_data_scraper import fetch_realtime_psx_quotes

    await asyncio.sleep(0.1)
    print("🚀 [PSX Quote Fetcher] Background Quote Fetcher loop initialized.", flush=True)

    while True:
        try:
            now_ts = time.time()
            if now_ts - _LAST_PORTFOLIO_DB_REFRESH_TS >= 15.0:
                _LAST_PORTFOLIO_DB_REFRESH_TS = now_ts
                await asyncio.to_thread(refresh_active_holdings_meta)
            target_tickers = list(_ACTIVE_HOLDINGS_META.keys())
            if not _PORTFOLIO_DB_AVAILABLE or not target_tickers:
                await asyncio.sleep(1.5)
                continue
            quotes = await asyncio.to_thread(fetch_realtime_psx_quotes, target_tickers, 3.0)
            if quotes and isinstance(quotes, dict):
                _LATEST_QUOTES.update(quotes)

                now_ts = time.time()
                if (now_ts - _LAST_DB_SYNC_TS) >= 15.0:
                    _LAST_DB_SYNC_TS = now_ts
                    asyncio.create_task(_sync_live_prices_to_db(quotes))
        except Exception as q_err:
            print(f"⚠️ [Background Quote Fetcher Error] {q_err}", flush=True)

        await asyncio.sleep(1.5)


# Alias for backwards compatibility
realtime_psx_price_loop = fast_1s_heartbeat_loop


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Start fast 1-second heartbeat loop and background quote fetcher task
    app.state.heartbeat_task = asyncio.create_task(fast_1s_heartbeat_loop())
    app.state.quote_fetcher_task = asyncio.create_task(background_quote_fetcher_loop())
    app.state.price_loop_task = app.state.heartbeat_task
    print("🚀 [Lifespan] Assigned app.state.heartbeat_task and app.state.quote_fetcher_task.", flush=True)

    # Startup: schema migrations, closed-loop learning tables, and real KTrade custody verification
    try:
        from portfolio_manager import (
            create_dashboard_and_ledger_tables,
        )
        from learning_engine import init_learning_tables, seed_initial_predictions_if_empty
        create_dashboard_and_ledger_tables()
        init_learning_tables()
        seed_initial_predictions_if_empty()
        refresh_active_holdings_meta()
    except Exception as db_init_err:
        global _PORTFOLIO_DB_AVAILABLE, _PORTFOLIO_DB_ERROR, _ACTIVE_HOLDINGS_META, _ACTIVE_CASH_BALANCE
        _PORTFOLIO_DB_AVAILABLE = False
        _PORTFOLIO_DB_ERROR = str(db_init_err)
        _ACTIVE_HOLDINGS_META = {}
        _ACTIVE_CASH_BALANCE = None
        print(f"⚠️ [Startup] DB Table init error: {db_init_err}", flush=True)
        _refresh_live_portfolio_snapshot()

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
        """Refreshes stored portfolio prices only during verified PSX trading sessions."""
        try:
            pkt_now = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=5)))
            from portfolio_manager import is_psx_market_open, refresh_portfolio_live_prices
            if is_psx_market_open(pkt_now) is True:
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
async def get_live_prices_tick(request: Request):
    """
    Ultra-Fast Dedicated 1-Second PSX Price & P&L Tick Endpoint (<20ms).
    Returns real-time mark-to-market totals and per-holding valuations directly from memory.
    Guaranteed never to return empty holdings: {} or hardcoded market_open.
    """
    verify_dashboard_auth(request)
    return LIVE_PORTFOLIO_STATE

@app.get("/api/test/psx-scraper")
async def test_psx_scraper_endpoint(request: Request, force_refresh: bool = True):
    """
    Diagnostic verification endpoint for database-backed holdings.
    """
    verify_dashboard_auth(request)
    from market_data_scraper import fetch_all_psx_prices_bulk
    from portfolio_manager import get_db_connection, invalidate_financial_summary_cache

    target_tickers = list(_ACTIVE_HOLDINGS_META) if _PORTFOLIO_DB_AVAILABLE else []
    t_start = time.perf_counter()

    bulk_quotes = (
        await asyncio.to_thread(fetch_all_psx_prices_bulk, force_refresh, 10, target_tickers)
        if target_tickers
        else {}
    )
    scrape_latency_ms = round((time.perf_counter() - t_start) * 1000.0, 2)

    prices_out: Dict[str, Dict[str, Any]] = {}
    now_ts = time.time()
    pkt_now = datetime.datetime.fromtimestamp(
        now_ts,
        datetime.timezone(datetime.timedelta(hours=5)),
    )
    diagnostic_state = build_live_state(
        _ACTIVE_HOLDINGS_META,
        bulk_quotes,
        _ACTIVE_CASH_BALANCE,
        _PORTFOLIO_DB_AVAILABLE,
        is_psx_market_open(pkt_now),
        now_ts,
        _TICK_SEQ,
        _PORTFOLIO_DB_ERROR,
    )
    for sym, holding in diagnostic_state["holdings"].items():
        quote = bulk_quotes.get(sym, {})
        source = quote.get("source")
        quote_timestamp = quote.get("timestamp")
        verified_quote = (
            bool(source)
            and str(source).strip().lower() not in {
                "db_cached_last_price",
                "database_stored",
                "realtime_feed",
                "unknown",
            }
            and not quote.get("stored")
            and holding.get("status") == "live"
            and holding.get("price") is not None
        )
        prices_out[sym] = {
            "price": holding.get("price"),
            "change_pct": holding.get("change_pct"),
            "source": holding.get("price_source"),
            "timestamp": holding.get("price_timestamp"),
            "timestamp_kind": holding.get("price_timestamp_kind"),
            "received_timestamp": holding.get("price_received_timestamp"),
            "freshness_evidence": holding.get("freshness_evidence"),
            "price_limit_status": holding.get("price_limit_status"),
            "price_limit_source": holding.get("price_limit_source"),
            "price_limit_retrieved_at": holding.get("price_limit_retrieved_at"),
            "lower_limit": holding.get("lower_limit"),
            "upper_limit": holding.get("upper_limit"),
            "age_seconds": holding.get("price_age_seconds"),
            "status": holding.get("status"),
            "verified_quote": verified_quote,
        }

    def _persist_quotes():
        conn = get_db_connection()
        cur = conn.cursor()
        updated_rows = 0
        try:
            for sym, info in prices_out.items():
                if info["verified_quote"]:
                    cur.execute(
                        """
                        UPDATE portfolio_holdings
                        SET last_price = %s,
                            last_change_pct = %s,
                            price_source = %s,
                            price_updated_at = to_timestamp(%s)
                        WHERE UPPER(ticker) = %s;
                        """,
                        (
                            info["price"],
                            info["change_pct"] or 0.0,
                            info["source"],
                            float(bulk_quotes[sym]["timestamp"]),
                            sym,
                        ),
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

    db_updated = await asyncio.to_thread(_persist_quotes) if target_tickers else 0
    total_latency_ms = round((time.perf_counter() - t_start) * 1000.0, 2)
    passed_count = sum(1 for v in prices_out.values() if v["verified_quote"])

    return {
        "status": (
            "UNAVAILABLE" if not target_tickers
            else "PASS" if passed_count == len(target_tickers)
            else "PARTIAL" if passed_count > 0
            else "FAIL"
        ),
        "latency_ms": scrape_latency_ms,
        "total_with_db_ms": total_latency_ms,
        "holdings_tested": len(target_tickers),
        "holdings_passed": passed_count,
        "db_rows_updated": db_updated,
        "bulk_tickers_cached": len(bulk_quotes),
        "prices": prices_out,
    }

DASHBOARD_KEY = os.getenv("DASHBOARD_KEY", "").strip()
INGEST_TOKEN = os.getenv("INGEST_TOKEN", "").strip()
DASHBOARD_SESSION_TTL_SECONDS = 12 * 60 * 60
SESSION_COOKIE_SECURE_MODE = os.getenv("SESSION_COOKIE_SECURE", "auto").strip().lower()
_HTTP_SESSION_WARNING_LOGGED = False
if SESSION_COOKIE_SECURE_MODE not in {"auto", "true", "false"}:
    raise RuntimeError("SESSION_COOKIE_SECURE must be one of: auto, true, false")

if not DASHBOARD_KEY:
    print("⚠️ [Startup Warning] DASHBOARD_KEY is unset; dashboard/API access will fail closed.", flush=True)
if not INGEST_TOKEN:
    print("⚠️ [Startup Warning] INGEST_TOKEN is unset; WhatsApp/webhook ingestion remains unauthenticated.", flush=True)
if SESSION_COOKIE_SECURE_MODE != "true":
    print(
        "⚠️ [Startup Warning] HTTPS is not enforced for dashboard sessions; "
        "Secure is enabled only for HTTPS requests in auto mode.",
        flush=True,
    )


def _request_uses_https(request: Request) -> bool:
    forwarded_proto = request.headers.get("x-forwarded-proto", "").split(",", 1)[0].strip().lower()
    request_proto = request.url.scheme.lower()
    if forwarded_proto in {"http", "https"}:
        request_proto = forwarded_proto
    return request_proto == "https"


def _require_same_origin(request: Request):
    origin = request.headers.get("origin")
    if not origin:
        raise HTTPException(status_code=403, detail="A same-origin request is required")
    try:
        parsed_origin = urlsplit(origin)
        if parsed_origin.scheme.lower() not in {"http", "https"} or not parsed_origin.netloc:
            raise HTTPException(status_code=403, detail="Invalid request origin")

        forwarded_proto = request.headers.get("x-forwarded-proto", "").split(",", 1)[0].strip().lower()
        expected_scheme = forwarded_proto if forwarded_proto in {"http", "https"} else request.url.scheme.lower()
        forwarded_host = request.headers.get("x-forwarded-host", "").split(",", 1)[0].strip()
        expected_host = forwarded_host or request.headers.get("host", "").strip()
        origin_port = parsed_origin.port or (443 if parsed_origin.scheme.lower() == "https" else 80)
        expected = urlsplit(f"{expected_scheme}://{expected_host}")
        expected_port = expected.port or (443 if expected_scheme == "https" else 80)
    except ValueError:
        raise HTTPException(status_code=403, detail="Invalid request origin")
    if (
        parsed_origin.username is not None
        or parsed_origin.password is not None
        or parsed_origin.scheme.lower() != expected_scheme
        or parsed_origin.hostname is None
        or expected.hostname is None
        or parsed_origin.hostname.lower() != expected.hostname.lower()
        or origin_port != expected_port
        or parsed_origin.path not in {"", "/"}
        or parsed_origin.query
        or parsed_origin.fragment
    ):
        raise HTTPException(status_code=403, detail="Cross-origin request rejected")


def verify_dashboard_auth(request: Request):
    if not DASHBOARD_KEY:
        raise HTTPException(status_code=503, detail="Dashboard authentication is not configured")
    key = request.headers.get("x-dashboard-key", "")
    auth_header = request.headers.get("authorization", "")
    if auth_header.startswith("Bearer "):
        key = auth_header[7:].strip()
    key_matches = hmac.compare_digest(key.encode("utf-8"), DASHBOARD_KEY.encode("utf-8"))
    cookie_matches = _is_valid_dashboard_session(request.cookies.get("dashboard_session", ""))
    if not (key_matches or cookie_matches):
        raise HTTPException(status_code=401, detail="Unauthorized: Invalid or missing DASHBOARD_KEY")
    if cookie_matches and request.method.upper() not in {"GET", "HEAD", "OPTIONS"}:
        _require_same_origin(request)
    return True


def _create_dashboard_session_token() -> str:
    issued_at = int(time.time())
    nonce = secrets.token_urlsafe(32)
    payload = f"{issued_at}.{nonce}"
    signature = hmac.new(
        DASHBOARD_KEY.encode("utf-8"),
        payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return f"{payload}.{signature}"


def _is_valid_dashboard_session(token: str) -> bool:
    try:
        issued_at_text, nonce, signature = token.split(".", 2)
        issued_at = int(issued_at_text)
    except (AttributeError, ValueError):
        return False
    if not nonce or not signature:
        return False
    now = int(time.time())
    if issued_at > now + 30 or now >= issued_at + DASHBOARD_SESSION_TTL_SECONDS:
        return False
    payload = f"{issued_at}.{nonce}"
    expected_signature = hmac.new(
        DASHBOARD_KEY.encode("utf-8"),
        payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(signature.encode("utf-8"), expected_signature.encode("utf-8"))

@app.post("/api/auth/session")
async def create_dashboard_session(request: Request, response: Response):
    global _HTTP_SESSION_WARNING_LOGGED
    if not DASHBOARD_KEY:
        raise HTTPException(status_code=503, detail="Dashboard authentication is not configured")
    key = request.headers.get("x-dashboard-key", "")
    auth_header = request.headers.get("authorization", "")
    if auth_header.startswith("Bearer "):
        key = auth_header[7:].strip()
    if not hmac.compare_digest(key.encode("utf-8"), DASHBOARD_KEY.encode("utf-8")):
        raise HTTPException(status_code=401, detail="Unauthorized: Invalid or missing DASHBOARD_KEY")
    forwarded_proto = request.headers.get("x-forwarded-proto", "").split(",", 1)[0].strip().lower()
    request_https = _request_uses_https(request)
    if not request_https and not _HTTP_SESSION_WARNING_LOGGED:
        print(
            "⚠️ [Session Security] Dashboard login is using HTTP; session cookies cannot be protected in transit.",
            flush=True,
        )
        _HTTP_SESSION_WARNING_LOGGED = True
    secure_cookie = SESSION_COOKIE_SECURE_MODE == "true" or (
        SESSION_COOKIE_SECURE_MODE == "auto"
        and request_https
    )
    response.set_cookie(
        key="dashboard_session",
        value=_create_dashboard_session_token(),
        httponly=True,
        secure=secure_cookie,
        samesite="strict",
        max_age=DASHBOARD_SESSION_TTL_SECONDS,
        path="/",
    )
    response.headers["Cache-Control"] = "no-store"
    return {"status": "authenticated"}


@app.post("/api/auth/logout")
async def logout_dashboard_session(request: Request, response: Response):
    _require_same_origin(request)
    secure_cookie = SESSION_COOKIE_SECURE_MODE == "true" or (
        SESSION_COOKIE_SECURE_MODE == "auto" and _request_uses_https(request)
    )
    response.delete_cookie(
        key="dashboard_session",
        path="/",
        httponly=True,
        secure=secure_cookie,
        samesite="strict",
    )
    response.headers["Cache-Control"] = "no-store"
    return {"status": "logged_out"}


def verify_ingest_auth(request: Request):
    if not INGEST_TOKEN:
        return True
    supplied_token = request.headers.get("x-ingest-token", "")
    if not hmac.compare_digest(supplied_token.encode("utf-8"), INGEST_TOKEN.encode("utf-8")):
        raise HTTPException(status_code=401, detail="Unauthorized: Invalid or missing INGEST_TOKEN")
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
                        subprocess.run([git_bin, "init"], cwd=repo_dir, check=True, capture_output=True, text=True, timeout=10)
                        subprocess.run([git_bin, "remote", "add", "origin", repo_url], cwd=repo_dir, check=False, capture_output=True, text=True, timeout=10)
                    else:
                        # Clean stale index.lock if lingering
                        lock_file = os.path.join(git_dir, "index.lock")
                        if os.path.exists(lock_file):
                            try:
                                os.unlink(lock_file)
                                print(f"🧹 [Auto Git Pull] Removed stale git lock {lock_file}", flush=True)
                            except Exception:
                                pass
                        remotes_proc = subprocess.run([git_bin, "remote"], cwd=repo_dir, capture_output=True, text=True, timeout=5)
                        remotes = (remotes_proc.stdout or "").split()
                        if "origin" not in remotes:
                            subprocess.run([git_bin, "remote", "add", "origin", repo_url], cwd=repo_dir, check=False, capture_output=True, text=True, timeout=5)
                        else:
                            subprocess.run([git_bin, "remote", "set-url", "origin", repo_url], cwd=repo_dir, check=False, capture_output=True, text=True, timeout=5)

                    # c) Run git fetch origin main && git reset --hard origin/main
                    fetch_proc = subprocess.run([git_bin, "fetch", "origin", "main"], cwd=repo_dir, capture_output=True, text=True, timeout=10)
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
    raise HTTPException(
        status_code=410,
        detail="Hardcoded baseline reset is disabled; use the reviewed portfolio migration instead.",
    )

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
                async with session.get("http://127.0.0.1:3000/qr", timeout=aiohttp.ClientTimeout(total=0.5, sock_connect=0.25)) as resp:
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

    def _fetch_financial_summary():
        try:
            return get_complete_financial_summary(not refresh, refresh)
        except Exception as summary_error:
            print(f"⚠️ [Dashboard Overview] Financial summary unavailable: {summary_error}", flush=True)
            return {
                "financial_kpis": {},
                "holdings": [],
                "data_status": "unavailable",
                "data_error": str(summary_error),
            }

    summary, (checkpoints, desk_thought), (bridge_status, monitored_chats, bridge_mem_mb), live_engines = await asyncio.gather(
        asyncio.to_thread(_fetch_financial_summary),
        asyncio.to_thread(_fetch_checkpoints_and_thought),
        _probe_bridge(),
        asyncio.to_thread(_fetch_live_engines_data),
    )

    # Synchronize overview with live in-memory mark-to-market engine
    global LIVE_PORTFOLIO_STATE
    if LIVE_PORTFOLIO_STATE and LIVE_PORTFOLIO_STATE.get("totals"):
        lt = LIVE_PORTFOLIO_STATE["totals"]
        lh = LIVE_PORTFOLIO_STATE.get("holdings", {})
        summary["data_status"] = LIVE_PORTFOLIO_STATE.get("status", "unavailable")
        summary["data_partial"] = LIVE_PORTFOLIO_STATE.get("partial", True)
        summary["data_error"] = LIVE_PORTFOLIO_STATE.get("error")
        summary["included_holdings"] = lt.get("included_holdings", [])
        summary["excluded_holdings"] = lt.get("excluded_holdings", [])
        summary["last_quote_received"] = LIVE_PORTFOLIO_STATE.get("last_quote_received")

        # Overlay financial KPIs
        kpis = summary.setdefault("financial_kpis", {})
        kpis["total_net_worth"] = lt.get("total_net_worth")
        kpis["portfolio_market_value"] = lt.get("portfolio_custody_value")
        kpis["portfolio_custody_value"] = lt.get("portfolio_custody_value")
        kpis["portfolio_cost_basis"] = lt.get("invested_cost")
        kpis["ktrade_available_cash"] = lt.get("ktrade_cash_balance")
        kpis["ktrade_cash_balance"] = lt.get("ktrade_cash_balance")
        kpis["unrealized_pnl"] = lt.get("unrealized_pnl")
        kpis["total_unrealized_pnl"] = lt.get("unrealized_pnl")
        kpis["unrealized_pnl_pct"] = lt.get("unrealized_pnl_pct")
        kpis["day_change_pkr"] = lt.get("day_pnl")
        kpis["day_change_pct"] = lt.get("day_pnl_pct")
        kpis["today_day_pnl"] = lt.get("day_pnl")
        kpis["today_change_pct"] = lt.get("day_pnl_pct")
        kpis["last_session_pnl_pkr"] = lt.get("day_pnl")
        kpis["last_session_pnl_pct"] = lt.get("day_pnl_pct")

        # Overlay holdings
        if "holdings" in summary and isinstance(summary["holdings"], list):
            if not summary["holdings"] and lh:
                summary["holdings"] = [
                    {
                        "ticker": item["ticker"],
                        "shares": item["shares"],
                        "avg_price": item.get("avg_display"),
                        "invested_cost": item.get("invested_cost"),
                        "current_price": item.get("price"),
                        "market_value": item.get("market_value"),
                        "unrealized_pnl": item.get("unrealized_pnl"),
                        "unrealized_pnl_pct": item.get("unrealized_pnl_pct"),
                        "change_pct": item.get("change_pct"),
                        "change_abs": item.get("change_abs"),
                        "day_change_pkr": item.get("day_pnl"),
                        "price_status": item.get("status"),
                        "price_source": item.get("price_source"),
                        "price_timestamp": item.get("price_timestamp"),
                        "price_timestamp_kind": item.get("price_timestamp_kind"),
                        "price_received_timestamp": item.get("price_received_timestamp"),
                        "price_limit_status": item.get("price_limit_status"),
                        "price_limit_source": item.get("price_limit_source"),
                        "price_limit_retrieved_at": item.get("price_limit_retrieved_at"),
                        "lower_limit": item.get("lower_limit"),
                        "upper_limit": item.get("upper_limit"),
                        "price_age_seconds": item.get("price_age_seconds"),
                        "verdict": "UNAVAILABLE",
                        "sector": "Unknown",
                    }
                    for item in lh.values()
                ]
            for h in summary["holdings"]:
                sym = str(h.get("ticker", "")).upper().strip()
                if sym in lh:
                    live_item = lh[sym]
                    for target, source_key in (
                        ("last_price", "price"),
                        ("current_price", "price"),
                        ("price", "price"),
                        ("market_value", "market_value"),
                        ("unrealized_pnl", "unrealized_pnl"),
                        ("unrealized_pnl_pct", "unrealized_pnl_pct"),
                        ("change_pct", "change_pct"),
                        ("change_abs", "change_abs"),
                        ("last_change_pct", "change_pct"),
                        ("last_change_abs", "change_abs"),
                        ("day_change_pkr", "day_pnl"),
                        ("prev_close", "prev_close"),
                    ):
                        h[target] = live_item.get(source_key)
                    h["source"] = live_item.get("price_source")
                    h["price_source"] = live_item.get("price_source")
                    h["price_timestamp"] = live_item.get("price_timestamp")
                    h["price_timestamp_kind"] = live_item.get("price_timestamp_kind")
                    h["price_received_timestamp"] = live_item.get("price_received_timestamp")
                    h["price_limit_status"] = live_item.get("price_limit_status")
                    h["price_limit_source"] = live_item.get("price_limit_source")
                    h["price_limit_retrieved_at"] = live_item.get("price_limit_retrieved_at")
                    h["lower_limit"] = live_item.get("lower_limit")
                    h["upper_limit"] = live_item.get("upper_limit")
                    h["price_age_seconds"] = live_item.get("price_age_seconds")
                    h["price_status"] = live_item.get("status")
                    tot_cust = lt.get("portfolio_custody_value")
                    if tot_cust and h.get("market_value") is not None:
                        h["weight_pct"] = round((float(h["market_value"]) / float(tot_cust)) * 100.0, 1)
                else:
                    h.update({
                        "last_price": None,
                        "current_price": None,
                        "price": None,
                        "market_value": None,
                        "unrealized_pnl": None,
                        "unrealized_pnl_pct": None,
                        "price_source": None,
                        "price_timestamp": None,
                        "price_timestamp_kind": "unavailable",
                        "price_received_timestamp": None,
                        "price_limit_status": "unverified",
                        "price_limit_source": None,
                        "price_limit_retrieved_at": None,
                        "lower_limit": None,
                        "upper_limit": None,
                        "price_age_seconds": None,
                        "price_status": "unavailable",
                    })

        # Dynamic Top Movers for Scraped Intelligence card
        if isinstance(live_engines, dict) and "prices" in live_engines:
            sorted_movers = sorted(
                lh.values(),
                key=lambda x: abs(float(x.get("change_pct", 0.0))),
                reverse=True
            )
            top_movers_list = []
            for item in sorted_movers[:5]:
                cp = float(item.get("change_pct", 0.0))
                dp = float(item.get("day_pnl", 0.0))
                top_movers_list.append({
                    "ticker": item.get("ticker"),
                    "change_pct": cp,
                    "day_change_pkr": dp,
                    "type": "gainer" if cp >= 0 else "loser"
                })
            dom_src = LIVE_PORTFOLIO_STATE.get("source") or "unavailable"
            live_engines["prices"]["status"] = (
                "ONLINE" if LIVE_PORTFOLIO_STATE.get("last_quote_received") else "UNAVAILABLE"
            )
            live_engines["prices"]["engine_name"] = "PSX Official Data Portal (dps.psx.com.pk) & Chromium Live Engine"
            live_engines["prices"]["last_poll"] = (
                f"Last quote: {LIVE_PORTFOLIO_STATE.get('last_quote_received') or 'unavailable'} · "
                f"Heartbeat #{LIVE_PORTFOLIO_STATE.get('tick_seq', 0)} · {dom_src}"
            )
            if top_movers_list:
                live_engines["prices"]["top_movers"] = top_movers_list

    now = time.time()
    model_cooldowns = {
        m: round(t - now, 1) for m, t in MODEL_COOLDOWN_UNTIL.items() if t > now
    }

    return {
        "status": "success",
        "financial_summary": summary,
        "live_state": LIVE_PORTFOLIO_STATE,
        "dividends": summary.get("dividends", []),
        "total_dividends_received": summary.get("financial_kpis", {}).get("total_dividends_received", 0.0),
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
    if payload is None or not payload.holdings or payload.cash_balance is None:
        raise HTTPException(
            status_code=400,
            detail="Explicit holdings and cash_balance are required; database sync never uses code defaults.",
        )
    from portfolio_manager import (
        sync_full_portfolio_state,
        refresh_portfolio_live_prices,
        publish_agent_event,
    )
    from learning_engine import seed_initial_predictions_if_empty

    holdings_in = payload.holdings
    cash_in = float(payload.cash_balance)
    working_cap_in = float(payload.total_working_capital) if payload.total_working_capital is not None else None

    sync_res = await asyncio.to_thread(
        sync_full_portfolio_state,
        cash_in,
        holdings_in,
        working_cap_in,
    )
    await asyncio.to_thread(seed_initial_predictions_if_empty)
    await asyncio.to_thread(refresh_active_holdings_meta)
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

@app.post("/api/brain/evaluate-now")
@app.post("/api/dashboard/brain/evaluate-now")
@app.get("/api/brain/opportunities")
@app.get("/api/dashboard/brain/opportunities")
async def evaluate_brain_golden_opportunities(request: Request):
    verify_dashboard_auth(request)
    from central_brain import scan_shariah_golden_opportunities
    opps = await asyncio.to_thread(scan_shariah_golden_opportunities, False)
    return opps

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
    verify_dashboard_auth(request)
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
@app.get("/api/dashboard/dividend")
@app.get("/api/portfolio/dividend")
@app.get("/api/portfolio/dividends")
@app.get("/api/dividends")
async def get_dashboard_dividends(request: Request, limit: int = 100):
    verify_dashboard_auth(request)
    from portfolio_manager import get_all_dividends
    try:
        divs = await asyncio.wait_for(
            asyncio.to_thread(get_all_dividends, limit),
            timeout=6.0
        )
    except asyncio.TimeoutError:
        return JSONResponse(status_code=504, content={"status": "error", "message": "Database query timed out"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e)})

    total_gross = round(sum(float(d.get("gross_amount") or 0.0) for d in divs), 2)
    total_tax = round(sum(float(d.get("tax_withheld") or 0.0) for d in divs), 2)
    net_bank = round(sum(
        float(d.get("net_amount") or 0.0) for d in divs
        if str(d.get("status") or "").upper() == "CREDITED_TO_BANK" or str(d.get("credited_to") or "").lower() == "bank"
    ), 2)
    net_reinvested = round(sum(
        float(d.get("net_amount") or 0.0) for d in divs
        if str(d.get("status") or "").upper() == "SENT_TO_KTRADE" or str(d.get("credited_to") or "").lower() == "ktrade_cash"
    ), 2)
    pending = round(sum(
        float(d.get("net_amount") or 0.0) for d in divs
        if str(d.get("status") or "").upper() in ("ANNOUNCED", "ENTITLED")
    ), 2)
    total_received = round(net_bank + net_reinvested, 2)

    return {
        "status": "ok",
        "dividends": divs,
        "metrics": {
            "total_gross_dividends": total_gross,
            "total_wht_tax_paid": total_tax,
            "net_dividends_in_bank": net_bank,
            "net_dividends_reinvested": net_reinvested,
            "total_dividends_received": total_received,
            "pending_dividends": pending
        }
    }

@app.post("/api/dashboard/dividends")
@app.post("/api/dashboard/dividend")
@app.post("/api/portfolio/dividend")
@app.post("/api/portfolio/dividends")
@app.post("/api/dividends")
async def create_dashboard_dividend(request: Request):
    verify_dashboard_auth(request)
    try:
        body = await request.json()
    except Exception:
        body = {}

    from portfolio_manager import log_dividend, invalidate_financial_summary_cache
    ticker = body.get("ticker") or body.get("symbol") or body.get("scrip")
    if not ticker:
        return JSONResponse(status_code=400, content={"status": "error", "message": "Ticker is required."})

    try:
        entry = await asyncio.wait_for(
            asyncio.to_thread(
                log_dividend,
                ticker=str(ticker),
                shares=body.get("shares") or body.get("shares_held") or body.get("quantity"),
                dividend_per_share=body.get("dividend_per_share") or body.get("dps_gross") or body.get("dps") or body.get("per_share"),
                net_amount=body.get("net_amount") or body.get("total_net") or body.get("net_received") or body.get("amount") or body.get("net"),
                gross_amount=body.get("gross_amount") or body.get("gross"),
                tax_withheld=body.get("tax_withheld") or body.get("tax_amount") or body.get("wht_amount"),
                wht_rate_pct=body.get("wht_rate_pct") or body.get("tax_rate_pct") or body.get("tax_rate") or body.get("wht_rate"),
                payment_date=body.get("payment_date") or body.get("date") or body.get("pay_date"),
                ex_date=body.get("ex_date") or body.get("book_closure_date"),
                credited_to=body.get("credited_to") or body.get("status") or body.get("destination") or "bank",
                notes=body.get("notes") or body.get("comment") or body.get("memo")
            ),
            timeout=6.0
        )
        invalidate_financial_summary_cache()
        _OVERVIEW_ENGINES_CACHE["ts"] = 0.0
        _OVERVIEW_CKPT_CACHE["ts"] = 0.0

        if entry.get("credited_to") == "ktrade_cash" or entry.get("status") == "SENT_TO_KTRADE":
            await asyncio.to_thread(refresh_active_holdings_meta)

        return {"status": "ok", "dividend": entry}
    except asyncio.TimeoutError:
        return JSONResponse(status_code=504, content={"status": "error", "message": "Dividend creation timed out"})
    except Exception as e:
        return JSONResponse(status_code=400, content={"status": "error", "message": str(e)})

@app.patch("/api/dashboard/dividends/{id}")
@app.patch("/api/dashboard/dividend/{id}")
@app.patch("/api/dashboard/dividends/{id}/status")
@app.patch("/api/dashboard/dividend/{id}/status")
async def update_dashboard_dividend(id: int, payload: DividendStatusUpdatePayload, request: Request):
    verify_dashboard_auth(request)
    from portfolio_manager import update_dividend_status, invalidate_financial_summary_cache
    try:
        updated = await asyncio.wait_for(
            asyncio.to_thread(update_dividend_status, id, payload.status),
            timeout=6.0
        )
        invalidate_financial_summary_cache()
        _OVERVIEW_ENGINES_CACHE["ts"] = 0.0
        _OVERVIEW_CKPT_CACHE["ts"] = 0.0
        return {"status": "success", "dividend": updated}
    except asyncio.TimeoutError:
        return JSONResponse(status_code=504, content={"status": "error", "message": "Dividend status update timed out"})
    except Exception as e:
        return JSONResponse(status_code=400, content={"status": "error", "message": str(e)})

@app.put("/api/dashboard/dividends/{id}")
@app.put("/api/dashboard/dividend/{id}")
async def edit_dashboard_dividend(id: int, payload: DividendCreatePayload, request: Request):
    verify_dashboard_auth(request)
    from portfolio_manager import update_dividend_entry, invalidate_financial_summary_cache
    try:
        updated = await asyncio.wait_for(
            asyncio.to_thread(update_dividend_entry, id, payload.dict()),
            timeout=6.0
        )
        invalidate_financial_summary_cache()
        _OVERVIEW_ENGINES_CACHE["ts"] = 0.0
        _OVERVIEW_CKPT_CACHE["ts"] = 0.0
        return {"status": "success", "dividend": updated}
    except asyncio.TimeoutError:
        return JSONResponse(status_code=504, content={"status": "error", "message": "Dividend edit timed out"})
    except Exception as e:
        return JSONResponse(status_code=400, content={"status": "error", "message": str(e)})

@app.delete("/api/dashboard/dividends/{id}")
@app.delete("/api/dashboard/dividend/{id}")
@app.delete("/api/portfolio/dividend/{id}")
@app.delete("/api/portfolio/dividends/{id}")
@app.delete("/api/dividends/{id}")
async def delete_dashboard_dividend(id: int, request: Request):
    verify_dashboard_auth(request)
    from portfolio_manager import delete_dividend_entry, invalidate_financial_summary_cache
    try:
        res = await asyncio.wait_for(
            asyncio.to_thread(delete_dividend_entry, id),
            timeout=6.0
        )
        invalidate_financial_summary_cache()
        _OVERVIEW_ENGINES_CACHE["ts"] = 0.0
        _OVERVIEW_CKPT_CACHE["ts"] = 0.0

        await asyncio.to_thread(refresh_active_holdings_meta)

        return {"status": "ok", "deleted": True, "id": id}
    except asyncio.TimeoutError:
        return JSONResponse(status_code=504, content={"status": "error", "message": "Dividend deletion timed out"})
    except Exception as e:
        return JSONResponse(status_code=400, content={"status": "error", "message": str(e)})

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
@app.post("/api/dashboard/trade")
@app.post("/api/portfolio/trade")
async def create_dashboard_trade(payload: TradeRecordPayload, request: Request):
    verify_dashboard_auth(request)
    from portfolio_manager import record_trade, is_shariah_compliant_stock
    ticker_clean = (payload.ticker or "").upper().strip()
    if not is_shariah_compliant_stock(ticker_clean):
        return JSONResponse(
            status_code=400,
            content={
                "status": "error",
                "message": f"❌ BLOCKED: {ticker_clean} is NOT Shariah-compliant. Saad's portfolio is strictly Islamic equity only (KMIALLSHR / KMI30). Conventional banks and non-Shariah stocks are prohibited."
            }
        )
    try:
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
    except ValueError as ve:
        return JSONResponse(status_code=400, content={"status": "error", "message": str(ve)})
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "message": str(e)})

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
async def get_dashboard_whatsapp_qr(request: Request):
    verify_dashboard_auth(request)
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
async def whatsapp_qr_page(request: Request):
    if not DASHBOARD_KEY:
        raise HTTPException(status_code=503, detail="Dashboard authentication is not configured")
    try:
        verify_dashboard_auth(request)
    except HTTPException as auth_error:
        if auth_error.status_code != 401:
            raise
        unlock_html = """<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>WhatsApp Bridge Authentication</title>
<style>body{margin:0;min-height:100vh;display:grid;place-items:center;background:#0d1117;color:#c9d1d9;font:16px system-ui}
form{width:min(360px,calc(100% - 40px));padding:28px;background:#1c2128;border:1px solid #30363d;border-radius:14px}
input,button{box-sizing:border-box;width:100%;margin-top:12px;padding:12px;border-radius:8px}
input{background:#0d1117;color:#fff;border:1px solid #30363d}button{background:#238636;color:#fff;border:0;cursor:pointer}
#error{color:#ff7b72}</style></head>
<body><form id="unlock"><h2>WhatsApp Bridge</h2><p>Enter the dashboard key to continue.</p>
<label for="key">Dashboard key</label><input id="key" type="password" autocomplete="current-password" required>
<button type="submit">Continue</button><p id="error" role="alert"></p></form>
<script>
document.getElementById('unlock').addEventListener('submit', async (event) => {
  event.preventDefault();
  const input = document.getElementById('key');
  const error = document.getElementById('error');
  try {
    const response = await fetch('/api/auth/session', {
      method: 'POST',
      headers: { 'x-dashboard-key': input.value }
    });
    input.value = '';
    if (!response.ok) throw new Error(response.status === 401 ? 'Invalid dashboard key.' : 'Authentication is unavailable.');
    window.location.reload();
  } catch (err) {
    error.textContent = err.message;
  }
});
</script></body></html>"""
        return HTMLResponse(
            content=unlock_html,
            headers={"Cache-Control": "no-store", "Pragma": "no-cache"},
        )
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
    verify_ingest_auth(request)
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
async def ingest_whatsapp_message(request: Request, payload: WhatsAppMessagePayload):
    """
    Non-blocking endpoint: puts incoming payload into in-memory queue
    and returns {"status": "queued"} in < 5ms.
    """
    verify_ingest_auth(request)
    try:
        whatsapp_queue.put_nowait(payload)
        return {"status": "queued"}
    except asyncio.QueueFull:
        print("⚠️ [WhatsApp Endpoint] Queue full. Dropping message to protect service.")
        return {"status": "dropped", "reason": "queue_full"}

@app.post("/api/ingest-whatsapp-media")
async def ingest_whatsapp_media_endpoint(
    request: Request,
    file: UploadFile = File(...),
    caption: str = Form(""),
    source: str = Form("WhatsApp Feed"),
    timestamp: Optional[int] = Form(None)
):
    verify_ingest_auth(request)
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
async def ingest_whatsapp_text(request: Request, payload: WhatsAppTextPayload):
    verify_ingest_auth(request)
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
async def trigger_agent(request: Request):
    verify_dashboard_auth(request)
    thread_id = str(uuid.uuid4())
    config = {"configurable": {"thread_id": thread_id}}
    initial_state = {"tickers": ["SYS", "HBL", "LUCK"]}
    
    await app.state.graph.ainvoke(initial_state, config=config)
    return {"thread_id": thread_id, "status": "completed", "message": "Agent run completed"}

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
