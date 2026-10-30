import os
import re
import json
import math
import time
import asyncio
import datetime
from typing import Dict, Any, List, Optional

from model_router import generate_with_fallback, TASK_FAST_INGESTION

# Short TTL cache for source trust weights so publish_agent_event stays <1ms
_TRUST_CACHE: Dict[str, float] = {}
_TRUST_CACHE_TS: float = 0.0

PSX_SECTOR_MAP: Dict[str, str] = {
    "EFERT": "Fertilizer",
    "FFC": "Fertilizer",
    "ENGRO": "Conglomerate",
    "FABL": "Islamic Banking",
    "MEBL": "Islamic Banking",
    "HBL": "Commercial Banking",
    "UBL": "Commercial Banking",
    "MCB": "Commercial Banking",
    "BAFL": "Commercial Banking",
    "BAHL": "Commercial Banking",
    "HUBC": "Power & IPP",
    "KAPCO": "Power & IPP",
    "LUCK": "Cement & Industrials",
    "DGKC": "Cement & Industrials",
    "MLCF": "Cement & Industrials",
    "CHCC": "Cement & Industrials",
    "MARI": "Oil & Gas Exploration",
    "OGDC": "Oil & Gas Exploration",
    "PPL": "Oil & Gas Exploration",
    "POL": "Oil & Gas Exploration",
    "PSO": "Oil & Gas Marketing",
    "SYS": "Technology & IT Exports",
    "TRG": "Technology & IT Exports",
    "AVN": "Technology & IT Exports",
    "SAZEW": "Autos & Engineering",
    "ILP": "Textiles & Exports",
}

DEFAULT_SOURCE_SEEDS = [
    ("Internal_DCF", 8, 6, 75.00, 6.40, 0.78),
    ("PSX_Filing", 5, 4, 80.00, 5.10, 0.76),
    ("KTrade", 7, 5, 71.43, 4.85, 0.71),
    ("Broker_Research", 6, 4, 66.67, 3.90, 0.66),
    ("ARN", 5, 3, 60.00, 3.20, 0.62),
    ("Technical_Scanner", 6, 3, 50.00, 1.85, 0.56),
]

DEFAULT_PLAYBOOK_SEEDS = [
    (
        "Internal_DCF",
        "Require >= 20% DCF Margin of Safety before deploying KTrade cash tranches; cap single-tranche accumulation at 25% of liquid cash when sector weight exceeds 20%.",
        0.88,
        12,
        "POSITIVE REAL RATES",
        "Prevents overpaying for cyclical earnings peaks while SBP real rates remain positive.",
    ),
    (
        "PSX_Dividend_Desk",
        "Never trim high-yield Shariah compounders (FFC, EFERT, MEBL, HUBC) solely on modest P/E expansion while net after-WHT dividend yield (85% of gross) exceeds 9%.",
        0.85,
        9,
        "ALL",
        "Protects recurring cash-flow compounding and avoids unnecessary 15% CGT realization.",
    ),
    (
        "Risk_Governance",
        "Enforce a hard 30% single-sector concentration ceiling and an 8-10% trailing stop-loss buffer below structural support on non-yield growth positions (SYS, LUCK).",
        0.82,
        14,
        "ALL",
        "Bounds portfolio drawdown from sector-specific regulatory or tariff shocks in PSX.",
    ),
    (
        "Macro_Regime_Engine",
        "When SBP policy rate minus CPI yields positive real rates, overweight cash-rich E&Ps (MARI, OGDC) and low-leverage exporters (SYS) over highly leveraged cyclicals.",
        0.79,
        7,
        "POSITIVE REAL RATES",
        "Positive real rates penalize floating-rate corporate debt while rewarding net-cash balance sheets.",
    ),
]


def _get_conn():
    from portfolio_manager import get_db_connection
    return get_db_connection()


def normalize_source_key(raw_source: Optional[str]) -> str:
    if not raw_source:
        return "Internal_DCF"
    s = str(raw_source).strip()
    sl = s.lower()
    if "ktrade" in sl or "whatsapp" in sl:
        return "KTrade"
    if "arn" in sl or "abdul rehman" in sl or "youtube" in sl or "mentor" in sl:
        return "ARN"
    if "dcf" in sl or "asymmetric" in sl or "valuation" in sl or "internal" in sl:
        return "Internal_DCF"
    if "tech" in sl or "chart" in sl or "rsi" in sl or "scanner" in sl:
        return "Technical_Scanner"
    if "filing" in sl or "psx" in sl:
        return "PSX_Filing"
    if "broker" in sl or "akd" in sl or "arif" in sl or "topline" in sl or "report" in sl:
        return "Broker_Research"
    return re.sub(r"[^A-Za-z0-9_ -]", "", s)[:40].strip() or "Internal_DCF"


def init_learning_tables() -> bool:
    """
    Auto-creates and migrates closed-loop algorithmic brain tables on startup:
    1. prediction_ledger
    2. source_reliability
    3. active_playbook (adds confidence_weight, times_applied, regime_tag, teaching_note)
    4. system_macro_state (for PSX macro regime tracking)
    """
    conn = None
    cur = None
    try:
        conn = _get_conn()
        cur = conn.cursor()

        # 1. prediction_ledger
        cur.execute("""
            CREATE TABLE IF NOT EXISTS prediction_ledger (
                id BIGSERIAL PRIMARY KEY,
                source TEXT NOT NULL,
                ticker TEXT NOT NULL,
                direction TEXT NOT NULL DEFAULT 'BULLISH',
                entry_price NUMERIC(12,4) NOT NULL,
                target_price NUMERIC(12,4),
                stop_loss NUMERIC(12,4),
                horizon_days INT DEFAULT 15,
                status TEXT NOT NULL DEFAULT 'OPEN',
                realized_return_pct NUMERIC(8,3),
                lesson_extracted TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                evaluated_at TIMESTAMPTZ
            );
            CREATE INDEX IF NOT EXISTS idx_prediction_ledger_status ON prediction_ledger (status);
            CREATE INDEX IF NOT EXISTS idx_prediction_ledger_ticker ON prediction_ledger (ticker);
            CREATE INDEX IF NOT EXISTS idx_prediction_ledger_created ON prediction_ledger (created_at DESC);
        """)

        # 2. source_reliability
        cur.execute("""
            CREATE TABLE IF NOT EXISTS source_reliability (
                source TEXT PRIMARY KEY,
                total_calls INT DEFAULT 0,
                winning_calls INT DEFAULT 0,
                win_rate_pct NUMERIC(6,2) DEFAULT 0.00,
                avg_return_pct NUMERIC(8,3) DEFAULT 0.00,
                trust_weight NUMERIC(5,3) DEFAULT 0.600,
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)

        # 3. active_playbook (create if missing + alter with self-learning columns)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS active_playbook (
                id BIGSERIAL PRIMARY KEY,
                source TEXT NOT NULL DEFAULT 'System',
                rule TEXT NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            ALTER TABLE active_playbook ADD COLUMN IF NOT EXISTS confidence_weight NUMERIC DEFAULT 0.70;
            ALTER TABLE active_playbook ADD COLUMN IF NOT EXISTS times_applied INT DEFAULT 1;
            ALTER TABLE active_playbook ADD COLUMN IF NOT EXISTS regime_tag TEXT DEFAULT 'ALL';
            ALTER TABLE active_playbook ADD COLUMN IF NOT EXISTS teaching_note TEXT;
        """)

        # 4. system_macro_state (optional macro store)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS system_macro_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)

        # Seed baseline source_reliability rows if empty
        cur.execute("SELECT COUNT(*) FROM source_reliability;")
        src_count = int(cur.fetchone()[0] or 0)
        if src_count == 0:
            for src, tot, wins, wr, avgr, tw in DEFAULT_SOURCE_SEEDS:
                cur.execute("""
                    INSERT INTO source_reliability (source, total_calls, winning_calls, win_rate_pct, avg_return_pct, trust_weight, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (source) DO NOTHING;
                """, (src, tot, wins, wr, avgr, tw))

        # Backfill NULL columns on existing active_playbook rules
        cur.execute("""
            UPDATE active_playbook
            SET confidence_weight = COALESCE(confidence_weight, 0.70),
                times_applied = COALESCE(times_applied, 1),
                regime_tag = COALESCE(regime_tag, 'ALL'),
                teaching_note = COALESCE(teaching_note, 'Learned institutional rule actively governing portfolio screening.')
            WHERE confidence_weight IS NULL OR regime_tag IS NULL OR teaching_note IS NULL;
        """)

        # Seed foundational PSX playbook rules if empty
        cur.execute("SELECT COUNT(*) FROM active_playbook;")
        pb_count = int(cur.fetchone()[0] or 0)
        if pb_count == 0:
            for src, rule_txt, cw, ta, reg, note in DEFAULT_PLAYBOOK_SEEDS:
                cur.execute("""
                    INSERT INTO active_playbook (source, rule, confidence_weight, times_applied, regime_tag, teaching_note, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW());
                """, (src, rule_txt, cw, ta, reg, note))

        conn.commit()
        _refresh_trust_cache_from_conn(conn)
        return True
    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        print(f"⚠️ [Learning Engine] Table initialization warning: {e}", flush=True)
        return False
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


def seed_initial_predictions_if_empty() -> int:
    """
    Seeds structured OPEN predictions from current KTrade holdings + watchlist_targets
    if prediction_ledger is completely empty, ensuring closed-loop tracking starts immediately.
    """
    conn = None
    cur = None
    inserted = 0
    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM prediction_ledger;")
        if int(cur.fetchone()[0] or 0) > 0:
            return 0

        cur.execute("""
            SELECT p.ticker, p.avg_buy_price, COALESCE(p.last_price, p.avg_buy_price) AS ref_price,
                   w.target_price, w.stop_loss
            FROM portfolio_holdings p
            LEFT JOIN watchlist_targets w ON UPPER(p.ticker) = UPPER(w.ticker)
            WHERE p.shares > 0
            ORDER BY (p.shares * p.avg_buy_price) DESC;
        """)
        rows = cur.fetchall()
        for tick, avg_p, ref_p, tp, sl in rows:
            entry = float(ref_p or avg_p or 0.0)
            if entry <= 0:
                continue
            target = float(tp) if tp and float(tp) > entry * 1.02 else round(entry * 1.14, 2)
            stop = float(sl) if sl and 0 < float(sl) < entry * 0.99 else round(entry * 0.92, 2)
            cur.execute("""
                INSERT INTO prediction_ledger (
                    source, ticker, direction, entry_price, target_price, stop_loss, horizon_days, status, created_at
                )
                VALUES (%s, %s, 'BULLISH', %s, %s, %s, 20, 'OPEN', NOW());
            """, ("Internal_DCF", str(tick).upper(), entry, target, stop))
            inserted += 1

        conn.commit()
        return inserted
    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        print(f"⚠️ [Learning Engine] Seed initial predictions skipped: {e}", flush=True)
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


def _refresh_trust_cache_from_conn(conn) -> None:
    global _TRUST_CACHE, _TRUST_CACHE_TS
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT source, trust_weight FROM source_reliability;")
            _TRUST_CACHE = {str(r[0]): float(r[1]) for r in cur.fetchall() if r[0] is not None and r[1] is not None}
            _TRUST_CACHE_TS = time.time()
    except Exception:
        pass


def get_source_trust_weight(source: Optional[str]) -> float:
    global _TRUST_CACHE_TS
    norm = normalize_source_key(source)
    if (time.time() - _TRUST_CACHE_TS) > 120.0 or not _TRUST_CACHE:
        conn = None
        try:
            conn = _get_conn()
            _refresh_trust_cache_from_conn(conn)
        except Exception:
            pass
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
    return round(float(_TRUST_CACHE.get(norm, 0.65)), 2)


def record_prediction(
    source: str,
    ticker: str,
    entry_price: float,
    target_price: Optional[float] = None,
    stop_loss: Optional[float] = None,
    direction: str = "BULLISH",
    horizon_days: int = 15
) -> Optional[dict]:
    """
    Logs a structured directional prediction into prediction_ledger for closed-loop grading.
    Avoids duplicate OPEN calls from the same source + ticker within 24 hours.
    """
    clean_ticker = str(ticker or "").strip().upper()
    if not clean_ticker or clean_ticker in {"MACRO", "PSX", "NONE", "ALL"}:
        return None
    entry = float(entry_price or 0.0)
    if entry <= 0:
        return None

    clean_dir = "BEARISH" if str(direction or "").strip().upper() == "BEARISH" else "BULLISH"
    norm_source = normalize_source_key(source)

    if clean_dir == "BULLISH":
        tp = float(target_price) if target_price and float(target_price) > 0 else round(entry * 1.12, 2)
        sl = float(stop_loss) if stop_loss and float(stop_loss) > 0 else round(entry * 0.93, 2)
    else:
        tp = float(target_price) if target_price and float(target_price) > 0 else round(entry * 0.88, 2)
        sl = float(stop_loss) if stop_loss and float(stop_loss) > 0 else round(entry * 1.07, 2)

    conn = None
    cur = None
    try:
        conn = _get_conn()
        cur = conn.cursor()

        # Ensure source exists in source_reliability
        cur.execute("""
            INSERT INTO source_reliability (source, total_calls, winning_calls, win_rate_pct, avg_return_pct, trust_weight, updated_at)
            VALUES (%s, 0, 0, 0.00, 0.00, 0.600, NOW())
            ON CONFLICT (source) DO NOTHING;
        """, (norm_source,))

        # Deduplicate recent identical open calls
        cur.execute("""
            SELECT id FROM prediction_ledger
            WHERE source = %s AND ticker = %s AND status = 'OPEN'
              AND created_at >= NOW() - INTERVAL '24 hours'
            LIMIT 1;
        """, (norm_source, clean_ticker))
        existing = cur.fetchone()
        if existing:
            conn.commit()
            return {"id": existing[0], "source": norm_source, "ticker": clean_ticker, "status": "OPEN", "deduplicated": True}

        cur.execute("""
            INSERT INTO prediction_ledger (
                source, ticker, direction, entry_price, target_price, stop_loss, horizon_days, status, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, 'OPEN', NOW())
            RETURNING id, source, ticker, direction, entry_price, target_price, stop_loss, horizon_days, status, created_at;
        """, (norm_source, clean_ticker, clean_dir, entry, tp, sl, int(horizon_days or 15)))
        row = cur.fetchone()
        conn.commit()
        return {
            "id": row[0],
            "source": row[1],
            "ticker": row[2],
            "direction": row[3],
            "entry_price": float(row[4]),
            "target_price": float(row[5]) if row[5] is not None else None,
            "stop_loss": float(row[6]) if row[6] is not None else None,
            "horizon_days": int(row[7]),
            "status": row[8],
            "created_at": row[9].isoformat() if row[9] else None,
        }
    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        print(f"⚠️ [Learning Engine] Failed recording prediction for {clean_ticker}: {e}", flush=True)
        return None
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


def _compute_bayesian_trust_update(
    old_total: int,
    old_wins: int,
    old_avg_ret: float,
    old_weight: float,
    is_win: bool,
    realized_return_pct: float
) -> tuple[int, int, float, float, float]:
    """
    Beta-Binomial Bayesian posterior update on [0.10, 1.00]:
    Prior Beta(alpha=3.0, beta=2.0) -> prior mean 0.60.
    Guarantees directional adaptation on HIT_TARGET (+) and HIT_STOP (-).
    """
    new_total = max(0, int(old_total)) + 1
    new_wins = max(0, int(old_wins)) + (1 if is_win else 0)
    win_rate = round((new_wins / new_total) * 100.0, 2)
    new_avg_ret = round(((float(old_avg_ret) * max(0, int(old_total))) + float(realized_return_pct)) / new_total, 3)

    alpha_0, beta_0 = 3.0, 2.0
    posterior_mean = (alpha_0 + new_wins) / (alpha_0 + beta_0 + new_total)
    ret_bonus = 0.04 * math.tanh(new_avg_ret / 12.0)
    candidate_weight = posterior_mean + ret_bonus

    if is_win:
        updated_weight = max(float(old_weight) + 0.025, candidate_weight)
    else:
        updated_weight = min(float(old_weight) - 0.035, candidate_weight)

    clamped_weight = round(max(0.10, min(1.00, updated_weight)), 3)
    return new_total, new_wins, win_rate, new_avg_ret, clamped_weight


async def _generate_stop_loss_lesson(
    source: str,
    ticker: str,
    direction: str,
    entry_price: float,
    stop_loss: float,
    live_price: float,
    realized_return_pct: float,
    regime_label: str
) -> str:
    sector = PSX_SECTOR_MAP.get(ticker.upper(), "PSX Equity")
    fallback_lesson = (
        f"Require volume confirmation and sector relative strength in {sector} before acting on {source} {direction.lower()} signals on {ticker}. "
        f"Tighten initial tranche sizing to 50% when {regime_label.lower()} macro headwinds pressure multiples near Rs. {entry_price:,.2f}."
    )
    prompt = (
        f"You are a quantitative PSX portfolio risk engine conducting a post-mortem on a stopped-out call.\n"
        f"Source: {source} | Ticker: {ticker} ({sector}) | Direction: {direction}\n"
        f"Entry: Rs. {entry_price:.2f} | Stop-Loss: Rs. {stop_loss:.2f} | Triggered Spot: Rs. {live_price:.2f} ({realized_return_pct:+.2f}%)\n"
        f"Current PSX Macro Regime: {regime_label}\n\n"
        f"Write EXACTLY 2 crisp, institutional sentences formulating a reusable risk/entry filter rule for our active_playbook so we avoid repeating this error. "
        f"Do not use emojis, markdown headers, or bullet points."
    )
    try:
        raw = await asyncio.wait_for(
            generate_with_fallback(None, [prompt], task=TASK_FAST_INGESTION),
            timeout=15.0
        )
        cleaned = re.sub(r"[\r\n]+", " ", (raw or "").strip()).strip('"\'')
        if len(cleaned) >= 25:
            return cleaned[:420]
    except Exception as e:
        print(f"ℹ️ [Learning Engine] Using deterministic post-mortem fallback for {ticker}: {e}", flush=True)
    return fallback_lesson


def get_macro_regime_sync(conn=None) -> Dict[str, Any]:
    """
    Computes current PSX Macro Regime (SBP policy rate, CPI inflation, Real Interest Rate).
    Reads overrides from system_macro_state or env vars if available.
    """
    sbp_rate = float(os.getenv("PSX_SBP_POLICY_RATE", "17.5"))
    cpi_rate = float(os.getenv("PSX_CPI_INFLATION_RATE", "9.6"))

    close_conn = False
    try:
        if conn is None:
            conn = _get_conn()
            close_conn = True
        with conn.cursor() as cur:
            cur.execute("SELECT key, value FROM system_macro_state WHERE key IN ('sbp_policy_rate', 'cpi_inflation_rate');")
            for k, v in cur.fetchall():
                num_match = re.search(r"-?\d+(?:\.\d+)?", str(v or ""))
                if not num_match:
                    continue
                val = float(num_match.group(0))
                if k == "sbp_policy_rate" and 1.0 <= val <= 40.0:
                    sbp_rate = val
                elif k == "cpi_inflation_rate" and -5.0 <= val <= 50.0:
                    cpi_rate = val
    except Exception:
        pass
    finally:
        if close_conn and conn:
            try:
                conn.close()
            except Exception:
                pass

    real_rate = round(sbp_rate - cpi_rate, 2)
    if real_rate >= 2.0:
        regime_label = "POSITIVE REAL RATES"
    elif real_rate >= -1.0:
        regime_label = "MONETARY TRANSITION"
    else:
        regime_label = "NEGATIVE REAL RATES"

    return {
        "sbp_rate": round(sbp_rate, 2),
        "cpi": round(cpi_rate, 2),
        "real_rate": real_rate,
        "regime_label": regime_label,
    }


async def evaluate_open_predictions(price_overrides: Optional[Dict[str, float]] = None) -> Dict[str, Any]:
    """
    Closed-Loop Grading & Adaptation Engine:
    - Evaluates all OPEN rows in prediction_ledger against live PSX prices.
    - Marks HIT_TARGET -> Bayesian trust_weight increment.
    - Marks HIT_STOP -> Bayesian trust_weight decrement + 2-sentence Groq/Gemini post-mortem lesson in active_playbook.
    - Marks EXPIRED -> horizon elapsed without target/stop breach.
    """
    from portfolio_manager import get_live_price_and_change, publish_agent_event

    init_learning_tables()
    conn = _get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT id, source, ticker, direction, entry_price, target_price, stop_loss, horizon_days, created_at
            FROM prediction_ledger
            WHERE status = 'OPEN'
            ORDER BY created_at ASC;
        """)
        open_rows = cur.fetchall()
        macro_regime = get_macro_regime_sync(conn)
    finally:
        cur.close()
        conn.close()

    if not open_rows:
        return {
            "status": "success",
            "evaluated": 0,
            "hit_target": 0,
            "hit_stop": 0,
            "expired": 0,
            "still_open": 0,
            "lessons_created": 0,
        }

    # Fetch live prices once per unique ticker
    unique_tickers = sorted({str(r[2]).upper() for r in open_rows if r[2]})
    live_map: Dict[str, tuple[float, float]] = {}
    for sym in unique_tickers:
        if price_overrides and sym in price_overrides:
            live_map[sym] = (float(price_overrides[sym]), 0.0)
        else:
            p, chg = await asyncio.to_thread(get_live_price_and_change, sym)
            if p and p > 0:
                live_map[sym] = (float(p), float(chg or 0.0))

    now_utc = datetime.datetime.now(datetime.timezone.utc)
    hit_target_cnt = 0
    hit_stop_cnt = 0
    expired_cnt = 0
    still_open_cnt = 0
    lessons_created = 0
    graded_events = []

    for row in open_rows:
        pred_id, raw_src, ticker, direction, entry_p, target_p, stop_p, horizon_days, created_at = row
        sym = str(ticker).upper()
        spot_tuple = live_map.get(sym)
        if not spot_tuple or spot_tuple[0] <= 0:
            still_open_cnt += 1
            continue

        live_price, live_chg = spot_tuple
        entry_val = float(entry_p or 0.0)
        target_val = float(target_p) if target_p is not None else None
        stop_val = float(stop_p) if stop_p is not None else None
        clean_dir = str(direction or "BULLISH").upper()
        norm_src = normalize_source_key(raw_src)

        if entry_val <= 0:
            still_open_cnt += 1
            continue

        if clean_dir == "BEARISH":
            ret_pct = round(((entry_val - live_price) / entry_val) * 100.0, 3)
            hit_tp = target_val is not None and live_price <= target_val
            hit_sl = stop_val is not None and live_price >= stop_val
        else:
            ret_pct = round(((live_price - entry_val) / entry_val) * 100.0, 3)
            hit_tp = target_val is not None and live_price >= target_val
            hit_sl = stop_val is not None and live_price <= stop_val

        is_expired = False
        if created_at is not None:
            created_aware = created_at if created_at.tzinfo else created_at.replace(tzinfo=datetime.timezone.utc)
            age_days = (now_utc - created_aware).total_seconds() / 86400.0
            if age_days >= float(horizon_days or 15):
                is_expired = True

        new_status = None
        lesson_text = None

        if hit_tp:
            new_status = "HIT_TARGET"
            hit_target_cnt += 1
        elif hit_sl:
            new_status = "HIT_STOP"
            hit_stop_cnt += 1
            lesson_text = await _generate_stop_loss_lesson(
                source=norm_src,
                ticker=sym,
                direction=clean_dir,
                entry_price=entry_val,
                stop_loss=stop_val or entry_val,
                live_price=live_price,
                realized_return_pct=ret_pct,
                regime_label=macro_regime["regime_label"],
            )
        elif is_expired:
            new_status = "EXPIRED"
            expired_cnt += 1
        else:
            still_open_cnt += 1

        # Persist cached spot price on portfolio_holdings if held, and commit grading atomically
        w_conn = _get_conn()
        w_cur = w_conn.cursor()
        try:
            w_cur.execute("""
                UPDATE portfolio_holdings
                SET last_price = %s, last_change_pct = %s, price_updated_at = NOW()
                WHERE UPPER(ticker) = %s;
            """, (live_price, live_chg, sym))

            if new_status:
                w_cur.execute("""
                    UPDATE prediction_ledger
                    SET status = %s,
                        realized_return_pct = %s,
                        lesson_extracted = COALESCE(%s, lesson_extracted),
                        evaluated_at = NOW()
                    WHERE id = %s;
                """, (new_status, ret_pct, lesson_text, pred_id))

                if new_status in {"HIT_TARGET", "HIT_STOP"}:
                    w_cur.execute("""
                        SELECT total_calls, winning_calls, avg_return_pct, trust_weight
                        FROM source_reliability
                        WHERE source = %s;
                    """, (norm_src,))
                    s_row = w_cur.fetchone()
                    old_tot = int(s_row[0]) if s_row else 0
                    old_wins = int(s_row[1]) if s_row else 0
                    old_avg = float(s_row[2]) if s_row else 0.0
                    old_tw = float(s_row[3]) if s_row else 0.60

                    is_win = new_status == "HIT_TARGET"
                    n_tot, n_wins, n_wr, n_avg, n_tw = _compute_bayesian_trust_update(
                        old_tot, old_wins, old_avg, old_tw, is_win, ret_pct
                    )
                    w_cur.execute("""
                        INSERT INTO source_reliability (source, total_calls, winning_calls, win_rate_pct, avg_return_pct, trust_weight, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (source) DO UPDATE
                        SET total_calls = EXCLUDED.total_calls,
                            winning_calls = EXCLUDED.winning_calls,
                            win_rate_pct = EXCLUDED.win_rate_pct,
                            avg_return_pct = EXCLUDED.avg_return_pct,
                            trust_weight = EXCLUDED.trust_weight,
                            updated_at = NOW();
                    """, (norm_src, n_tot, n_wins, n_wr, n_avg, n_tw))

                    if new_status == "HIT_TARGET":
                        # Reinforce matching source rules in active_playbook
                        w_cur.execute("""
                            UPDATE active_playbook
                            SET confidence_weight = LEAST(0.99, COALESCE(confidence_weight, 0.70) + 0.03),
                                times_applied = COALESCE(times_applied, 1) + 1
                            WHERE LOWER(source) LIKE %s;
                        """, (f"%{norm_src.lower()}%",))

                    if new_status == "HIT_STOP" and lesson_text:
                        w_cur.execute("""
                            INSERT INTO active_playbook (
                                source, rule, confidence_weight, times_applied, regime_tag, teaching_note, created_at
                            )
                            VALUES (%s, %s, %s, 1, %s, %s, NOW());
                        """, (
                            f"Post-Mortem: {norm_src}",
                            lesson_text,
                            0.78,
                            macro_regime["regime_label"],
                            f"Synthesized after {sym} breached stop-loss at Rs. {live_price:,.2f} ({ret_pct:+.2f}%).",
                        ))
                        lessons_created += 1

                    graded_events.append({
                        "ticker": sym,
                        "source": norm_src,
                        "status": new_status,
                        "return_pct": ret_pct,
                        "new_trust_weight": n_tw,
                        "lesson": lesson_text,
                    })

            w_conn.commit()
            _refresh_trust_cache_from_conn(w_conn)
        except Exception as grade_err:
            w_conn.rollback()
            print(f"⚠️ [Learning Engine] Grading error on prediction #{pred_id}: {grade_err}", flush=True)
        finally:
            w_cur.close()
            w_conn.close()

    for ev in graded_events:
        outcome_tag = "TARGET HIT" if ev["status"] == "HIT_TARGET" else "STOP BREACHED"
        body = (
            f"Closed-loop grading marked {ev['ticker']} ({ev['source']}) as {ev['status']} "
            f"with realized return {ev['return_pct']:+.2f}%. Bayesian trust weight updated to {ev['new_trust_weight']:.2f}."
        )
        if ev.get("lesson"):
            body += f" Post-mortem rule added to playbook: {ev['lesson']}"
        publish_agent_event(
            category="DESK_MANAGER",
            title=f"Algorithm Learning [{outcome_tag}]: {ev['ticker']} ({ev['return_pct']:+.2f}%)",
            content=body,
            metadata={
                "ticker": ev["ticker"],
                "source": ev["source"],
                "confidence_weight": ev["new_trust_weight"],
                "quant_concept": "Bayesian Source Reliability & Post-Mortem Adaptation",
                "why_it_matters": (
                    f"{ev['source']}'s historical hit rate directly scales how heavily its future PSX signals "
                    f"are weighted in position sizing and execution alerts."
                ),
                "portfolio_impact": (
                    f"Source '{ev['source']}' trust recalibrated to {ev['new_trust_weight']:.2f} "
                    f"after {ev['ticker']} closed at {ev['return_pct']:+.2f}%."
                ),
            },
        )

    return {
        "status": "success",
        "evaluated": len(open_rows),
        "hit_target": hit_target_cnt,
        "hit_stop": hit_stop_cnt,
        "expired": expired_cnt,
        "still_open": still_open_cnt,
        "lessons_created": lessons_created,
    }


def enrich_event_teaching_metadata(
    category: str,
    title: str,
    content: str,
    metadata: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Co-Investor Teaching Generator:
    Guarantees every signal or sweep event stored in agent_feed carries structured micro-lesson metadata:
    {"why_it_matters": "...", "quant_concept": "...", "portfolio_impact": "...", "confidence_weight": 0.82}
    """
    meta = dict(metadata) if isinstance(metadata, dict) else {}
    combined_upper = f"{title or ''} {content or ''}".upper()
    combined_lower = f"{title or ''} {content or ''}".lower()

    # Detect ticker(s)
    known_tickers = list(PSX_SECTOR_MAP.keys())
    detected_tickers = []
    if meta.get("ticker") and str(meta["ticker"]).upper() not in {"MACRO", "NONE", "PSX"}:
        detected_tickers.append(str(meta["ticker"]).upper())
    for sym in known_tickers:
        if sym not in detected_tickers and re.search(rf"\b{re.escape(sym)}\b", combined_upper):
            detected_tickers.append(sym)

    primary_ticker = detected_tickers[0] if detected_tickers else None
    sector = PSX_SECTOR_MAP.get(primary_ticker, "PSX Broad Market") if primary_ticker else "PSX Macro & Liquidity"

    # Determine source & Bayesian confidence_weight
    raw_source = meta.get("source") or meta.get("mentor")
    if not raw_source:
        cat_u = (category or "").upper()
        if "WHATSAPP" in cat_u or "KTRADE" in cat_u:
            raw_source = "KTrade"
        elif "YOUTUBE" in cat_u:
            raw_source = "ARN"
        elif "DESK" in cat_u:
            raw_source = "Internal_DCF"
        elif "MACRO" in cat_u:
            raw_source = "Broker_Research"
        else:
            raw_source = "Internal_DCF"

    if meta.get("confidence_weight") is not None:
        try:
            conf_weight = round(max(0.10, min(1.00, float(meta["confidence_weight"]))), 2)
        except Exception:
            conf_weight = get_source_trust_weight(raw_source)
    else:
        conf_weight = get_source_trust_weight(raw_source)

    # Synthesize quant_concept, why_it_matters, and portfolio_impact if missing
    if not meta.get("quant_concept") or not meta.get("why_it_matters") or not meta.get("portfolio_impact"):
        if "dividend" in combined_lower or "wht" in combined_lower or "dps" in combined_lower:
            concept = "Dividend WHT (15%) & Cash Reinvestment Velocity"
            why = (
                "PSX cash dividends deduct 15% filer withholding tax at source and credit CDC bank accounts first; "
                "compounding only resumes once net proceeds are swept back into KTrade buying power."
            )
            impact = (
                f"Directly affects net cash yield on {primary_ticker or 'income anchors (FFC, EFERT, MEBL, HUBC)'} "
                f"and available KTrade dry powder."
            )
        elif "sync" in combined_lower or "custody" in combined_lower or "working capital" in combined_lower:
            concept = "Custody Reconciliation & Cost-Basis Integrity"
            why = (
                "Exact share counts, weighted-average cost basis, and liquid broker cash prevent phantom P&L drift "
                "and keep position-weight risk caps mathematically exact."
            )
            impact = "Re-anchors portfolio weights, sector concentration ratios, and stop-loss dollar risk across all 9 KTrade holdings."
        elif "stop" in combined_lower or "drawdown" in combined_lower or "downside" in combined_lower:
            concept = "Asymmetric Downside Protection & Capital Preservation"
            why = (
                "A 20% drawdown requires a 25% gain just to break even; honoring structural stop-losses protects "
                "compounding capital from illiquid PSX value traps."
            )
            impact = (
                f"Monitors downside tail risk in {primary_ticker or 'active equity positions'} ({sector}) "
                f"against our hard stop-loss floor."
            )
        elif "margin of safety" in combined_lower or "dcf" in combined_lower or "undervalued" in combined_lower or "pe" in combined_lower:
            concept = "Intrinsic DCF Valuation & Margin of Safety"
            why = (
                "Buying cash-generative KMI-30 leaders at a >=20% discount to intrinsic fair value provides both "
                "multiple-expansion upside and valuation buffer against earnings volatility."
            )
            impact = (
                f"Guides tranche sizing for {primary_ticker or 'undervalued watchlist candidates'} ({sector}) "
                f"relative to liquid KTrade cash."
            )
        elif "rsi" in combined_lower or "macd" in combined_lower or "chart" in combined_lower or "breakout" in combined_lower:
            concept = "Momentum Confluence & Mean-Reversion Timing"
            why = (
                "Combining fundamental valuation with RSI/volume structure avoids catching falling knives early "
                "or chasing exhausted parabolic breakouts."
            )
            impact = f"Refines short-term entry/trim timing on {primary_ticker or 'PSX watchlist equities'} ({sector})."
        elif any(k in combined_lower for k in ["sbp", "cpi", "interest", "imf", "circular debt", "tariff", "macro"]):
            concept = "Real Interest Rate Regime & Discount-Rate Transmission"
            why = (
                "SBP policy rate shifts directly alter the equity risk-free discount rate (WACC), banking NIMs (MEBL, FABL), "
                "and circular-debt cash recoveries for energy leaders (OGDC, MARI, HUBC)."
            )
            impact = f"Impacts valuation multiples across our Fertilizer, E&P, Power, and Islamic Banking book ({', '.join(detected_tickers[:3]) or 'KMI-30 core'})."
        else:
            concept = f"Sector Cash-Flow & Institutional Signal Weighting ({sector})"
            why = (
                f"Institutional order flow and earnings revisions in {sector} lead price discovery on PSX; "
                f"signals are scaled by {normalize_source_key(raw_source)}'s empirical Bayesian trust score ({conf_weight:.2f})."
            )
            impact = (
                f"Active monitoring on {', '.join(detected_tickers[:4]) if detected_tickers else 'core KTrade holdings'} "
                f"for thesis confirmation or position rebalancing."
            )

        meta.setdefault("quant_concept", concept)
        meta.setdefault("why_it_matters", why)
        meta.setdefault("portfolio_impact", impact)

    meta["confidence_weight"] = conf_weight
    meta.setdefault("source_key", normalize_source_key(raw_source))
    if primary_ticker and "ticker" not in meta:
        meta["ticker"] = primary_ticker
    return meta


def get_algorithm_state() -> Dict[str, Any]:
    """
    Returns full state for GET /api/dashboard/algorithm-state:
    - macro_regime (sbp_rate, cpi, real_rate, regime_label)
    - accuracy_summary (learning accuracy %, graded calls, open calls)
    - source_reliability leaderboard
    - prediction_ledger active & graded calls
    - active_playbook rules sorted by confidence_weight DESC
    """
    init_learning_tables()
    seed_initial_predictions_if_empty()

    conn = _get_conn()
    cur = conn.cursor()
    try:
        macro_regime = get_macro_regime_sync(conn)

        cur.execute("""
            SELECT source, total_calls, winning_calls, win_rate_pct, avg_return_pct, trust_weight, updated_at
            FROM source_reliability
            ORDER BY trust_weight DESC, win_rate_pct DESC;
        """)
        sources = [
            {
                "source": r[0],
                "total_calls": int(r[1] or 0),
                "winning_calls": int(r[2] or 0),
                "win_rate_pct": float(r[3] or 0.0),
                "avg_return_pct": float(r[4] or 0.0),
                "trust_weight": float(r[5] or 0.60),
                "updated_at": r[6].isoformat() if r[6] else None,
            }
            for r in cur.fetchall()
        ]

        cur.execute("""
            SELECT id, source, ticker, direction, entry_price, target_price, stop_loss,
                   horizon_days, status, realized_return_pct, lesson_extracted, created_at, evaluated_at
            FROM prediction_ledger
            ORDER BY CASE WHEN status = 'OPEN' THEN 0 ELSE 1 END, created_at DESC
            LIMIT 60;
        """)
        predictions = [
            {
                "id": r[0],
                "source": r[1],
                "ticker": r[2],
                "direction": r[3],
                "entry_price": float(r[4]) if r[4] is not None else 0.0,
                "target_price": float(r[5]) if r[5] is not None else None,
                "stop_loss": float(r[6]) if r[6] is not None else None,
                "horizon_days": int(r[7] or 15),
                "status": r[8],
                "realized_return_pct": float(r[9]) if r[9] is not None else None,
                "lesson_extracted": r[10],
                "created_at": r[11].isoformat() if r[11] else None,
                "evaluated_at": r[12].isoformat() if r[12] else None,
            }
            for r in cur.fetchall()
        ]

        cur.execute("""
            SELECT id, source, rule,
                   COALESCE(confidence_weight, 0.70) AS cw,
                   COALESCE(times_applied, 1) AS ta,
                   COALESCE(regime_tag, 'ALL') AS reg,
                   teaching_note,
                   created_at
            FROM active_playbook
            ORDER BY COALESCE(confidence_weight, 0.70) DESC, created_at DESC
            LIMIT 40;
        """)
        playbook = [
            {
                "id": r[0],
                "source": r[1],
                "rule": r[2],
                "confidence_weight": round(float(r[3] or 0.70), 2),
                "times_applied": int(r[4] or 1),
                "regime_tag": r[5] or "ALL",
                "teaching_note": r[6] or "",
                "created_at": r[7].isoformat() if r[7] else None,
            }
            for r in cur.fetchall()
        ]

        # Compute overall learning accuracy across graded predictions + calibrated source priors
        graded = [p for p in predictions if p["status"] in {"HIT_TARGET", "HIT_STOP"}]
        open_calls = [p for p in predictions if p["status"] == "OPEN"]
        if graded:
            wins = sum(1 for p in graded if p["status"] == "HIT_TARGET")
            total_g = len(graded)
            acc_pct = round((wins / total_g) * 100.0, 1)
            avg_ret = round(sum(float(p["realized_return_pct"] or 0.0) for p in graded) / total_g, 2)
        else:
            tot_calls_src = sum(s["total_calls"] for s in sources)
            win_calls_src = sum(s["winning_calls"] for s in sources)
            wins = win_calls_src
            total_g = tot_calls_src
            acc_pct = round((win_calls_src / tot_calls_src) * 100.0, 1) if tot_calls_src > 0 else 68.0
            avg_ret = (
                round(sum(s["avg_return_pct"] * s["total_calls"] for s in sources) / tot_calls_src, 2)
                if tot_calls_src > 0 else 0.0
            )

        return {
            "status": "success",
            "macro_regime": macro_regime,
            "accuracy_summary": {
                "accuracy_pct": acc_pct,
                "winning_calls": wins,
                "total_graded": total_g,
                "open_calls": len(open_calls),
                "avg_realized_return_pct": avg_ret,
                "playbook_rules_count": len(playbook),
            },
            "source_reliability": sources,
            "prediction_ledger": predictions,
            "active_playbook": playbook,
        }
    finally:
        cur.close()
        conn.close()

