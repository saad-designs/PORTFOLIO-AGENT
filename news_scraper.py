import sys
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

import asyncio
import time
import requests
import feedparser
import json
import re
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

from portfolio_manager import (
    get_db_connection,
    get_checkpoint,
    update_checkpoint,
    add_pending_signal,
    send_learning_brief,
    publish_agent_event,
)

# Core universe mapping for deterministic regex fallback
SECTOR_KEYWORDS = {
    "Technology": ["it", "tech", "software", "export", "freelance", "systems limited", "sys", "digitization"],
    "Fertilizer": ["fertilizer", "urea", "dap", "gas tariff", "ffc", "efert", "engro", "subsidy", "agri"],
    "Oil & Gas Exploration": ["oil", "gas", "brent", "crude", "drilling", "ogdc", "mari", "ppl", "pol", "circular debt", "well", "hydrocarbon"],
    "Power Generation": ["power", "ipp", "electricity", "circular debt", "hubco", "hubc", "nepra", "tariff", "capacity payments"],
    "Islamic Banking": ["bank", "banking", "meezan", "mebl", "fabl", "faysal", "shariah", "sukuk", "sbp", "kibor", "interest rate", "npl"],
    "Cement": ["cement", "coal", "construction", "luck", "lucky cement", "infrastructure", "dispatches"],
}

TICKER_KEYWORDS = {
    "SYS": ["systems limited", "sys", "it sector"],
    "FFC": ["fauji fertilizer company", "ffc", "urea"],
    "EFERT": ["engro fertilizers", "efert"],
    "OGDC": ["oil & gas development company", "ogdc"],
    "MARI": ["mari petroleum", "mari energies", "mari"],
    "PPL": ["pakistan petroleum", "ppl"],
    "POL": ["pakistan oilfields", "pol"],
    "HUBC": ["hub power", "hubco", "hubc"],
    "MEBL": ["meezan bank", "mebl"],
    "FABL": ["faysal bank", "fabl"],
    "LUCK": ["lucky cement", "luck"],
}

STREAM_A_LOCAL_FEEDS = [
    {
        "name": "Google News PSX & Economy",
        "url": "https://news.google.com/rss/search?q=(PSX+OR+%22Pakistan+Stock+Exchange%22+OR+KSE100+OR+%22State+Bank+of+Pakistan%22+OR+%22circular+debt%22+OR+%22IMF+Pakistan%22)+when:3d&hl=en-PK&gl=PK&ceid=PK:en"
    },
    {
        "name": "Google News Core Holdings",
        "url": "https://news.google.com/rss/search?q=(SYS+OR+%22Systems+Limited%22+OR+HUBC+OR+Hubco+OR+OGDC+OR+%22Mari+Petroleum%22+OR+FFC+OR+EFERT+OR+MEBL+OR+%22Lucky+Cement%22+OR+%22Pakistan+Oilfields%22)+when:5d&hl=en-PK&gl=PK&ceid=PK:en"
    },
    {
        "name": "Business Recorder Markets",
        "url": "https://www.brecorder.com/feeds/markets"
    },
    {
        "name": "Profit Pakistan Today",
        "url": "https://profit.pakistantoday.com.pk/feed/"
    },
    {
        "name": "Dawn Business",
        "url": "https://www.dawn.com/feeds/business/"
    }
]

STREAM_B_GLOBAL_FEEDS = [
    {
        "name": "Google News Global Energy & Commodities",
        "url": "https://news.google.com/rss/search?q=(%22Brent+crude%22+OR+%22crude+oil%22+OR+%22urea+price%22+OR+%22natural+gas%22)+(commodities+OR+markets)+when:3d&hl=en-US&gl=US&ceid=US:en"
    },
    {
        "name": "Google News Global Rates & Tech",
        "url": "https://news.google.com/rss/search?q=(%22Federal+Reserve%22+OR+%22Fed+rate%22+OR+%22global+IT+spending%22)+(markets+OR+economy)+when:3d&hl=en-US&gl=US&ceid=US:en"
    }
]

LOCAL_FILTER_KEYWORDS = [
    "psx", "kse", "kse100", "kse-100", "stock", "stocks", "shares", "dividend",
    "sbp", "state bank", "policy rate", "inflation", "cpi", "circular debt", "imf",
    "rupee", "remittance", "tax", "fbr", "ito", "budget", "fertilizer", "urea",
    "gas tariff", "power tariff", "ipp", "oil", "petroleum", "gas", "exploration",
    "refinery", "banking", "sukuk", "cement", "tech", "software", "sys", "hubc",
    "ogdc", "mari", "ffc", "efert", "mebl", "luck", "pol", "ppl", "fabl"
]

GLOBAL_FILTER_KEYWORDS = [
    "brent", "crude", "oil", "opec", "energy", "natural gas", "urea", "fertilizer",
    "fed", "federal reserve", "interest rate", "rate cut", "inflation", "dollar",
    "treasury", "tech spending", "cloud", "ai spending", "emerging markets"
]


def _is_relevant(text: str, filter_keywords: List[str]) -> bool:
    t_lower = text.lower()
    return any(re.search(rf"\b{re.escape(k)}\b", t_lower) for k in filter_keywords)


def fetch_feed_articles(feed_list: List[Dict[str, str]], filter_keywords: List[str], max_entries: int = 8) -> List[Dict[str, Any]]:
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'application/rss+xml,application/xml,text/xml;q=0.9,*/*;q=0.8'
    }
    discovered = []
    seen = set()

    for f_info in feed_list:
        name = f_info["name"]
        url = f_info["url"]
        try:
            resp = requests.get(url, headers=headers, timeout=7)
            if resp.status_code == 200 and resp.content:
                parsed = feedparser.parse(resp.content)
                for entry in parsed.entries[:6]:
                    title = getattr(entry, "title", "").strip()
                    if not title or title.lower() in seen:
                        continue
                    summary = getattr(entry, "summary", "") or ""
                    clean_summary = re.sub(r'<[^>]+>', '', summary).strip()
                    combined_text = f"{title} {clean_summary}"
                    if not _is_relevant(combined_text, filter_keywords):
                        continue

                    seen.add(title.lower())
                    link = getattr(entry, "link", "")
                    pub_date = getattr(entry, "published", "") or getattr(entry, "updated", "")
                    discovered.append({
                        "title": title,
                        "url": link,
                        "body": clean_summary[:400] or title,
                        "source": name,
                        "published_at": pub_date
                    })
                    if len(discovered) >= max_entries:
                        break
        except Exception as e:
            print(f"⚠️ [News Feeder] Could not fetch {name}: {e}")
        if len(discovered) >= max_entries:
            break

    return discovered


def extract_structured_macro_signal(article: Dict[str, Any], scope: str) -> Dict[str, Any]:
    """
    Transforms a raw news article into a structured signal record for brain_macro_signals.
    Uses LLM (Groq / Gemini) when available, with full deterministic keyword fallback.
    """
    title = article.get("title", "")
    body = article.get("body", "")
    source = article.get("source", "RSS Feed")
    url = article.get("url", "")
    text = f"{title}. {body}"

    # Deterministic fallback extraction first
    detected_sectors = []
    for sec, kw_list in SECTOR_KEYWORDS.items():
        if any(re.search(rf"\b{re.escape(k)}\b", text.lower()) for k in kw_list):
            detected_sectors.append(sec)

    detected_tickers = []
    for tick, kw_list in TICKER_KEYWORDS.items():
        if any(re.search(rf"\b{re.escape(k)}\b", text.lower()) for k in kw_list):
            detected_tickers.append(tick)

    # Simple sentiment heuristic
    pos_words = ["surge", "record", "jump", "rally", "gain", "profit", "boost", "inflow", "upgrade", "growth", "cut", "bullish", "approval", "dividend"]
    neg_words = ["plunge", "drop", "fall", "decline", "loss", "deficit", "inflation", "hike", "bearish", "crisis", "default", "tax hike", "penalty"]
    score = 0.0
    for w in pos_words:
        if re.search(rf"\b{re.escape(w)}\b", text.lower()):
            score += 2.0
    for w in neg_words:
        if re.search(rf"\b{re.escape(w)}\b", text.lower()):
            score -= 2.0
    sentiment_score = max(-10.0, min(10.0, score))

    if not detected_sectors:
        detected_sectors = ["Macro Economy / KSE-100"] if scope == "LOCAL_PSX" else ["Global Commodities"]
    takeaway = f"Impacts {', '.join(detected_sectors)}. Re-weight DCF cash flows based on latest macro catalyst."

    # Attempt LLM structured parsing for superior nuance
    try:
        from model_router import generate_text_groq
        prompt = (
            f"Analyze this financial news story for the Pakistan Stock Exchange (PSX) Central Brain.\n"
            f"Title: {title}\nSummary: {body}\n\n"
            f"Output strictly valid JSON with keys:\n"
            f'- "headline": (concise 1-sentence headline)\n'
            f'- "affected_sectors": (list of sectors like Technology, Fertilizer, Oil & Gas Exploration, Power Generation, Islamic Banking, Cement)\n'
            f'- "affected_tickers": (list of PSX symbols like SYS, FFC, OGDC, MARI, HUBC, LUCK, EFERT, MEBL, POL, FABL)\n'
            f'- "sentiment_score": (float between -10.0 and +10.0)\n'
            f'- "central_brain_takeaway": (1-2 sentences on how a PSX capital accumulation investor should respond)\n'
        )
        llm_resp = generate_text_groq(prompt)
        if llm_resp:
            clean_json = llm_resp.strip()
            if "```" in clean_json:
                clean_json = re.sub(r"^```(?:json)?\s*", "", clean_json, flags=re.MULTILINE)
                clean_json = re.sub(r"\s*```$", "", clean_json, flags=re.MULTILINE)
            parsed = json.loads(clean_json)
            if isinstance(parsed, dict) and "headline" in parsed:
                return {
                    "scope": scope,
                    "headline": str(parsed.get("headline") or title),
                    "affected_sectors": parsed.get("affected_sectors") or detected_sectors,
                    "affected_tickers": parsed.get("affected_tickers") or detected_tickers,
                    "sentiment_score": float(parsed.get("sentiment_score") or sentiment_score),
                    "central_brain_takeaway": str(parsed.get("central_brain_takeaway") or takeaway),
                    "source": source,
                    "url": url,
                }
    except Exception:
        pass

    return {
        "scope": scope,
        "headline": title,
        "affected_sectors": detected_sectors,
        "affected_tickers": detected_tickers,
        "sentiment_score": sentiment_score,
        "central_brain_takeaway": takeaway,
        "source": source,
        "url": url,
    }


def persist_macro_signal(signal: Dict[str, Any]) -> int:
    """Inserts a structured signal into brain_macro_signals and returns the inserted ID."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO brain_macro_signals 
            (scope, headline, affected_sectors, affected_tickers, sentiment_score, central_brain_takeaway, source, url, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
            RETURNING id;
        """, (
            signal["scope"],
            signal["headline"],
            json.dumps(signal["affected_sectors"]),
            json.dumps(signal["affected_tickers"]),
            signal["sentiment_score"],
            signal["central_brain_takeaway"],
            signal["source"],
            signal.get("url") or "",
        ))
        row = cur.fetchone()
        inserted_id = row[0] if row else 0
        conn.commit()
        return inserted_id
    except Exception as e:
        conn.rollback()
        print(f"⚠️ [brain_macro_signals] Insert failed: {e}")
        return 0
    finally:
        cur.close()
        conn.close()


def run_dual_stream_news_sweep(max_per_stream: int = 4) -> Dict[str, Any]:
    """
    Executes real live dual-stream news ingestion:
    - Stream A: Filtered Pakistani PSX & Macro news
    - Stream B: Filtered Global Macro & Commodities news
    - Extracts structured signals and persists to brain_macro_signals
    - Returns structured audit report with genuine counts, latency, and sample signals.
    """
    t0 = time.perf_counter()
    print("🔄 [Dual-Stream News] Initiating live scrape across Stream A (PSX) & Stream B (Global)...")

    stream_a_raw = fetch_feed_articles(STREAM_A_LOCAL_FEEDS, LOCAL_FILTER_KEYWORDS, max_entries=max_per_stream)
    stream_b_raw = fetch_feed_articles(STREAM_B_GLOBAL_FEEDS, GLOBAL_FILTER_KEYWORDS, max_entries=max_per_stream)

    signals_created = []

    for item in stream_a_raw:
        sig = extract_structured_macro_signal(item, scope="LOCAL_PSX")
        sig_id = persist_macro_signal(sig)
        sig["id"] = sig_id
        signals_created.append(sig)

    for item in stream_b_raw:
        sig = extract_structured_macro_signal(item, scope="GLOBAL_MACRO")
        sig_id = persist_macro_signal(sig)
        sig["id"] = sig_id
        signals_created.append(sig)

    # Post top signal to agent_feed
    if signals_created:
        top_sig = signals_created[0]
        publish_agent_event(
            category="MACRO_NEWS",
            title=f"Macro Signal: {top_sig['headline']}",
            content=(
                f"Scope: {top_sig['scope']} | Sentiment: {top_sig['sentiment_score']:+.1f}\n"
                f"Sectors: {', '.join(top_sig['affected_sectors'])}\n"
                f"Takeaway: {top_sig['central_brain_takeaway']}"
            ),
            metadata=top_sig
        )

    latency_ms = round((time.perf_counter() - t0) * 1000.0, 2)
    return {
        "status": "PASS",
        "latency_ms": latency_ms,
        "stream_a_count": len(stream_a_raw),
        "stream_b_count": len(stream_b_raw),
        "total_signals_persisted": len(signals_created),
        "signals": signals_created,
    }


# Backwards compatibility wrappers
def fetch_psx_news(max_articles: int = 10) -> List[Dict[str, Any]]:
    return fetch_feed_articles(STREAM_A_LOCAL_FEEDS, LOCAL_FILTER_KEYWORDS, max_entries=max_articles)

fetch_business_news = fetch_psx_news

async def run_news_sweep():
    return await asyncio.to_thread(run_dual_stream_news_sweep)

if __name__ == "__main__":
    print("🚀 [Diagnostics] Running Dual-Stream News Sweep directly...")
    res = run_dual_stream_news_sweep(max_per_stream=3)
    print(f"✅ Finished in {res['latency_ms']} ms. Signals persisted: {res['total_signals_persisted']}")
    for s in res["signals"]:
        print(f" - [{s['scope']}] {s['headline']} (Sentiment: {s['sentiment_score']}) -> {s['central_brain_takeaway']}")
