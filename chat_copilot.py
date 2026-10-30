import os
import sys
import uuid
import json
import re
import asyncio
from typing import Dict, Any, List, Optional

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

if sys.platform == "win32":
    if sys.version_info < (3, 14):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from model_router import (
    generate_text_groq,
    generate_with_fallback,
    get_active_google_api_key,
    TASK_DEEP_REASONING,
    TASK_MULTIMODAL,
)
from portfolio_manager import (
    get_complete_financial_summary,
    get_agent_feed,
    publish_agent_event,
    record_trade_execution,
    record_capital_transaction,
    record_dividend,
    transfer_dividend_to_ktrade,
    save_chat_message,
    get_chat_history,
    get_db_connection
)

SYSTEM_COPILOT_PROMPT = """You are the Senior PSX Portfolio Co-Investor & Automated Multimodal Ledger Operator.
Your job is to parse the user's natural language updates, questions, OR uploaded multimodal attachments (KTrade PDF trade confirmation statements, PSX Broker Research Report PDFs, and Technical Chart Screenshots) regarding the Pakistan Stock Exchange (PSX) portfolio, execute structured financial/research actions, and provide concise, executive, numbers-backed answers.

You must respond ONLY with a valid JSON object matching this schema:
{
  "action_type": "BUY_STOCK" | "SELL_STOCK" | "LOG_KTRADE_STATEMENT" | "DEPOSIT_CASH" | "WITHDRAW_CASH" | "LOG_DIVIDEND_IN_BANK" | "LOG_DIVIDEND_ENTITLED" | "TRANSFER_DIVIDEND_TO_KTRADE" | "LOG_RESEARCH_REPORT" | "CHART_ANALYSIS" | "GENERAL_QUERY",
  "action_data": { ... },
  "additional_actions": [
    {"action_type": "...", "action_data": { ... }}
  ],
  "reply": "Executive, concise direct response."
}
(Note: "additional_actions" is optional—include it when the user uploads multiple files that warrant multiple distinct actions, such as two broker reports, multiple chart screenshots, or a KTrade PDF statement alongside a chart screenshot.)

ACTION DEFINITIONS:
1. BUY_STOCK:
   - Single share purchase (e.g. "Bought 500 HUBC at 118.50" or a 1-trade BUY confirmation slip)
   - action_data: {"ticker": "HUBC", "side": "BUY", "quantity": 500.0, "price": 118.50, "fees": 0.0, "notes": "Trade note if any"}
2. SELL_STOCK:
   - Single share sale (e.g. "Sold 300 MEBL at 245.00" or a 1-trade SELL confirmation slip)
   - action_data: {"ticker": "MEBL", "side": "SELL", "quantity": 300.0, "price": 245.00, "fees": 0.0, "notes": "Trade note if any"}
3. LOG_KTRADE_STATEMENT:
   - Uploaded KTrade PDF daily trade confirmation statement / broker contract note containing one or more executed PSX trades. Extract exact ticker symbol, BUY/SELL side, quantity, execution price, and total commission + SST + CDC + CVT fees for each trade!
   - action_data: {
       "trades": [
         {"ticker": "SYS", "side": "BUY", "quantity": 500.0, "price": 440.50, "fees": 65.20, "notes": "Parsed from KTrade PDF Statement"}
       ]
     }
4. DEPOSIT_CASH:
   - Fresh capital deposited into KTrade (e.g. "Deposited 100,000 into KTrade", "Added 50k cash to broker")
   - action_data: {"amount": 100000.0, "notes": "Notes if any"}
5. WITHDRAW_CASH:
   - Cash withdrawn from KTrade to personal bank (e.g. "Withdrew 25,000 from KTrade")
   - action_data: {"amount": 25000.0, "notes": "Notes if any"}
6. LOG_DIVIDEND_IN_BANK:
   - Dividend received / credited into personal bank account (e.g. "Received Rs 4/share dividend from HUBC in bank", "Got 8,500 net dividend from MEBL in my bank")
   - action_data: {"ticker": "HUBC", "shares_held": null, "dps_gross": 4.0, "net_amount": null, "gross_amount": null, "tax_rate_pct": 15.0, "notes": "Notes if any"}
   - Note: If user only mentions DPS, set "dps_gross" and leave "shares_held": null. If user mentions net total, set "net_amount".
7. LOG_DIVIDEND_ENTITLED:
   - Dividend announced or entitled but not yet credited to bank (e.g. "HUBC announced Rs 4 dividend")
   - action_data: {"ticker": "HUBC", "shares_held": null, "dps_gross": 4.0, "net_amount": null, "gross_amount": null, "tax_rate_pct": 15.0, "notes": "Notes if any"}
8. TRANSFER_DIVIDEND_TO_KTRADE:
   - User moved previously credited bank dividend into KTrade broker account for reinvestment (e.g. "Sent the MEBL dividend from bank to KTrade")
   - action_data: {"ticker": "MEBL", "dividend_id": null}
9. LOG_RESEARCH_REPORT:
   - Uploaded Broker Research Report PDF / institutional equity note (e.g., AKD, Arif Habib, Topline, KTrade, JS Global). Read the full thesis, compare the broker's target price & EPS assumptions directly against our internal Valuation / DCF Watchlist Targets in LIVE FINANCIAL CONTEXT, and summarize actionable insights.
   - action_data: {
       "source": "KTrade Research",
       "ticker": "LUCK",
       "subject": "Initiating / Earnings Update Title",
       "sentiment": "BULLISH" | "BEARISH" | "NEUTRAL",
       "broker_target_price": 1250.0,
       "summary": "2-4 sentence institutional thesis summary & comparison vs our internal target model."
     }
10. CHART_ANALYSIS:
   - Uploaded Technical Chart Screenshot (TradingView / KTrade / PSX chart). Inspect price structure, candlesticks, RSI, MACD, moving averages (EMA/SMA), volume bars, and support/resistance breakout levels. Cross-reference against our current holding, average cost, and stop-loss if held.
   - action_data: {
       "ticker": "SYS",
       "setup": "BULLISH_BREAKOUT" | "PULLBACK_SUPPORT" | "OVERBOUGHT_CAUTION" | "BEARISH_BREAKDOWN" | "NEUTRAL_CONSOLIDATION",
       "summary": "Concise technical setup note (RSI/MACD/key levels)."
     }
11. GENERAL_QUERY:
   - Portfolio inquiries, thesis review, valuations, macro queries, mentor research questions, or health checks.
   - action_data: {}

CRITICAL RULES:
- Output STRICT JSON only. Do not wrap in markdown or prefix with text.
- Be exact with PSX ticker symbols (SYS, HUBC, LUCK, FFC, EFERT, MEBL, MARI, OGDC, PPL, PSO, HBL, UBL, MCB, SAZEW, etc.).
- Never invent trades if the user is merely asking a question or uploading a research report/chart (only log trades when the user explicitly states an executed trade or uploads an actual broker trade confirmation statement).
- Responses must be sharp, highly confident, and numbers-backed.
"""


def extract_json_payload(raw_text: str) -> dict:
    """Extracts and parses JSON object from model response."""
    if not raw_text:
        return {}
    text = raw_text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?", "", text).strip()
        text = re.sub(r"```$", "", text).strip()

    try:
        return json.loads(text)
    except Exception:
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            try:
                return json.loads(text[start:end + 1])
            except Exception:
                pass
    return {}


def get_live_financial_context() -> str:
    """Gathers live holdings, cash ledgers, valuation/DCF targets, playbook rules, and recent feed."""
    summary = {}
    try:
        summary = get_complete_financial_summary()
    except Exception as e:
        print(f"⚠️ [Chat Copilot] Failed fetching financial summary: {e}")

    kpis = summary.get("financial_kpis", {})
    holdings = summary.get("holdings", [])
    recent_feed = []
    try:
        recent_feed = get_agent_feed(limit=5)
    except Exception:
        pass

    playbook_rules = []
    valuation_targets = []
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        try:
            cur.execute("SELECT source, rule FROM active_playbook ORDER BY created_at DESC LIMIT 5;")
            playbook_rules = [{"source": r[0], "rule": r[1]} for r in cur.fetchall()]
        except Exception:
            conn.rollback()

        try:
            cur.execute("""
                SELECT ticker, target_price, stop_loss, historical_fair_pe, current_trailing_pe
                FROM watchlist_targets
                ORDER BY ticker ASC
                LIMIT 30;
            """)
            for r in cur.fetchall():
                valuation_targets.append({
                    "ticker": r[0],
                    "internal_target_price": float(r[1]) if r[1] is not None else None,
                    "stop_loss": float(r[2]) if r[2] is not None else None,
                    "fair_pe": float(r[3]) if r[3] is not None else None,
                    "trailing_pe": float(r[4]) if r[4] is not None else None
                })
        except Exception:
            conn.rollback()

        cur.close()
        conn.close()
    except Exception:
        pass

    return f"""LIVE FINANCIAL CONTEXT:
- Total Net Worth: Rs. {kpis.get('total_net_worth', 0):,.2f}
- Fresh Capital Invested: Rs. {kpis.get('fresh_capital_invested', 0):,.2f}
- KTrade Available Cash: Rs. {kpis.get('ktrade_available_cash', 0):,.2f}
- Portfolio Market Value: Rs. {kpis.get('portfolio_market_value', 0):,.2f}
- Unrealized P&L: Rs. {kpis.get('unrealized_pnl', 0):,.2f} ({kpis.get('unrealized_pnl_pct', 0):+.2f}%)
- Dividends in Bank: Rs. {kpis.get('net_dividends_in_bank', 0):,.2f}
- Dividends Sent to KTrade: Rs. {kpis.get('net_dividends_reinvested', 0):,.2f}
- Pending Dividends: Rs. {kpis.get('pending_dividends', 0):,.2f}

ACTIVE HOLDINGS:
{json.dumps([{h['ticker']: f"{h['shares']} shs @ Rs. {h['avg_price']} (Cur: Rs. {h['current_price']})"} for h in holdings], indent=2)}

INTERNAL DCF / VALUATION WATCHLIST TARGETS:
{json.dumps(valuation_targets, indent=2)}

ACTIVE PLAYBOOK RULES:
{json.dumps(playbook_rules, indent=2)}

RECENT RELEVANT SIGNALS / FEED:
{json.dumps([{f['category']: f"{f['title']} - {f['content'][:120]}"} for f in recent_feed], indent=2)}"""


def parse_copilot_response(raw_response: str, persisted_user_msg: str = "User query") -> dict:
    """
    Parses structured JSON copilot output, executes ledger/research/feed actions
    (including multi-file batch actions), persists chat history, and formats the response.
    """
    parsed = extract_json_payload(raw_response or "")
    primary_action_type = parsed.get("action_type", "GENERAL_QUERY")
    primary_action_data = parsed.get("action_data", {}) or {}
    reply_text = parsed.get("reply", "")
    if not parsed and raw_response and raw_response.strip():
        reply_text = raw_response.strip()

    actions_taken = []

    def _execute_single_trade(t_obj: dict, default_side: Optional[str] = None, default_note: str = "Executed via Dashboard Chat Copilot"):
        ticker = str(t_obj.get("ticker", "")).strip().upper()
        side = str(t_obj.get("side") or default_side or "BUY").strip().upper()
        if side not in {"BUY", "SELL"}:
            side = "BUY"
        qty = float(t_obj.get("quantity") or 0)
        price = float(t_obj.get("price") or 0)
        fees = float(t_obj.get("fees") or t_obj.get("fees_and_tax") or 0.0)
        notes = t_obj.get("notes") or default_note

        if ticker and qty > 0 and price > 0:
            trade_res = record_trade_execution(
                ticker=ticker,
                side=side,
                quantity=qty,
                price=price,
                fees=fees,
                notes=notes
            )
            action_badge = {
                "type": "TRADE",
                "label": f"{side} {qty:,.0f} {ticker} @ Rs. {price:,.2f}",
                "details": f"Net Total: Rs. {trade_res['net_total']:,.2f}",
                "status": "COMPLETED"
            }
            actions_taken.append(action_badge)
            publish_agent_event(
                category="SYSTEM_ALERT",
                title=f"Trade Executed: {side} {qty:,.0f} {ticker}",
                content=f"{side} {qty:,.0f} {ticker} executed at Rs. {price:,.2f} (Fees: Rs. {fees:,.2f}). Cash adjusted by Rs. {trade_res['net_total']:,.2f}.",
                metadata=trade_res
            )
            return trade_res
        return None

    def _execute_action(action_type: str, action_data: dict):
        nonlocal reply_text
        if not action_type or action_type == "GENERAL_QUERY":
            return

        if action_type in {"BUY_STOCK", "SELL_STOCK"}:
            default_side = "BUY" if action_type == "BUY_STOCK" else "SELL"
            trade_res = _execute_single_trade(action_data, default_side=default_side)
            if trade_res and not reply_text:
                reply_text = (
                    f"✅ Executed **{trade_res['side']} {trade_res['quantity']:,.0f} {trade_res['ticker']}** "
                    f"at **Rs. {trade_res['price']:,.2f}** (Net Total: Rs. {trade_res['net_total']:,.2f}). Holdings & cash updated."
                )

        elif action_type == "LOG_KTRADE_STATEMENT":
            trades_list = action_data.get("trades") or []
            if isinstance(trades_list, dict):
                trades_list = [trades_list]
            logged_count = 0
            for t_item in trades_list:
                if isinstance(t_item, dict):
                    res = _execute_single_trade(t_item, default_note="Parsed from KTrade PDF Statement")
                    if res:
                        logged_count += 1
            if logged_count > 0 and not reply_text:
                reply_text = f"✅ Parsed KTrade PDF statement and logged **{logged_count}** trade execution(s) into `trade_history` and updated live holdings."

        elif action_type == "DEPOSIT_CASH":
            amount = float(action_data.get("amount", 0))
            notes = action_data.get("notes") or "Fresh deposit via Dashboard Chat Copilot"
            if amount > 0:
                cap_res = record_capital_transaction("DEPOSIT_TO_KTRADE", amount, notes=notes)
                actions_taken.append({
                    "type": "CAPITAL",
                    "label": f"Deposited Rs. {amount:,.2f} to KTrade",
                    "status": "COMPLETED"
                })
                publish_agent_event(
                    category="SYSTEM_ALERT",
                    title=f"Capital Deposited: Rs. {amount:,.2f}",
                    content=f"Fresh capital of Rs. {amount:,.2f} deposited into KTrade cash balance.",
                    metadata=cap_res
                )
                if not reply_text:
                    reply_text = f"💵 Deposited **Rs. {amount:,.2f}** into KTrade available cash. Total fresh capital updated."

        elif action_type == "WITHDRAW_CASH":
            amount = float(action_data.get("amount", 0))
            notes = action_data.get("notes") or "Withdrawal via Dashboard Chat Copilot"
            if amount > 0:
                cap_res = record_capital_transaction("WITHDRAWAL_FROM_KTRADE", amount, notes=notes)
                actions_taken.append({
                    "type": "CAPITAL",
                    "label": f"Withdrew Rs. {amount:,.2f} to Bank",
                    "status": "COMPLETED"
                })
                publish_agent_event(
                    category="SYSTEM_ALERT",
                    title=f"Capital Withdrawn: Rs. {amount:,.2f}",
                    content=f"Withdrawal of Rs. {amount:,.2f} recorded from KTrade to personal bank.",
                    metadata=cap_res
                )
                if not reply_text:
                    reply_text = f"📤 Withdrew **Rs. {amount:,.2f}** from KTrade to bank account. Available cash balance debited."

        elif action_type in {"LOG_DIVIDEND_IN_BANK", "LOG_DIVIDEND_ENTITLED"}:
            ticker = str(action_data.get("ticker", "")).strip().upper()
            shares = action_data.get("shares_held")
            dps = action_data.get("dps_gross", 0.0)
            net_amt = action_data.get("net_amount")
            gross_amt = action_data.get("gross_amount")
            tax_rate = action_data.get("tax_rate_pct", 15.00)
            status = "CREDITED_TO_BANK" if action_type == "LOG_DIVIDEND_IN_BANK" else "ENTITLED"
            notes = action_data.get("notes") or f"Logged via Dashboard Chat Copilot ({status})"

            if ticker:
                div_res = record_dividend(
                    ticker=ticker,
                    shares_held=shares,
                    dps_gross=dps,
                    gross_amount=gross_amt,
                    tax_rate_pct=tax_rate,
                    net_amount=net_amt,
                    status=status,
                    notes=notes
                )
                actions_taken.append({
                    "type": "DIVIDEND",
                    "label": f"{ticker} Div: Rs. {div_res['net_amount']:,.2f} ({status})",
                    "status": "COMPLETED"
                })
                publish_agent_event(
                    category="SYSTEM_ALERT",
                    title=f"Dividend Logged: {ticker} ({status})",
                    content=f"Logged dividend for {ticker}: Gross Rs. {div_res['gross_amount']:,.2f}, WHT Rs. {div_res['tax_withheld']:,.2f}, Net Rs. {div_res['net_amount']:,.2f}.",
                    metadata=div_res
                )
                if not reply_text:
                    stage_label = "in your personal bank account" if status == "CREDITED_TO_BANK" else "as entitled (pending bank credit)"
                    reply_text = f"💰 Logged **{ticker}** dividend of **Rs. {div_res['net_amount']:,.2f}** net {stage_label} (WHT Rs. {div_res['tax_withheld']:,.2f} deducted at {div_res['tax_rate_pct']}%)."

        elif action_type == "TRANSFER_DIVIDEND_TO_KTRADE":
            target = action_data.get("dividend_id") or action_data.get("ticker")
            if target:
                trans_res = transfer_dividend_to_ktrade(target)
                actions_taken.append({
                    "type": "DIVIDEND_REINVEST",
                    "label": f"Transferred {trans_res['ticker']} Div Rs. {trans_res['net_amount']:,.2f} to KTrade",
                    "status": "COMPLETED"
                })
                publish_agent_event(
                    category="SYSTEM_ALERT",
                    title=f"Dividend Reinvested: {trans_res['ticker']} Rs. {trans_res['net_amount']:,.2f}",
                    content=f"Dividend for {trans_res['ticker']} moved from bank to KTrade available cash balance.",
                    metadata=trans_res
                )
                if not reply_text:
                    reply_text = f"🚀 Transferred **{trans_res['ticker']}** dividend of **Rs. {trans_res['net_amount']:,.2f}** from bank into KTrade cash. Available cash is now deployed and ready."

        elif action_type == "LOG_RESEARCH_REPORT":
            report_items = action_data.get("reports") if isinstance(action_data.get("reports"), list) else [action_data]
            for rep in report_items:
                if not isinstance(rep, dict):
                    continue
                source = str(rep.get("source") or "Uploaded Broker PDF").strip()
                ticker = str(rep.get("ticker") or "MACRO").strip().upper()
                subject = str(rep.get("subject") or f"{ticker} Research Report").strip()
                sentiment = str(rep.get("sentiment") or "NEUTRAL").strip().upper()
                if sentiment not in {"BULLISH", "BEARISH", "NEUTRAL"}:
                    sentiment = "NEUTRAL"
                summary_body = str(rep.get("summary") or reply_text or "Institutional research report analyzed.").strip()

                try:
                    conn = get_db_connection()
                    cur = conn.cursor()
                    cur.execute(
                        """
                        INSERT INTO institutional_reports (source, ticker, subject, sentiment, summary)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (source, ticker, subject, sentiment, summary_body)
                    )
                    conn.commit()
                    cur.close()
                    conn.close()
                except Exception as rep_err:
                    print(f"⚠️ [Chat Copilot] Could not insert into institutional_reports: {rep_err}")

                publish_agent_event(
                    category="MACRO_NEWS",
                    title=f"📑 Broker Report ({source}): {ticker} [{sentiment}]",
                    content=summary_body[:4000],
                    metadata=rep
                )
                actions_taken.append({
                    "type": "RESEARCH",
                    "label": f"Logged {ticker} Report ({sentiment})",
                    "status": "COMPLETED"
                })

        elif action_type == "CHART_ANALYSIS":
            chart_items = action_data.get("charts") if isinstance(action_data.get("charts"), list) else [action_data]
            for ch in chart_items:
                if not isinstance(ch, dict):
                    continue
                ticker = str(ch.get("ticker") or "PSX").strip().upper()
                setup = str(ch.get("setup") or "TECHNICAL_REVIEW").strip().upper()
                summary_body = str(ch.get("summary") or reply_text or "Technical chart evaluated.").strip()
                publish_agent_event(
                    category="DESK_MANAGER",
                    title=f"📈 Technical Chart Review: {ticker} ({setup})",
                    content=summary_body[:4000],
                    metadata=ch
                )
                actions_taken.append({
                    "type": "CHART",
                    "label": f"Chart Evaluated: {ticker} ({setup})",
                    "status": "COMPLETED"
                })

    action_queue: List[tuple] = []
    if isinstance(parsed.get("actions"), list):
        for item in parsed["actions"]:
            if isinstance(item, dict) and item.get("action_type"):
                action_queue.append((str(item["action_type"]), item.get("action_data") or {}))
    else:
        action_queue.append((primary_action_type, primary_action_data))
        if isinstance(parsed.get("additional_actions"), list):
            for extra in parsed["additional_actions"]:
                if isinstance(extra, dict) and extra.get("action_type"):
                    action_queue.append((str(extra["action_type"]), extra.get("action_data") or {}))

    for act_type, act_data in action_queue:
        try:
            _execute_action(act_type, act_data)
        except Exception as exec_err:
            print(f"❌ [Chat Copilot Action Execution Error ({act_type})]: {exec_err}")
            actions_taken.append({
                "type": "ERROR",
                "label": f"Action Failed: {str(exec_err)[:50]}",
                "status": "FAILED"
            })
            reply_text += f"\n\n*(Note: Automated ledger adjustment failed: {exec_err})*"

    if not reply_text:
        reply_text = "I have processed your portfolio command and attachments. All records and ledgers are synchronized."

    try:
        save_chat_message("user", persisted_user_msg, [])
        save_chat_message("assistant", reply_text, actions_taken)
    except Exception as save_err:
        print(f"⚠️ [Chat Copilot] Could not persist to chat_messages: {save_err}")

    return {
        "reply": reply_text,
        "actions_taken": actions_taken,
        "action_type": primary_action_type,
        "status": "success"
    }


CHAT_MULTIMODAL_TIMEOUT_SECONDS = 30.0
MAX_IMAGE_DIMENSION_PX = 1024
JPEG_COMPRESSION_QUALITY = 85


def optimize_uploaded_file_for_gemini(
    file_obj: Dict[str, Any],
    max_dim: int = MAX_IMAGE_DIMENSION_PX,
    jpeg_quality: int = JPEG_COMPRESSION_QUALITY
) -> Dict[str, Any]:
    """
    Downscales and compresses `image/*` uploads in memory (`io.BytesIO`) to <=1024px JPEG (quality=85)
    using `PIL.Image` to prevent OOM crashes on Render's 512MB RAM tier. Leaves PDFs untouched as-is.
    """
    if not isinstance(file_obj, dict) or file_obj.get("_optimized"):
        return file_obj

    raw_bytes = file_obj.get("bytes")
    mime_type = str(file_obj.get("mime_type") or "").strip().lower()
    filename = str(file_obj.get("filename") or "attachment").strip()

    # Keep PDFs and non-image documents untouched as-is
    if not raw_bytes or not mime_type.startswith("image/"):
        out = dict(file_obj)
        out["_optimized"] = True
        return out

    try:
        import io
        from PIL import Image, ImageOps

        orig_size_kb = len(raw_bytes) / 1024.0
        with io.BytesIO(raw_bytes) as in_buf:
            with Image.open(in_buf) as img:
                img = ImageOps.exif_transpose(img)
                orig_w, orig_h = img.size

                # Convert RGBA / palette / non-RGB modes cleanly before JPEG encoding
                if img.mode in ("RGBA", "LA") or (img.mode == "P" and "transparency" in img.info):
                    alpha_rgba = img.convert("RGBA")
                    bg = Image.new("RGB", alpha_rgba.size, (255, 255, 255))
                    bg.paste(alpha_rgba, mask=alpha_rgba.split()[3])
                    rgb_img = bg
                    alpha_rgba.close()
                elif img.mode != "RGB":
                    rgb_img = img.convert("RGB")
                else:
                    rgb_img = img.copy()

        try:
            resample_filter = getattr(Image, "Resampling", Image).LANCZOS
            rgb_img.thumbnail((max_dim, max_dim), resample_filter)
            new_w, new_h = rgb_img.size

            out_buf = io.BytesIO()
            rgb_img.save(out_buf, format="JPEG", quality=jpeg_quality, optimize=True)
            compressed_bytes = out_buf.getvalue()
            out_buf.close()
        finally:
            rgb_img.close()

        new_size_kb = len(compressed_bytes) / 1024.0
        base_stem, _ = os.path.splitext(filename)
        new_filename = f"{base_stem or 'image'}.jpg"
        reduction_pct = ((1.0 - (len(compressed_bytes) / max(1, len(raw_bytes)))) * 100.0)
        print(
            f"🖼️ [Image Optimizer] Compressed '{filename}' ({orig_w}x{orig_h} -> {new_w}x{new_h}): "
            f"{orig_size_kb:,.1f} KB -> {new_size_kb:,.1f} KB ({reduction_pct:+.1f}% saved)",
            flush=True
        )
        return {
            "filename": new_filename,
            "mime_type": "image/jpeg",
            "bytes": compressed_bytes,
            "_optimized": True
        }
    except Exception as img_err:
        print(f"⚠️ [Image Optimizer] Could not compress '{filename}' ({img_err}); sending original bytes.", flush=True)
        fallback = dict(file_obj)
        fallback["_optimized"] = True
        return fallback


async def process_dashboard_chat_with_multimodal(
    user_message: str,
    processed_files: Optional[List[Dict[str, Any]]] = None
) -> dict:
    """
    Multimodal & conversational AI copilot for the Matte Dark Dashboard:
    - Automatically downscales/compresses `image/*` attachments to <=1024px JPEG (quality=85)
      while keeping PDFs as-is.
    - Runs inside `user_priority_context()` so user-initiated chat bypasses background sweep
      semaphores (YouTube / news / sentinel) instead of waiting behind Concurrency=2.
    - Enforces a hard 30.0s `asyncio.wait_for` timeout on Gemini multimodal calls, releasing
      concurrency locks immediately and returning a clean JSON timeout reply.
    """
    from rate_limiter import user_priority_context

    with user_priority_context():
        clean_msg = (user_message or "").strip()
        raw_valid_files = [
            f for f in (processed_files or [])
            if isinstance(f, dict) and f.get("bytes") and f.get("mime_type")
        ]
        valid_files = []
        for f_item in raw_valid_files:
            if f_item.get("_optimized"):
                valid_files.append(f_item)
            else:
                valid_files.append(await asyncio.to_thread(optimize_uploaded_file_for_gemini, f_item))
        has_attachments = len(valid_files) > 0

        if not clean_msg and not has_attachments:
            return {"reply": "Please enter a message or attach PDFs/images.", "actions_taken": [], "status": "empty"}

        if has_attachments and (not clean_msg or clean_msg == "Analyze attached files."):
            names_str = ", ".join(str(f.get("filename") or f["mime_type"]) for f in valid_files)
            clean_msg = (
                f"Analyze all {len(valid_files)} uploaded file(s) ({names_str}). "
                f"For any KTrade daily trade confirmation PDF, extract and log all executed trades via LOG_KTRADE_STATEMENT. "
                f"For any PSX broker research report PDF, evaluate the thesis against our internal valuation targets and log via LOG_RESEARCH_REPORT. "
                f"For any technical chart screenshot, evaluate RSI/MACD/price structure and breakout levels via CHART_ANALYSIS. "
                f"If multiple files require different actions, include additional_actions so every file is logged properly."
            )

        if has_attachments:
            attached_label = ", ".join(str(f.get("filename") or f["mime_type"]) for f in valid_files)
            raw_user_text = (user_message or "").strip()
            if raw_user_text and raw_user_text != "Analyze attached files.":
                persisted_user_msg = f"📎 [Attached ({len(valid_files)}): {attached_label}]\n{raw_user_text}"
            else:
                persisted_user_msg = f"📎 [Attached ({len(valid_files)}): {attached_label}]"
        else:
            persisted_user_msg = clean_msg

        context_str = await asyncio.to_thread(get_live_financial_context)
        prompt = f"{context_str}\n\nUser Query: {clean_msg}\n\nRespond with strict JSON."

        raw_response = None

        if has_attachments:
            from google import genai
            from google.genai import types

            active_key = get_active_google_api_key() or os.getenv("GOOGLE_API_KEY")
            clean_key = active_key.strip().strip('"').strip("'") if active_key else None
            client = genai.Client(api_key=clean_key, http_options={"timeout": 28000}) if clean_key else None
            if client is not None and clean_key:
                client._active_google_key = clean_key

            file_manifest = "\n".join(
                f"- File {idx + 1}: {f.get('filename') or f'attachment_{idx + 1}'} ({f['mime_type']}, {len(f['bytes'])} bytes)"
                for idx, f in enumerate(valid_files)
            )
            full_multimodal_prompt = (
                f"{SYSTEM_COPILOT_PROMPT}\n\n"
                f"ATTACHED FILES ({len(valid_files)}):\n{file_manifest}\n\n"
                f"{prompt}"
            )

            # Construct multi-part contents payload for Gemini (gemini-3.8-flash cascade via google-genai SDK)
            contents: List[Any] = [full_multimodal_prompt]
            for file_obj in valid_files:
                contents.append(
                    types.Part.from_bytes(
                        data=file_obj["bytes"],
                        mime_type=file_obj["mime_type"]
                    )
                )

            async def _invoke_multimodal_gemini() -> Optional[str]:
                try:
                    print(
                        f"📎 [Chat Copilot] Priority multimodal call for {len(valid_files)} attachment(s) (timeout={CHAT_MULTIMODAL_TIMEOUT_SECONDS}s)...",
                        flush=True
                    )
                    return await generate_with_fallback(
                        client,
                        contents,
                        task=TASK_DEEP_REASONING,
                        priority=True
                    )
                except (asyncio.CancelledError, asyncio.TimeoutError, TimeoutError):
                    raise
                except Exception as inline_err:
                    print(f"⚠️ [Chat Copilot] Inline Part.from_bytes threw ({inline_err}). Trying Gemini Files API fallback...", flush=True)
                    if client is None:
                        raise
                    temp_paths: List[str] = []
                    uploaded_refs: List[Any] = []
                    try:
                        for file_obj in valid_files:
                            fname = str(file_obj.get("filename") or "")
                            mtype = str(file_obj["mime_type"])
                            safe_ext = os.path.splitext(fname)[1] or (".pdf" if "pdf" in mtype.lower() else ".png")
                            temp_path = f"temp_copilot_{uuid.uuid4().hex[:8]}{safe_ext}"
                            with open(temp_path, "wb") as tf:
                                tf.write(file_obj["bytes"])
                            temp_paths.append(temp_path)
                            uploaded_file = await asyncio.to_thread(
                                client.files.upload,
                                file=temp_path,
                                config=types.UploadFileConfig(mime_type=mtype)
                            )
                            uploaded_refs.append(uploaded_file)

                        return await generate_with_fallback(
                            client,
                            [full_multimodal_prompt, *uploaded_refs],
                            task=TASK_MULTIMODAL,
                            priority=True
                        )
                    finally:
                        for u_ref in uploaded_refs:
                            try:
                                client.files.delete(name=u_ref.name)
                            except Exception:
                                pass
                        for t_path in temp_paths:
                            if os.path.exists(t_path):
                                try:
                                    os.remove(t_path)
                                except Exception:
                                    pass

            try:
                raw_response = await asyncio.wait_for(
                    _invoke_multimodal_gemini(),
                    timeout=CHAT_MULTIMODAL_TIMEOUT_SECONDS
                )
            except (asyncio.TimeoutError, TimeoutError):
                print(
                    f"⏱️ [Chat Copilot] Gemini multimodal call timed out after {CHAT_MULTIMODAL_TIMEOUT_SECONDS}s; priority lock released.",
                    flush=True
                )
                return {
                    "reply": "⚠️ Gemini multimodal processing timed out. Please try sending a smaller image or text only.",
                    "actions_taken": [],
                    "status": "timeout"
                }
            except Exception as mm_err:
                print(f"❌ [Chat Copilot] Gemini multimodal generation failed: {mm_err}", flush=True)
                return {
                    "reply": f"⚠️ Gemini multimodal processing encountered an error: {mm_err}",
                    "actions_taken": [],
                    "status": "error"
                }
        else:
            # Pure text command: fast Groq cascade first, then priority Gemini fallback with 30s timeout
            try:
                raw_response = await asyncio.to_thread(generate_text_groq, prompt, SYSTEM_COPILOT_PROMPT)
            except Exception as g_err:
                print(f"⚠️ [Chat Copilot] Groq generation failed: {g_err}")

            if not raw_response or not raw_response.strip():
                print("ℹ️ [Chat Copilot] Cascading to priority Gemini fallback for copilot...")
                try:
                    raw_response = await asyncio.wait_for(
                        generate_with_fallback(
                            None,
                            [f"{SYSTEM_COPILOT_PROMPT}\n\n{prompt}"],
                            task=TASK_DEEP_REASONING,
                            priority=True
                        ),
                        timeout=CHAT_MULTIMODAL_TIMEOUT_SECONDS
                    )
                except (asyncio.TimeoutError, TimeoutError):
                    print(
                        f"⏱️ [Chat Copilot] Gemini text fallback timed out after {CHAT_MULTIMODAL_TIMEOUT_SECONDS}s; priority lock released.",
                        flush=True
                    )
                    return {
                        "reply": "⚠️ AI Copilot request timed out while waiting for Gemini. Please retry in a moment.",
                        "actions_taken": [],
                        "status": "timeout"
                    }
                except Exception as fb_err:
                    print(f"❌ [Chat Copilot] Gemini fallback failed: {fb_err}")
                    raw_response = None

        return await asyncio.to_thread(parse_copilot_response, raw_response or "", persisted_user_msg)


async def process_dashboard_chat(user_message: str) -> dict:
    """Backwards-compatible wrapper for text-only copilot calls."""
    return await process_dashboard_chat_with_multimodal(user_message=user_message, processed_files=[])


if __name__ == "__main__":
    test_msg = "What is our current portfolio net worth and available cash?"
    print(f"Testing Chat Copilot with: '{test_msg}'...")
    res = asyncio.run(process_dashboard_chat(test_msg))
    print("Result:", json.dumps(res, indent=2))
