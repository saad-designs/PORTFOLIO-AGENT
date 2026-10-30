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
  "reply": "Executive, concise direct response."
}

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


def parse_copilot_response(raw_response: str, persisted_user_msg: str) -> dict:
    """
    Parses structured JSON copilot output, executes ledger/research/feed actions,
    persists chat history, and formats the response for the frontend drawer.
    """
    parsed = extract_json_payload(raw_response or "")
    action_type = parsed.get("action_type", "GENERAL_QUERY")
    action_data = parsed.get("action_data", {}) or {}
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

    try:
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
            source = str(action_data.get("source") or "Uploaded Broker PDF").strip()
            ticker = str(action_data.get("ticker") or "MACRO").strip().upper()
            subject = str(action_data.get("subject") or f"{ticker} Research Report").strip()
            sentiment = str(action_data.get("sentiment") or "NEUTRAL").strip().upper()
            if sentiment not in {"BULLISH", "BEARISH", "NEUTRAL"}:
                sentiment = "NEUTRAL"
            summary_body = str(action_data.get("summary") or reply_text or "Institutional research report analyzed.").strip()

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
                metadata=action_data
            )
            actions_taken.append({
                "type": "RESEARCH",
                "label": f"Logged {ticker} Report ({sentiment})",
                "status": "COMPLETED"
            })

        elif action_type == "CHART_ANALYSIS":
            ticker = str(action_data.get("ticker") or "PSX").strip().upper()
            setup = str(action_data.get("setup") or "TECHNICAL_REVIEW").strip().upper()
            summary_body = str(action_data.get("summary") or reply_text or "Technical chart evaluated.").strip()
            publish_agent_event(
                category="DESK_MANAGER",
                title=f"📈 Technical Chart Review: {ticker} ({setup})",
                content=summary_body[:4000],
                metadata=action_data
            )
            actions_taken.append({
                "type": "CHART",
                "label": f"Chart Evaluated: {ticker} ({setup})",
                "status": "COMPLETED"
            })

    except Exception as exec_err:
        print(f"❌ [Chat Copilot Action Execution Error]: {exec_err}")
        actions_taken.append({
            "type": "ERROR",
            "label": f"Action Failed: {str(exec_err)[:50]}",
            "status": "FAILED"
        })
        reply_text += f"\n\n*(Note: Automated ledger adjustment failed: {exec_err})*"

    if not reply_text:
        reply_text = "I have processed your portfolio command and attachment. All records and ledgers are synchronized."

    try:
        save_chat_message("user", persisted_user_msg, [])
        save_chat_message("assistant", reply_text, actions_taken)
    except Exception as save_err:
        print(f"⚠️ [Chat Copilot] Could not persist to chat_messages: {save_err}")

    return {
        "reply": reply_text,
        "actions_taken": actions_taken,
        "action_type": action_type,
        "status": "success"
    }


async def process_dashboard_chat_with_multimodal(
    user_message: str,
    file_bytes: Optional[bytes] = None,
    mime_type: Optional[str] = None,
    filename: Optional[str] = None
) -> dict:
    """
    Multimodal & conversational AI copilot for the Matte Dark Dashboard:
    - Text + PDF/Image attachments route directly to Gemini (gemini-3.8-flash cascade via google-genai SDK)
      using types.Part.from_bytes (with automatic Files API fallback for large PDFs).
    - Pure text queries route to Groq fast text cascade first, with Gemini fallback.
    """
    clean_msg = (user_message or "").strip()
    has_attachment = bool(file_bytes and mime_type)

    if not clean_msg and not has_attachment:
        return {"reply": "Please enter a message or attach a PDF/image.", "actions_taken": [], "status": "empty"}

    if has_attachment and not clean_msg:
        clean_msg = (
            f"Analyze this uploaded file ({filename or mime_type}). "
            f"If it is a KTrade daily trade confirmation PDF, extract and log all executed trades via LOG_KTRADE_STATEMENT. "
            f"If it is a PSX broker research report PDF, evaluate the thesis against our internal valuation targets and log via LOG_RESEARCH_REPORT. "
            f"If it is a technical chart screenshot, evaluate RSI/MACD/price structure and breakout levels via CHART_ANALYSIS."
        )

    persisted_user_msg = (
        f"📎 [Attached: {filename or mime_type}]\n{(user_message or '').strip()}".strip()
        if has_attachment
        else clean_msg
    )

    context_str = await asyncio.to_thread(get_live_financial_context)
    prompt = f"{context_str}\n\nUSER MESSAGE:\n\"{clean_msg}\"\n\nRespond with strict JSON."

    raw_response = None

    if has_attachment:
        from google import genai
        from google.genai import types

        active_key = get_active_google_api_key() or os.getenv("GOOGLE_API_KEY")
        clean_key = active_key.strip().strip('"').strip("'") if active_key else None
        client = genai.Client(api_key=clean_key, http_options={"timeout": 300000}) if clean_key else None

        full_multimodal_prompt = (
            f"{SYSTEM_COPILOT_PROMPT}\n\n"
            f"ATTACHED FILE: {filename or 'attachment'} ({mime_type}, {len(file_bytes)} bytes)\n\n"
            f"{prompt}"
        )

        try:
            print(f"📎 [Chat Copilot] Analyzing multimodal upload '{filename or 'attachment'}' ({mime_type}, {len(file_bytes)} bytes) via Gemini...", flush=True)
            media_part = types.Part.from_bytes(data=file_bytes, mime_type=mime_type)
            raw_response = await generate_with_fallback(
                client,
                [media_part, full_multimodal_prompt],
                task=TASK_DEEP_REASONING
            )
        except Exception as inline_err:
            print(f"⚠️ [Chat Copilot] Inline Part.from_bytes threw ({inline_err}). Falling back to Gemini Files API...", flush=True)
            if client is not None:
                safe_ext = os.path.splitext(filename or "")[1] or (".pdf" if "pdf" in (mime_type or "").lower() else ".png")
                temp_path = f"temp_copilot_{uuid.uuid4().hex[:8]}{safe_ext}"
                try:
                    with open(temp_path, "wb") as tf:
                        tf.write(file_bytes)
                    uploaded_file = await asyncio.to_thread(
                        client.files.upload,
                        file=temp_path,
                        config=types.UploadFileConfig(mime_type=mime_type)
                    )
                    try:
                        raw_response = await generate_with_fallback(
                            client,
                            [uploaded_file, full_multimodal_prompt],
                            task=TASK_MULTIMODAL
                        )
                    finally:
                        try:
                            client.files.delete(name=uploaded_file.name)
                        except Exception:
                            pass
                except Exception as upload_err:
                    print(f"❌ [Chat Copilot] Gemini Files API fallback failed: {upload_err}", flush=True)
                finally:
                    if os.path.exists(temp_path):
                        try:
                            os.remove(temp_path)
                        except Exception:
                            pass
    else:
        # Pure text command: fast Groq cascade first, then Gemini deep reasoning fallback
        try:
            raw_response = await asyncio.to_thread(generate_text_groq, prompt, SYSTEM_COPILOT_PROMPT)
        except Exception as g_err:
            print(f"⚠️ [Chat Copilot] Groq generation failed: {g_err}")

        if not raw_response or not raw_response.strip():
            print("ℹ️ [Chat Copilot] Cascading to Gemini fallback for copilot...")
            try:
                raw_response = await generate_with_fallback(
                    None,
                    [f"{SYSTEM_COPILOT_PROMPT}\n\n{prompt}"],
                    task=TASK_DEEP_REASONING
                )
            except Exception as fb_err:
                print(f"❌ [Chat Copilot] Gemini fallback failed: {fb_err}")
                raw_response = None

    return await asyncio.to_thread(parse_copilot_response, raw_response or "", persisted_user_msg)


async def process_dashboard_chat(user_message: str) -> dict:
    """Backwards-compatible wrapper for text-only copilot calls."""
    return await process_dashboard_chat_with_multimodal(user_message=user_message)


if __name__ == "__main__":
    test_msg = "What is our current portfolio net worth and available cash?"
    print(f"Testing Chat Copilot with: '{test_msg}'...")
    res = asyncio.run(process_dashboard_chat(test_msg))
    print("Result:", json.dumps(res, indent=2))
