import os
import sys
import json
import re
import asyncio
from typing import Dict, Any, List

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from model_router import generate_text_groq, generate_with_fallback, TASK_DEEP_REASONING
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

SYSTEM_COPILOT_PROMPT = """You are the Senior PSX Portfolio Co-Investor & Automated Ledger Operator.
Your job is to parse the user's natural language updates or questions regarding the Pakistan Stock Exchange (PSX) portfolio, execute structured financial actions, and provide concise, executive, direct, no-fluff answers.

You must respond ONLY with a valid JSON object matching this schema:
{
  "action_type": "BUY_STOCK" | "SELL_STOCK" | "DEPOSIT_CASH" | "WITHDRAW_CASH" | "LOG_DIVIDEND_IN_BANK" | "LOG_DIVIDEND_ENTITLED" | "TRANSFER_DIVIDEND_TO_KTRADE" | "GENERAL_QUERY",
  "action_data": { ... },
  "reply": "Executive, concise direct response."
}

ACTION DEFINITIONS:
1. BUY_STOCK:
   - When user bought shares (e.g. "Bought 500 HUBC at 118.50", "Buy 200 SYS at 440")
   - action_data: {"ticker": "HUBC", "side": "BUY", "quantity": 500.0, "price": 118.50, "fees": 0.0, "notes": "Trade note if any"}
2. SELL_STOCK:
   - When user sold shares (e.g. "Sold 300 MEBL at 245.00")
   - action_data: {"ticker": "MEBL", "side": "SELL", "quantity": 300.0, "price": 245.00, "fees": 0.0, "notes": "Trade note if any"}
3. DEPOSIT_CASH:
   - Fresh capital deposited into KTrade (e.g. "Deposited 100,000 into KTrade", "Added 50k cash to broker")
   - action_data: {"amount": 100000.0, "notes": "Notes if any"}
4. WITHDRAW_CASH:
   - Cash withdrawn from KTrade to personal bank (e.g. "Withdrew 25,000 from KTrade")
   - action_data: {"amount": 25000.0, "notes": "Notes if any"}
5. LOG_DIVIDEND_IN_BANK:
   - Dividend received / credited into personal bank account (e.g. "Received Rs 4/share dividend from HUBC in bank", "Got 8,500 net dividend from MEBL in my bank", "HUBC paid 5/share div into bank")
   - action_data: {"ticker": "HUBC", "shares_held": null or float, "dps_gross": null or float, "net_amount": null or float, "gross_amount": null or float, "tax_rate_pct": 15.0, "notes": "Notes if any"}
   - Note: If user only mentions DPS (e.g. "Rs 4/share from HUBC"), set "dps_gross": 4.0, "shares_held": null (the engine auto-looks up current shares held). If user mentions net total (e.g. "Got 8,500 net from MEBL"), set "net_amount": 8500.0.
6. LOG_DIVIDEND_ENTITLED:
   - Dividend announced or entitled but not yet credited to bank (e.g. "HUBC announced Rs 4 dividend", "FFC entitled to 7.5 DPS")
   - action_data: {"ticker": "HUBC", "shares_held": null or float, "dps_gross": null or float, "net_amount": null or float, "gross_amount": null or float, "tax_rate_pct": 15.0, "notes": "Notes if any"}
7. TRANSFER_DIVIDEND_TO_KTRADE:
   - User moved previously credited bank dividend into KTrade broker account for reinvestment (e.g. "Sent the MEBL dividend from bank to KTrade", "Transferred HUBC dividend to KTrade", "Reinvested MEBL dividend")
   - action_data: {"ticker": "MEBL", "dividend_id": null or int}
8. GENERAL_QUERY:
   - Portfolio inquiries, thesis review, valuations, macro queries, mentor research questions, or health checks (e.g. "What is our current cash?", "How much HUBC do we hold?", "Summarize recent KTrade signals")
   - action_data: {}

CRITICAL RULES:
- Output STRICT JSON only. Do not wrap in markdown or prefix with text.
- Be exact with ticker symbols (SYS, HUBC, LUCK, FFC, EFERT, MEBL, MARI, etc.).
- Never invent trades if the user is merely asking a question.
- Responses must be sharp, highly confident, and numbers-backed.
"""

def extract_json_payload(raw_text: str) -> dict:
    """Extracts and parses JSON object from model response."""
    if not raw_text:
        return {}
    text = raw_text.strip()
    # Strip markdown fences if present
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?", "", text).strip()
        text = re.sub(r"```$", "", text).strip()

    try:
        return json.loads(text)
    except Exception:
        # Search for first { and last }
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            try:
                return json.loads(text[start:end+1])
            except Exception:
                pass
    return {}

async def process_dashboard_chat(user_message: str) -> dict:
    """
    Core conversational AI copilot for the Matte Dark Dashboard.
    1. Parses user message using Groq (llama-3.3-70b-versatile via model_router.py, with Gemini fallback).
    2. Performs structured ledger actions (Trades, Capital, Dividends).
    3. Persists conversation and badges in Supabase chat_messages.
    4. Returns {"reply": str, "actions_taken": list, "status": "success"}.
    """
    clean_msg = user_message.strip()
    if not clean_msg:
        return {"reply": "Please enter a message or command.", "actions_taken": [], "status": "empty"}

    # 1. Fetch live financial state & context
    summary = {}
    try:
        summary = get_complete_financial_summary()
    except Exception as e:
        print(f"⚠️ [Chat Copilot] Failed fetching financial summary: {e}")

    kpis = summary.get("financial_kpis", {})
    holdings = summary.get("holdings", [])
    recent_feed = get_agent_feed(limit=5)
    recent_history = get_chat_history(limit=6)

    playbook_rules = []
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT source, rule FROM active_playbook ORDER BY created_at DESC LIMIT 5;")
        playbook_rules = [{"source": r[0], "rule": r[1]} for r in cur.fetchall()]
        cur.close()
        conn.close()
    except Exception:
        pass

    context_str = f"""
LIVE FINANCIAL CONTEXT:
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

ACTIVE PLAYBOOK RULES:
{json.dumps(playbook_rules, indent=2)}

RECENT RELEVANT SIGNALS / FEED:
{json.dumps([{f['category']: f"{f['title']} - {f['content'][:120]}"} for f in recent_feed], indent=2)}
"""

    prompt = f"{context_str}\n\nUSER MESSAGE:\n\"{clean_msg}\"\n\nRespond with strict JSON."

    # 2. Invoke Groq llama-3.3-70b-versatile with Gemini fallback
    raw_response = None
    try:
        raw_response = await asyncio.to_thread(generate_text_groq, prompt, SYSTEM_COPILOT_PROMPT)
    except Exception as g_err:
        print(f"⚠️ [Chat Copilot] Groq generation failed: {g_err}")

    if not raw_response or not raw_response.strip():
        # Fallback to Gemini
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

    parsed = extract_json_payload(raw_response or "")
    action_type = parsed.get("action_type", "GENERAL_QUERY")
    action_data = parsed.get("action_data", {})
    reply_text = parsed.get("reply", "")

    actions_taken = []

    # 3. Execute identified ledger actions
    try:
        if action_type in {"BUY_STOCK", "SELL_STOCK"}:
            ticker = action_data.get("ticker", "").strip().upper()
            side = "BUY" if action_type == "BUY_STOCK" else "SELL"
            qty = float(action_data.get("quantity", 0))
            price = float(action_data.get("price", 0))
            fees = float(action_data.get("fees", 0.0))
            notes = action_data.get("notes") or f"Executed via Dashboard Chat Copilot"

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
                    content=f"{side} {qty:,.0f} {ticker} executed at Rs. {price:,.2f}. Cash adjusted by Rs. {trade_res['net_total']:,.2f}.",
                    metadata=trade_res
                )
                if not reply_text:
                    reply_text = f"✅ Executed **{side} {qty:,.0f} {ticker}** at **Rs. {price:,.2f}** (Net Total: Rs. {trade_res['net_total']:,.2f}). Holdings & cash updated."

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
            ticker = action_data.get("ticker", "").strip().upper()
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

    except Exception as exec_err:
        print(f"❌ [Chat Copilot Action Execution Error]: {exec_err}")
        actions_taken.append({
            "type": "ERROR",
            "label": f"Action Failed: {str(exec_err)[:50]}",
            "status": "FAILED"
        })
        reply_text += f"\n\n*(Note: Automated ledger adjustment failed: {exec_err})*"

    # Fallback if no reply was produced
    if not reply_text:
        reply_text = "I have processed your portfolio command. All records and ledgers are synchronized."

    # 4. Save messages to Supabase chat_messages
    try:
        save_chat_message("user", clean_msg, [])
        save_chat_message("assistant", reply_text, actions_taken)
    except Exception as save_err:
        print(f"⚠️ [Chat Copilot] Could not persist to chat_messages: {save_err}")

    return {
        "reply": reply_text,
        "actions_taken": actions_taken,
        "action_type": action_type,
        "status": "success"
    }

if __name__ == "__main__":
    test_msg = "What is our current portfolio net worth and available cash?"
    print(f"Testing Chat Copilot with: '{test_msg}'...")
    res = asyncio.run(process_dashboard_chat(test_msg))
    print("Result:", json.dumps(res, indent=2))
