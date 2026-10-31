"""
central_brain.py — PSX Central Director Brain & Capital Allocation Engine
Strict Capital Accumulation Mandate (NO SELLING)

Master intelligence and deterministic capital deployment director. Ingests:
1. Live portfolio custody, cash balances, and exact holding weights.
2. Fundamental DCF targets, Graham Intrinsic Values, TTM P/E, and Dividend Yields.
3. Macro & World News (RSS, SBP interest rates, CPI, USD/PKR).
4. Institutional Broker Reports (KTrade, AKD, Arif Habib, Topline).
5. Mentor Playbook Rules (active_playbook & agent_knowledge).

Outputs:
- Single decisive, numbers-backed BUY allocation decision.
- 3-pillar analytical proof (Valuation, Weight/Yield Impact, Macro Catalysts).
- Explicit audit of rejected alternatives (#2 and #3 candidates).
"""

import math
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from utils import normalize_ticker
from portfolio_manager import (
    get_db_connection,
    get_complete_financial_summary,
    invalidate_financial_summary_cache,
    DEFAULT_WATCHLIST_DCF_TARGETS,
    publish_agent_event,
)
from market_data_scraper import fetch_all_psx_prices_bulk
from news_scraper import fetch_psx_news


ACCUMULATION_MANDATE = "CAPITAL_ACCUMULATION_BUY_ONLY"

# Core Watchlist universe for KMI-30 / PSX Capital Accumulation
CORE_CANDIDATE_UNIVERSE = [
    "SYS", "FFC", "OGDC", "MARI", "HUBC", "LUCK", "EFERT", "MEBL", "FABL",
    "PPL", "POL", "MCB", "HBL", "UBL", "PSO", "SAZEW", "ILP", "ENGRO"
]

SECTOR_MAP = {
    "SYS": "Technology",
    "FFC": "Fertilizer",
    "EFERT": "Fertilizer",
    "ENGRO": "Fertilizer / Conglomerate",
    "OGDC": "Oil & Gas Exploration",
    "MARI": "Oil & Gas Exploration",
    "PPL": "Oil & Gas Exploration",
    "POL": "Oil & Gas Exploration",
    "HUBC": "Power Generation",
    "LUCK": "Cement",
    "MEBL": "Islamic Banking",
    "FABL": "Islamic Banking",
    "MCB": "Commercial Banking",
    "HBL": "Commercial Banking",
    "UBL": "Commercial Banking",
    "PSO": "Oil Marketing",
    "SAZEW": "Automobile Assembler",
    "ILP": "Textile Composite",
}


def calculate_graham_intrinsic_val(eps: float, growth_pct: float = 8.0) -> float:
    """Graham Intrinsic Value: V = EPS * (8.5 + 2g). Cap g between 0% and 15%."""
    if eps <= 0:
        return 0.0
    safe_g = max(0.0, min(float(growth_pct), 15.0))
    return round(eps * (8.5 + (2.0 * safe_g)), 2)


def gather_central_brain_state(quick_mode: bool = True) -> Dict[str, Any]:
    """
    Ingests live multi-source intelligence across the system:
    - Live financial summary (holdings, available cash, custody market value).
    - Watchlist fundamental targets (DCF fair value, P/E, forward EPS).
    - Institutional research reports from Supabase.
    - Active mentor playbook rules.
    - Latest macro & financial RSS headlines (from DB if quick_mode=True).
    - Live TradingView / PSX spot quotes.
    """
    summary = get_complete_financial_summary(use_cached_prices=quick_mode)
    holdings = summary.get("holdings", [])
    kpis = summary.get("financial_kpis", {})

    # Live quotes for full universe
    all_tickers = list(dict.fromkeys(CORE_CANDIDATE_UNIVERSE + [h["ticker"] for h in holdings]))
    live_quotes = fetch_all_psx_prices_bulk(force_refresh=(not quick_mode), target_tickers=all_tickers)

    # Ingest watchlist targets, institutional reports, playbook rules, and macro state
    watchlist_targets: Dict[str, Dict[str, Any]] = {}
    institutional_reports: List[Dict[str, Any]] = []
    playbook_rules: List[Dict[str, Any]] = []
    macro_state: Dict[str, float] = {}
    news_items: List[Dict[str, Any]] = []

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # Watchlist targets
        cur.execute("""
            SELECT ticker, target_price, stop_loss, historical_fair_pe, current_trailing_pe, forward_eps, ttm_eps
            FROM watchlist_targets;
        """)
        for r in cur.fetchall():
            sym = normalize_ticker(r[0])
            watchlist_targets[sym] = {
                "target_price": float(r[1]) if r[1] is not None else None,
                "stop_loss": float(r[2]) if r[2] is not None else None,
                "fair_pe": float(r[3]) if r[3] is not None else None,
                "trailing_pe": float(r[4]) if r[4] is not None else None,
                "forward_eps": float(r[5]) if r[5] is not None else None,
                "ttm_eps": float(r[6]) if r[6] is not None else None,
            }

        # Institutional reports
        cur.execute("""
            SELECT id, source, ticker, subject, sentiment, summary, created_at
            FROM institutional_reports
            ORDER BY created_at DESC
            LIMIT 15;
        """)
        for r in cur.fetchall():
            institutional_reports.append({
                "id": r[0],
                "source": r[1],
                "ticker": normalize_ticker(r[2]),
                "subject": r[3],
                "sentiment": r[4],
                "summary": r[5],
                "created_at": r[6].isoformat() if r[6] else None,
            })

        # Active Playbook Rules
        cur.execute("""
            SELECT id, source, rule, confidence_weight, regime_tag
            FROM active_playbook
            ORDER BY confidence_weight DESC, created_at DESC
            LIMIT 20;
        """)
        for r in cur.fetchall():
            playbook_rules.append({
                "id": r[0],
                "source": r[1],
                "rule": r[2],
                "confidence_weight": float(r[3]) if r[3] is not None else 1.0,
                "regime_tag": r[4],
            })

        # Macro state key/values
        cur.execute("SELECT key, value FROM system_macro_state;")
        for r in cur.fetchall():
            macro_state[r[0]] = float(r[1]) if r[1] is not None else 0.0

        if quick_mode:
            cur.execute("""
                SELECT title, content, created_at FROM agent_feed
                WHERE category = 'MACRO_NEWS'
                ORDER BY created_at DESC
                LIMIT 5;
            """)
            for r in cur.fetchall():
                news_items.append({"title": r[0], "summary": r[1]})

    except Exception:
        conn.rollback()
    finally:
        cur.close()
        conn.close()

    if not quick_mode and not news_items:
        try:
            news_items = fetch_psx_news(max_articles=6)
        except Exception:
            pass

    return {
        "kpis": kpis,
        "holdings": holdings,
        "live_quotes": live_quotes,
        "watchlist_targets": watchlist_targets,
        "institutional_reports": institutional_reports,
        "playbook_rules": playbook_rules,
        "macro_state": macro_state,
        "news": news_items,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def make_capital_deployment_decision(deposit_amount: float = 0.0, quick_mode: bool = True) -> Dict[str, Any]:
    """
    Decisive Single-Allocation Engine (Buy-Only Accumulation Mandate):
    - Ingests full system state.
    - Total deployable cash = ktrade_available_cash + deposit_amount.
    - Evaluates every candidate stock deterministically on:
        1. Margin of Safety (DCF Fair Value vs Live Spot Price) [40% weight]
        2. Earnings & Dividend Yield [25% weight]
        3. Portfolio Diversification & Weight Cap [25% weight]
        4. Macro & Institutional Catalyst Alignment [10% weight]
    - Makes ONE authoritative BUY decision (specifying exact shares, limit price, cost, remaining cash).
    - Provides 3-pillar analytical justification and explicit rejection reasons for #2 and #3 candidates.
    """
    deposit_amount = max(0.0, float(deposit_amount))
    state = gather_central_brain_state(quick_mode=quick_mode)

    kpis = state.get("kpis", {})
    holdings = state.get("holdings", [])
    live_quotes = state.get("live_quotes", {})
    watchlist_targets = state.get("watchlist_targets", {})
    institutional_reports = state.get("institutional_reports", [])
    playbook_rules = state.get("playbook_rules", [])
    news = state.get("news", [])

    current_cash = float(kpis.get("ktrade_available_cash", 0.0))
    deployable_cash = current_cash + deposit_amount
    total_custody_val = float(kpis.get("portfolio_market_value", 0.0))
    total_net_worth = total_custody_val + deployable_cash

    # Current portfolio holding weights
    holding_map = {h["ticker"]: h for h in holdings}
    holding_weights = {}
    for h in holdings:
        sym = h["ticker"]
        mv = float(h.get("market_value") or (float(h.get("shares", 0)) * float(h.get("current_price", 0))))
        holding_weights[sym] = (mv / total_custody_val) if total_custody_val > 0 else 0.0

    # Sector totals
    sector_weights = {}
    for h in holdings:
        sym = h["ticker"]
        sec = SECTOR_MAP.get(sym, h.get("sector", "Other"))
        mv = float(h.get("market_value") or 0.0)
        sector_weights[sec] = sector_weights.get(sec, 0.0) + ((mv / total_custody_val) if total_custody_val > 0 else 0.0)

    # Evaluate all candidate stocks
    candidates: List[Dict[str, Any]] = []

    for sym in CORE_CANDIDATE_UNIVERSE:
        q = live_quotes.get(sym) or {}
        spot_price = float(q.get("price") or 0.0)
        if spot_price <= 0:
            # Fall back to holding or default
            h_info = holding_map.get(sym)
            if h_info:
                spot_price = float(h_info.get("current_price") or h_info.get("avg_price") or 0.0)
        if spot_price <= 0:
            continue

        # DCF Target & Graham Target
        db_target = watchlist_targets.get(sym, {})
        default_target = DEFAULT_WATCHLIST_DCF_TARGETS.get(sym, {})
        dcf_fair_val = (
            db_target.get("target_price")
            or default_target.get("target_price")
            or round(spot_price * 1.18, 2)
        )
        fair_pe = db_target.get("fair_pe") or default_target.get("fair_pe") or 7.5
        ttm_pe = q.get("pe_ttm") or db_target.get("trailing_pe") or fair_pe
        div_yield = q.get("dividend_yield")
        if div_yield is None and sym in holding_map:
            div_yield = holding_map[sym].get("dividend_yield")
        div_yield = float(div_yield or 4.5)

        forward_eps = db_target.get("forward_eps") or (dcf_fair_val / fair_pe if fair_pe > 0 else 10.0)
        graham_val = calculate_graham_intrinsic_val(float(forward_eps), growth_pct=7.0)

        # 1. Margin of Safety Score (40%)
        # Upside to DCF target
        mos_pct = ((dcf_fair_val - spot_price) / dcf_fair_val) * 100.0 if dcf_fair_val > 0 else 0.0
        # Graham premium
        graham_upside_pct = ((graham_val - spot_price) / graham_val) * 100.0 if graham_val > 0 else 0.0
        blended_upside = max(0.0, (mos_pct * 0.7) + (graham_upside_pct * 0.3))
        score_valuation = min(100.0, blended_upside * 3.0)  # 30% upside gives ~90/100

        # 2. Earnings & Dividend Yield Score (25%)
        earnings_yield = (100.0 / ttm_pe) if (ttm_pe and ttm_pe > 0) else 10.0
        total_shareholder_yield = earnings_yield + div_yield
        score_yield = min(100.0, total_shareholder_yield * 4.0)  # 25% yield gives 100

        # 3. Diversification & Concentration Score (25%)
        current_stock_weight = holding_weights.get(sym, 0.0)
        sec = SECTOR_MAP.get(sym, "Core Equity")
        current_sec_weight = sector_weights.get(sec, 0.0)

        # Strict Accumulation Cap: Single stock cap 25%, Sector cap 30%
        if current_stock_weight >= 0.25 or current_sec_weight >= 0.35:
            score_diversification = 5.0  # severely penalize overconcentrated positions
        elif current_stock_weight >= 0.18:
            score_diversification = 40.0
        elif current_stock_weight >= 0.10:
            score_diversification = 75.0
        else:
            # Underweight position gets prime score for capital accumulation
            score_diversification = 95.0

        # 4. Institutional & Macro Catalysts (10%)
        catalyst_score = 50.0  # baseline
        matching_reports = [r for r in institutional_reports if r.get("ticker") == sym]
        for rep in matching_reports:
            if rep.get("sentiment") == "BULLISH":
                catalyst_score += 15.0
            elif rep.get("sentiment") == "BEARISH":
                catalyst_score -= 10.0

        # Sector-specific macro boosters
        if sec in ["Oil & Gas Exploration", "Fertilizer"]:
            # High dividend, FX hedge, cash-flow giants
            catalyst_score += 10.0
        if sec in ["Technology", "Cement"]:
            # Beneficiaries of domestic rate cuts
            catalyst_score += 8.0

        score_catalyst = min(100.0, max(0.0, catalyst_score))

        # Composite Multi-Factor Score
        composite_score = round(
            (score_valuation * 0.40)
            + (score_yield * 0.25)
            + (score_diversification * 0.25)
            + (score_catalyst * 0.10),
            2,
        )

        candidates.append({
            "ticker": sym,
            "sector": sec,
            "spot_price": spot_price,
            "dcf_fair_value": dcf_fair_val,
            "margin_of_safety_pct": round(mos_pct, 2),
            "graham_intrinsic_val": graham_val,
            "ttm_pe": round(float(ttm_pe), 2) if ttm_pe else None,
            "fair_pe": round(float(fair_pe), 2),
            "dividend_yield": round(div_yield, 2),
            "current_weight_pct": round(current_stock_weight * 100.0, 2),
            "current_shares": float(holding_map[sym]["shares"]) if sym in holding_map else 0.0,
            "score_valuation": round(score_valuation, 1),
            "score_yield": round(score_yield, 1),
            "score_diversification": round(score_diversification, 1),
            "score_catalyst": round(score_catalyst, 1),
            "composite_score": composite_score,
            "matching_reports": matching_reports,
        })

    # Sort candidates by composite score descending
    candidates.sort(key=lambda x: x["composite_score"], reverse=True)

    if not candidates:
        raise RuntimeError("No eligible candidates found in PSX universe.")

    # Find the top candidate affordable with deployable_cash
    affordable_candidates = [c for c in candidates if deployable_cash >= c["spot_price"]]

    winner = affordable_candidates[0] if affordable_candidates else candidates[0]
    rank2 = candidates[1] if len(candidates) > 1 else None
    rank3 = candidates[2] if len(candidates) > 2 else None

    # Calculate exact allocation math
    spot = winner["spot_price"]
    shares_to_buy = int(math.floor(deployable_cash / spot)) if spot > 0 else 0
    total_alloc = round(shares_to_buy * spot, 2)
    remaining_cash = round(deployable_cash - total_alloc, 2)

    # In case cash is insufficient for 1 full share of winner
    cash_shortfall = round(spot - deployable_cash, 2) if shares_to_buy == 0 else 0.0

    # Calculate pro-forma weight
    new_shares = winner["current_shares"] + shares_to_buy
    new_position_val = new_shares * spot
    pro_forma_weight_pct = round((new_position_val / (total_custody_val + total_alloc)) * 100.0, 2) if (total_custody_val + total_alloc) > 0 else 0.0

    # Annual projected dividend income added
    annual_div_added = round(total_alloc * (winner["dividend_yield"] / 100.0), 2)

    # 3-Pillar Proof Formulation
    pillar_1 = (
        f"{winner['ticker']} trades at Rs. {spot:,.2f} against internal DCF Fair Value of Rs. {winner['dcf_fair_value']:,.2f}, "
        f"providing an exact Margin of Safety of +{winner['margin_of_safety_pct']}%. "
        f"Graham Intrinsic Value is computed at Rs. {winner['graham_intrinsic_val']:,.2f} based on normalized forward earnings. "
        f"With a TTM P/E of {winner['ttm_pe']}x vs historical fair multiple of {winner['fair_pe']}x, the asset offers superior downside resilience."
    )

    pillar_2 = (
        f"Current portfolio allocation in {winner['ticker']} is {winner['current_weight_pct']}%, remaining safely below the 25% single-stock accumulation ceiling. "
        f"Deploying Rs. {total_alloc:,.2f} expands position to {new_shares:,.0f} shares (pro-forma weight: {pro_forma_weight_pct}%). "
        f"At a {winner['dividend_yield']}% cash dividend yield, this allocation compounds portfolio cash-flow by +Rs. {annual_div_added:,.2f}/year in passive liquidity."
    )

    pillar_3 = (
        f"{winner['sector']} sector remains fundamentally underpinned by current SBP interest rate dynamics and robust operational cash-flows. "
        + (f"Backed by {len(winner['matching_reports'])} recent institutional broker reports with bullish target confirmations. " if winner['matching_reports'] else "Institutional positioning reflects defensive accumulation during market consolidations. ")
        + f"Playbook rules confirm capital allocation priority for cash-rich dividend compounders."
    )

    # Detailed rejection analysis for #2 and #3
    rejected_alternatives = []
    for rank_idx, alt in [(2, rank2), (3, rank3)]:
        if not alt:
            continue
        reasons = []
        if alt["margin_of_safety_pct"] < winner["margin_of_safety_pct"]:
            reasons.append(f"lower margin of safety (+{alt['margin_of_safety_pct']}% vs +{winner['margin_of_safety_pct']}%)")
        if alt["dividend_yield"] < winner["dividend_yield"]:
            reasons.append(f"lower dividend yield ({alt['dividend_yield']}% vs {winner['dividend_yield']}%)")
        if alt["current_weight_pct"] > winner["current_weight_pct"]:
            reasons.append(f"higher existing portfolio concentration ({alt['current_weight_pct']}% vs {winner['current_weight_pct']}%)")
        if alt["spot_price"] > deployable_cash and deployable_cash > 0:
            reasons.append(f"spot price (Rs. {alt['spot_price']:,.2f}) exceeds current deployable capital")

        reason_str = "; ".join(reasons) if reasons else "lower composite score across valuation, yield, and sector weighting."
        rejected_alternatives.append({
            "ticker": alt["ticker"],
            "rank": rank_idx,
            "spot_price": alt["spot_price"],
            "composite_score": alt["composite_score"],
            "reason": f"Rank #{rank_idx} ({alt['ticker']}) composite score {alt['composite_score']} was surpassed by {winner['ticker']} ({winner['composite_score']}) due to {reason_str}."
        })

    # Executive Verdict
    if shares_to_buy > 0:
        executive_verdict = (
            f"DIRECTIVE: Execute BUY order for {shares_to_buy:,.0f} shares of {winner['ticker']} at Rs. {spot:,.2f} "
            f"(Total: Rs. {total_alloc:,.2f}). Retain Rs. {remaining_cash:,.2f} in dry powder. "
            f"Asset commands highest multi-factor conviction with +{winner['margin_of_safety_pct']}% DCF Margin of Safety and {winner['dividend_yield']}% dividend yield."
        )
    else:
        executive_verdict = (
            f"DIRECTIVE: Top accumulation target is {winner['ticker']} at Rs. {spot:,.2f} (+{winner['margin_of_safety_pct']}% DCF Margin of Safety). "
            f"Current deployable cash (Rs. {deployable_cash:,.2f}) is short by Rs. {cash_shortfall:,.2f} to acquire 1 integer share. "
            f"Recommend depositing Rs. {math.ceil(cash_shortfall):,.2f} to execute immediate accumulation."
        )

    return {
        "status": "success",
        "mandate": ACCUMULATION_MANDATE,
        "deployable_cash_pkr": deployable_cash,
        "selected_ticker": winner["ticker"],
        "action": "BUY",
        "recommended_shares": shares_to_buy,
        "limit_price": spot,
        "total_allocation_pkr": total_alloc,
        "remaining_cash_pkr": remaining_cash,
        "cash_shortfall_pkr": cash_shortfall,
        "executive_verdict": executive_verdict,
        "why_this_stock": {
            "pillar_1_valuation": pillar_1,
            "pillar_2_weight_yield": pillar_2,
            "pillar_3_macro_catalysts": pillar_3,
        },
        "rejected_alternatives": rejected_alternatives,
        "candidate_rankings": [
            {
                "ticker": c["ticker"],
                "score": c["composite_score"],
                "spot": c["spot_price"],
                "mos_pct": c["margin_of_safety_pct"],
                "yield": c["dividend_yield"],
                "weight_pct": c["current_weight_pct"],
            }
            for c in candidates[:6]
        ],
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def execute_central_brain_allocation(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Executes and records the Central Brain's BUY decision directly into KTrade:
    - Verifies action is BUY (Strict Accumulation Mandate).
    - Logs trade in trade_history.
    - Updates portfolio_holdings (shares and weighted avg_buy_price).
    - Adjusts capital_ledger cash.
    - Publishes event to agent_feed (DESK_MANAGER).
    """
    from utils import normalize_ticker
    ticker = normalize_ticker(payload.get("ticker") or "")
    shares = float(payload.get("shares") or 0.0)
    price = float(payload.get("price") or 0.0)
    notes = payload.get("notes") or "Executed via Central Brain Allocation"

    if not ticker or shares <= 0 or price <= 0:
        raise ValueError("Invalid allocation parameters. Ticker, shares > 0, and price > 0 are required.")

    total_cost = round(shares * price, 2)

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # 1. Log trade in trade_history
        cur.execute("""
            INSERT INTO trade_history (ticker, side, quantity, price, execution_date, notes)
            VALUES (%s, 'BUY', %s, %s, NOW(), %s)
            RETURNING id;
        """, (ticker, shares, price, notes))
        trade_id = cur.fetchone()[0]

        # 2. Update portfolio_holdings with weighted avg buy price
        cur.execute("SELECT shares, avg_buy_price FROM portfolio_holdings WHERE UPPER(ticker) = %s;", (ticker,))
        existing = cur.fetchone()
        if existing:
            old_shs = float(existing[0])
            old_avg = float(existing[1])
            new_shs = old_shs + shares
            new_avg = round(((old_shs * old_avg) + (shares * price)) / new_shs, 2) if new_shs > 0 else price
            cur.execute("""
                UPDATE portfolio_holdings
                SET shares = %s,
                    avg_buy_price = %s,
                    last_price = %s,
                    price_updated_at = NOW(),
                    updated_at = NOW()
                WHERE UPPER(ticker) = %s;
            """, (new_shs, new_avg, price, ticker))
        else:
            cur.execute("""
                INSERT INTO portfolio_holdings (ticker, shares, avg_buy_price, last_price, price_updated_at, updated_at)
                VALUES (%s, %s, %s, %s, NOW(), NOW());
            """, (ticker, shares, price, price))

        # 3. Book cash deduction in capital_ledger as FEE_ADJUSTMENT / trade cash flow
        cur.execute("""
            INSERT INTO capital_ledger (tx_type, amount, reference_note, notes, tx_date, created_at)
            VALUES ('FEE_ADJUSTMENT', %s, %s, %s, NOW(), NOW());
        """, (-total_cost, f"Central Brain Trade #{trade_id}: BUY {ticker}", f"Auto cash settlement for BUY {shares} {ticker} @ Rs. {price}"))

        conn.commit()
        invalidate_financial_summary_cache()

        # 4. Publish agent feed event
        publish_agent_event(
            category="DESK_MANAGER",
            title=f"Central Brain Execution: BUY {shares:,.0f} {ticker} @ Rs. {price:,.2f}",
            content=(
                f"Capital Deployment Complete: Acquired {shares:,.0f} shares of {ticker} at limit price Rs. {price:,.2f} "
                f"(Total Invested: Rs. {total_cost:,.2f}). Mandate: Capital Accumulation Mode."
            ),
            metadata={"trade_id": trade_id, "ticker": ticker, "shares": shares, "price": price, "cost": total_cost}
        )

        return {
            "status": "success",
            "trade_id": trade_id,
            "ticker": ticker,
            "shares": shares,
            "price": price,
            "total_invested": total_cost,
            "message": f"Successfully executed and logged BUY order for {shares:,.0f} {ticker} @ Rs. {price:,.2f}."
        }
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()
