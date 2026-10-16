import numpy as np

def calculate_kelly_position_size(
    entry_price: float,
    target_price: float,
    stop_loss: float,
    total_portfolio_value: float,
    liquid_cash: float,
    win_rate: float = 0.55,  # Conservative default until paper ledger builds empirical history
    max_position_cap_pct: float = 0.15,  # Max 15% of total portfolio in one equity
    kelly_fraction: float = 0.25  # Quarter-Kelly
) -> dict:
    """
    Calculates quantitative position sizing using Fractional Kelly Criterion
    gated by institutional risk caps and live cash availability.
    """
    if entry_price <= 0 or target_price <= entry_price or stop_loss >= entry_price:
        return {
            "recommended_shares": 0,
            "allocated_capital": 0.0,
            "portfolio_weight_pct": 0.0,
            "fractional_kelly": 0.0,
            "reason": "Invalid price boundaries for risk calculation."
        }

    # 1. Calculate Payoff Ratio (b)
    upside = (target_price - entry_price) / entry_price
    downside = (entry_price - stop_loss) / entry_price
    
    if downside <= 0:
        return {"recommended_shares": 0, "allocated_capital": 0.0, "portfolio_weight_pct": 0.0, "reason": "Invalid downside."}
        
    b = upside / downside
    q = 1.0 - win_rate

    # 2. Raw Kelly Percentage (f*)
    raw_kelly = (win_rate * b - q) / b
    
    if raw_kelly <= 0:
        return {
            "recommended_shares": 0,
            "allocated_capital": 0.0,
            "portfolio_weight_pct": 0.0,
            "fractional_kelly": 0.0,
            "reason": "Negative edge under current payoff and win rate."
        }

    # 3. Fractional Kelly Sizing
    fractional_kelly = raw_kelly * kelly_fraction

    # 4. Enforce Hard Maximum Single-Position Cap
    capped_kelly = min(fractional_kelly, max_position_cap_pct)

    # 5. Theoretical Capital Allocation vs Real Buying Power
    ideal_capital = total_portfolio_value * capped_kelly
    executable_capital = min(ideal_capital, liquid_cash)

    # 6. Share Calculation (Floored to integer)
    recommended_shares = int(executable_capital // entry_price)
    actual_allocation = recommended_shares * entry_price
    portfolio_weight_pct = (actual_allocation / total_portfolio_value) * 100 if total_portfolio_value > 0 else 0.0

    return {
        "recommended_shares": recommended_shares,
        "allocated_capital": round(actual_allocation, 2),
        "portfolio_weight_pct": round(portfolio_weight_pct, 2),
        "fractional_kelly_pct": round(fractional_kelly * 100, 2),
        "payoff_ratio": round(b, 2),
        "executable": recommended_shares > 0
    }

# Standard KMI-30 Sector Classifications
SECTOR_MAP = {
    # Tech
    "SYS": "Technology", "NETSOL": "Technology", "AVN": "Technology", "TRG": "Technology",
    # Fertilizer
    "EFERT": "Fertilizer", "FFC": "Fertilizer", "FATIMA": "Fertilizer",
    # Energy / E&P / Power
    "HUBC": "Power & Energy", "OGDC": "Oil & Gas", "PPL": "Oil & Gas", "MARI": "Oil & Gas", "PSO": "Oil & Gas",
    # Banking
    "MEBL": "Commercial Banks", "BIPL": "Commercial Banks",
    # Cement
    "LUCK": "Cement", "DGKC": "Cement", "MLCF": "Cement", "CHCC": "Cement",
    # Chemicals / Auto / Others
    "EPCL": "Chemicals", "ENGRO": "Conglomerate", "INDU": "Automobile"
}

MAX_SECTOR_ALLOCATION = 0.30  # 30% Hard Ceiling

def check_sector_concentration(ticker: str, proposed_allocation_pkr: float, total_portfolio_value: float, current_holdings: list[dict]) -> tuple[bool, str]:
    """
    Blocks trades that push an individual sector over the 30% concentration limit.
    """
    ticker_sector = SECTOR_MAP.get(ticker, "Unknown")
    if ticker_sector == "Unknown":
        return True, "Unknown sector; bypassing sector cap."

    # Calculate current capital deployed in this sector
    current_sector_exposure = 0.0
    for pos in current_holdings:
        pos_ticker = pos.get("ticker")
        pos_sector = SECTOR_MAP.get(pos_ticker, "Unknown")
        if pos_sector == ticker_sector:
            current_sector_exposure += pos.get("market_value", 0.0)

    # Calculate post-trade sector weight
    projected_sector_exposure = current_sector_exposure + proposed_allocation_pkr
    projected_sector_weight = projected_sector_exposure / total_portfolio_value

    if projected_sector_weight > MAX_SECTOR_ALLOCATION:
        max_allowed_pkr = (total_portfolio_value * MAX_SECTOR_ALLOCATION) - current_sector_exposure
        reason = (
            f"🚫 Sector Risk Breach: {ticker_sector} is already at "
            f"{round((current_sector_exposure / total_portfolio_value) * 100, 1)}% of the book. "
            f"Adding Rs. {proposed_allocation_pkr:,.2f} would push it to {round(projected_sector_weight * 100, 1)}% "
            f"(Cap: {int(MAX_SECTOR_ALLOCATION * 100)}%). Max allowed: Rs. {max(0.0, max_allowed_pkr):,.2f}."
        )
        return False, reason

    return True, f"Sector exposure safe ({round(projected_sector_weight * 100, 1)}% / 30% max)."

# Updated Asset Allocation Tiers
PORTFOLIO_TIERS = {
    "TIER_1_BLUECHIP": {"soft_cap": 0.25, "hard_cap": 0.35},  # SYS, MEBL, HUBC, FFC
    "TIER_2_CYCLICAL": {"soft_cap": 0.15, "hard_cap": 0.20},  # LUCK, DGKC, ATRL
    "TIER_3_HIGH_BETA": {"soft_cap": 0.05, "hard_cap": 0.10}  # Speculative / Growth
}

def evaluate_holding_risk(ticker: str, current_weight: float, pnl_pct: float, tier: str = "TIER_1_BLUECHIP"):
    caps = PORTFOLIO_TIERS.get(tier, PORTFOLIO_TIERS["TIER_2_CYCLICAL"])
    
    # 1. Below Soft Cap -> Completely Safe
    if current_weight <= caps["soft_cap"]:
        return {"status": "HEALTHY", "action": "HOLD"}
        
    # 2. Between Soft and Hard Cap -> Overweight, but don't panic
    if caps["soft_cap"] < current_weight <= caps["hard_cap"]:
        return {
            "status": "ELEVATED",
            "action": "DILUTE_WITH_NEW_CASH",
            "msg": f"{ticker} is at {current_weight*100:.1f}%. Direct new deposits to other sectors."
        }
        
    # 3. Above Hard Cap (>35%) -> True Concentration Risk
    if current_weight > caps["hard_cap"]:
        if pnl_pct < -0.05:
            # Down in the trade: Wait for mean-reversion rally rather than dumping at support
            return {
                "status": "CRITICAL_OVERWEIGHT",
                "action": "SET_LIMIT_TRIM_ON_BOUNCE",
                "target_weight": caps["soft_cap"],
                "msg": f"{ticker} is {current_weight*100:.1f}% of book. Do not dump at lows; trim on bounce."
            }
        else:
            # Green or flat: Trim immediately to soft cap
            return {
                "status": "CRITICAL_OVERWEIGHT",
                "action": "TRIM_IMMEDIATE",
                "target_weight": caps["soft_cap"]
            }

import pandas as pd
import numpy as np

def evaluate_exit_and_exhaustion(
    ticker: str, 
    current_price: float, 
    entry_price: float, 
    target_price: float, 
    fair_value: float,
    ohlc_df: pd.DataFrame,
    latest_research_sentiment: str = "NEUTRAL"
) -> dict:
    """
    Evaluates forward-looking indicators to detect downside breakdowns or 
    valuation/momentum exhaustion peaks.
    """
    if ohlc_df is None or len(ohlc_df) < 30:
        return {"action": "HOLD", "reason": "Insufficient historical data for exit analysis"}

    # 1. Technical Indicators Calculation
    closes = ohlc_df['Close']
    sma_20 = closes.rolling(window=20).mean().iloc[-1]
    sma_50 = closes.rolling(window=50).mean().iloc[-1] if len(closes) >= 50 else sma_20
    
    # Calculate RSI (14 periods)
    delta = closes.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / (loss + 1e-9)
    rsi_14 = 100 - (100 / (1 + rs)).iloc[-1]

    # --- TRACK 1: UPSIDE EXHAUSTION / MAXIMUM REACHED ---
    # Trigger A: Target Price or Intrinsic Fair Value Achieved
    if target_price > 0 and current_price >= target_price:
        return {
            "action": "TAKE_PROFIT_PEAK",
            "urgency": "HIGH",
            "reason": f"Target price of Rs. {target_price:,.2f} reached (Current: Rs. {current_price:,.2f}). Risk/reward is now exhausted.",
            "rsi": round(rsi_14, 2),
            "price": current_price
        }

    # Trigger B: Overbought Exhaustion (RSI > 75 and trading >15% above 20 SMA)
    if rsi_14 >= 75 and current_price > (sma_20 * 1.15):
        return {
            "action": "TAKE_PROFIT_OVERBOUGHT",
            "urgency": "MEDIUM",
            "reason": f"Asset is severely overbought (RSI: {rsi_14:.1f}) and overextended above 20 MA. High probability of mean-reversion pullback.",
            "rsi": round(rsi_14, 2),
            "price": current_price
        }

    # --- TRACK 2: DOWNSIDE ESCALATION / BREAKDOWN DETECTOR ---
    # Trigger A: Structural Support Collapse (Closed below 20 & 50 MA with negative RSI)
    structural_breakdown = (current_price < sma_20) and (current_price < sma_50) and (rsi_14 < 40)
    
    # Trigger B: Fundamental Deterioration flag from RAG/Research Sweep
    bearish_catalyst = latest_research_sentiment in ["BEARISH", "DOWNGRADE", "AVOID"]

    if structural_breakdown or bearish_catalyst:
        reason_details = []
        if structural_breakdown:
            reason_details.append("Severe price collapse triggering strict capital preservation rules")
        if bearish_catalyst:
            reason_details.append("Institutional/RAG research flagged negative catalyst / thesis invalidation")

        return {
            "action": "EXIT_DOWNSIDE_WARNING",
            "urgency": "HIGH" if (structural_breakdown and bearish_catalyst) else "MEDIUM",
            "reason": " | ".join(reason_details),
            "rsi": round(rsi_14, 2),
            "price": current_price
        }

    return {"action": "HOLD", "reason": "Trend healthy, holding within fair value channel."}
