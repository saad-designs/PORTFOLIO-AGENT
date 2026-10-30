import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

from market_data_scraper import fetch_live_price, fetch_latest_eps

# Load environment variables
load_dotenv()


def get_db_connection():
    """Creates and returns a connection to the Supabase database via portfolio_manager."""
    from portfolio_manager import get_db_connection as pm_get_db_connection
    return pm_get_db_connection()


def calculate_graham_intrinsic(eps: float, raw_g: float) -> float:
    """
    Calculates the Graham Intrinsic Value using a sanitized growth rate.
    Intrinsic Value = EPS * (8.5 + 2g)
    """
    if eps <= 0:
        return 0.0  # Money-losing companies have zero intrinsic value in this model

    # Sanitize g: Floor at 0%, Cap at 15% (0.15)
    g_percentage = raw_g * 100 if raw_g < 1.0 else raw_g
    safe_g = max(0.0, min(g_percentage, 15.0))
    intrinsic_value = eps * (8.5 + (2 * safe_g))
    return round(intrinsic_value, 2)


def sync_eps_and_targets():
    """
    Fetches the latest EPS from PSX and updates the Supabase watchlist_targets table.
    Applies deterministic math to recalculate the target price.
    """
    print("Fetching latest EPS data from PSX...")
    eps_data = fetch_latest_eps()

    if not eps_data:
        print("No EPS data fetched. Exiting sync.")
        return

    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT ticker, historical_fair_pe FROM watchlist_targets")
            targets = cursor.fetchall()

            updated_count = 0
            for target in targets:
                ticker = target["ticker"]
                historical_fair_pe = target["historical_fair_pe"]

                if historical_fair_pe is None:
                    continue

                new_eps = eps_data.get(ticker)
                if new_eps is None:
                    for company_name, eps in eps_data.items():
                        if ticker.lower() in company_name.lower().split() or ticker.lower() == company_name.lower()[:len(ticker)]:
                            new_eps = eps
                            break

                if new_eps is not None:
                    new_target = float(new_eps) * float(historical_fair_pe)
                    cursor.execute("""
                        UPDATE watchlist_targets
                        SET forward_eps = %s, target_price = %s, last_recalculated_date = CURRENT_DATE
                        WHERE ticker = %s
                    """, (new_eps, new_target, ticker))
                    print(f"✅ Updated {ticker}: Forward EPS = {new_eps}, Target Price = {new_target:.2f}")
                    updated_count += 1

        conn.commit()
        print(f"Sync complete. Updated {updated_count} tickers.")
    except Exception as e:
        print(f"🚨 Error syncing EPS and targets: {e}")
        conn.rollback()
    finally:
        conn.close()


def run_price_sentinel() -> str:
    """
    Evaluates active portfolio against concentration risk and stop-loss thresholds
    using portfolio_manager.get_complete_financial_summary() as the single source of truth.
    """
    from portfolio_manager import get_complete_financial_summary

    summary_alerts = []
    try:
        summary = get_complete_financial_summary(use_cached_prices=False)
        kpis = summary.get("financial_kpis", {})
        holdings = summary.get("holdings", [])
        liquid_cash = float(kpis.get("ktrade_available_cash", 0.0))
        portfolio_value = float(kpis.get("portfolio_market_value", 0.0))
        total_equity = liquid_cash + portfolio_value

        if not holdings:
            return "ℹ️ Sentinel Check: No active portfolio holdings found."

        conn = get_db_connection()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("SELECT ticker, stop_loss FROM watchlist_targets")
                stop_losses = {
                    str(row["ticker"]).upper(): float(row["stop_loss"])
                    for row in cursor.fetchall()
                    if row.get("stop_loss") is not None
                }
        finally:
            conn.close()

        for holding in holdings:
            ticker = str(holding["ticker"]).upper()
            shares = float(holding["shares"])
            price = float(holding.get("current_price") or 0.0)
            if price <= 0 or shares <= 0:
                continue

            position_val = shares * price
            stop_loss = stop_losses.get(ticker) or holding.get("stop_loss")
            if stop_loss and float(stop_loss) > 0 and price <= float(stop_loss):
                summary_alerts.append(
                    f"🚨 STOP-LOSS ALERT: {ticker} dropped to Rs.{price:,.2f} (Threshold: Rs.{float(stop_loss):,.2f})"
                )

            if total_equity > 0:
                concentration_ratio = position_val / total_equity
                if concentration_ratio >= 0.40:
                    pct = concentration_ratio * 100
                    summary_alerts.append(
                        f"⚠️ CONCENTRATION RISK: {ticker} represents {pct:.1f}% of total portfolio (Value: Rs.{position_val:,.2f})"
                    )
    except Exception as e:
        return f"🚨 Error running price sentinel: {e}"

    if not summary_alerts:
        return f"✅ Sentinel Check Complete. All {len(holdings)} positions are within safe thresholds (Total Equity: Rs.{total_equity:,.2f})."
    header = f"🚨 SENTINEL ALERTS (Total Equity: Rs.{total_equity:,.2f}) 🚨\n"
    return header + "\n".join(summary_alerts)


def execute_buy_fifo(user_id: str, ticker: str, buy_quantity: float, buy_price: float) -> dict:
    """
    Unified BUY execution routed through portfolio_manager (capital_ledger + portfolio_holdings + trade_history).
    Retains signature for backward compatibility without relying on legacy cash_ledger or discord_user_id.
    """
    from portfolio_manager import get_complete_financial_summary, record_trade_execution

    qty = float(buy_quantity)
    prc = float(buy_price)
    trade_value = round(qty * prc, 2)

    summary = get_complete_financial_summary(use_cached_prices=True)
    liquid_cash = float(summary.get("financial_kpis", {}).get("ktrade_available_cash", 0.0))

    if liquid_cash < trade_value:
        return {
            "status": "error",
            "message": f"❌ Insufficient cash to buy {qty:g} shares of {ticker}. Needed: Rs.{trade_value:,.2f}, Available: Rs.{liquid_cash:,.2f}."
        }

    trade_res = record_trade_execution(
        ticker=ticker,
        side="BUY",
        quantity=qty,
        price=prc,
        fees=0.0,
        notes="FIFO Buy Execution"
    )
    return {
        "status": "success",
        "message": f"✅ Ledger Updated: BOUGHT {qty:g} shares of {trade_res['ticker']} @ Rs.{prc:,.2f}. Deducted Rs.{trade_value:,.2f}",
        "trade": trade_res
    }


def execute_sell_fifo(user_id: str, ticker: str, sell_quantity: float, sell_price: float) -> dict:
    """
    Unified SELL execution routed through portfolio_manager (capital_ledger + portfolio_holdings + trade_history).
    """
    from portfolio_manager import get_complete_financial_summary, record_trade_execution

    clean_ticker = str(ticker).strip().upper()
    qty = float(sell_quantity)
    prc = float(sell_price)

    summary = get_complete_financial_summary(use_cached_prices=True)
    holdings_map = {str(h["ticker"]).upper(): h for h in summary.get("holdings", [])}
    held = holdings_map.get(clean_ticker)

    if not held or float(held["shares"]) < qty:
        avail = float(held["shares"]) if held else 0.0
        return {
            "status": "error",
            "message": f"❌ Insufficient shares for {clean_ticker}. Requested {qty:g}, held {avail:g}."
        }

    avg_buy = float(held["avg_price"])
    gross_pnl = round((prc - avg_buy) * qty, 2)
    cgt_deducted = round(gross_pnl * 0.15, 2) if gross_pnl > 0 else 0.0
    net_pnl = round(gross_pnl - cgt_deducted, 2)

    trade_res = record_trade_execution(
        ticker=clean_ticker,
        side="SELL",
        quantity=qty,
        price=prc,
        fees=cgt_deducted,
        notes=f"FIFO Sell Execution (CGT: Rs.{cgt_deducted:,.2f})"
    )
    return {
        "status": "success",
        "message": (
            f"✅ Ledger Updated: SOLD {qty:g} shares of {clean_ticker} @ Rs.{prc:,.2f}. "
            f"Gross PnL: Rs.{gross_pnl:,.2f} | CGT: Rs.{cgt_deducted:,.2f} | Net Proceeds: Rs.{trade_res['net_total']:,.2f}"
        ),
        "gross_pnl": gross_pnl,
        "cgt_deducted": cgt_deducted,
        "net_pnl": net_pnl
    }


def get_deterministic_portfolio_summary(use_cached_prices: bool = False) -> dict:
    """
    Reads liquid cash and holdings strictly from portfolio_manager.get_complete_financial_summary()
    (single source of truth across capital_ledger & portfolio_holdings).
    """
    from portfolio_manager import get_complete_financial_summary

    try:
        summary = get_complete_financial_summary(use_cached_prices=use_cached_prices)
    except Exception as e:
        print(f"Error fetching deterministic portfolio: {e}")
        summary = {"financial_kpis": {}, "holdings": []}

    kpis = summary.get("financial_kpis", {})
    holdings = summary.get("holdings", [])

    liquid_cash = float(kpis.get("ktrade_available_cash", 0.0))
    total_invested = float(kpis.get("portfolio_cost_basis", 0.0))
    total_current_value = float(kpis.get("portfolio_market_value", 0.0))
    net_worth = float(kpis.get("total_net_worth", total_current_value + liquid_cash))
    total_pnl = float(kpis.get("unrealized_pnl", total_current_value - total_invested))
    total_pnl_pct = float(kpis.get("unrealized_pnl_pct", 0.0))

    position_summaries = []
    for pos in holdings:
        ticker = pos["ticker"]
        shares = float(pos["shares"])
        avg_buy = float(pos["avg_price"])
        live_price = float(pos["current_price"])
        pos_pnl_pct = float(pos["unrealized_pnl_pct"])
        position_summaries.append(
            f"- {ticker}: {shares:g} shares @ Avg Rs {avg_buy:.2f} (Spot: Rs {live_price:.2f} | PnL: {pos_pnl_pct:+.2f}%)"
        )

    pnl_term = "GAIN" if total_pnl >= 0 else "LOSS"
    formatted_summary = (
        f"LIQUID CASH: Rs {liquid_cash:,.2f}\n"
        f"TOTAL INVESTED: Rs {total_invested:,.2f}\n"
        f"PORTFOLIO MARKET VALUE: Rs {total_current_value:,.2f}\n"
        f"TOTAL NET WORTH: Rs {net_worth:,.2f}\n"
        f"TOTAL UNREALIZED {pnl_term}: Rs {abs(total_pnl):,.2f} ({total_pnl_pct:+.2f}%)\n\n"
        f"ACTIVE POSITIONS:\n" + ("\n".join(position_summaries) if position_summaries else "None (100% Cash)")
    )

    return {
        "liquid_cash": liquid_cash,
        "total_invested": total_invested,
        "total_current_value": total_current_value,
        "net_worth": net_worth,
        "total_pnl": total_pnl,
        "total_pnl_pct": total_pnl_pct,
        "formatted_text": formatted_summary
    }


if __name__ == "__main__":
    print(run_price_sentinel())
    print(get_deterministic_portfolio_summary()["formatted_text"])
