import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

from market_data_scraper import fetch_live_price, fetch_latest_eps

# Load environment variables
load_dotenv()

def get_db_connection():
    """Creates and returns a connection to the Supabase database."""
    db_url = os.getenv("SUPABASE_DB_URL")
    if not db_url:
        raise ValueError("SUPABASE_DB_URL not found in environment variables.")
    return psycopg2.connect(db_url)

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
            # Query all active tickers from watchlist_targets
            cursor.execute("SELECT ticker, historical_fair_pe FROM watchlist_targets")
            targets = cursor.fetchall()

            updated_count = 0
            for target in targets:
                ticker = target['ticker']
                historical_fair_pe = target['historical_fair_pe']
                
                if historical_fair_pe is None:
                    continue
                    
                # Look for matching EPS in the scraped dictionary
                new_eps = eps_data.get(ticker)
                
                # Fallback: Scraper returns company names (e.g. 'Systems Limited' instead of 'SYS')
                # If exact ticker match fails, try to find a soft match
                if new_eps is None:
                    for company_name, eps in eps_data.items():
                        # Simple heuristic check (e.g., if 'SYS' is in the company name string)
                        # In production, a ticker-to-name mapping table is safer.
                        if ticker.lower() in company_name.lower().split() or ticker.lower() == company_name.lower()[:len(ticker)]:
                            new_eps = eps
                            break
                            
                if new_eps is not None:
                    new_target = float(new_eps) * float(historical_fair_pe)
                    
                    # Update the database
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

def run_price_sentinel():
    """
    Evaluates active portfolio against concentration risk and stop-loss thresholds using live prices.
    Returns a formatted text summary of triggered alerts or a clean status string.
    """
    conn = get_db_connection()
    summary_alerts = []
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            # 1. Query the single row in portfolio_cash to get the liquid buffer
            cursor.execute("SELECT liquid_cash FROM portfolio_cash WHERE id = 1")
            cash_row = cursor.fetchone()
            liquid_cash = float(cash_row['liquid_cash']) if cash_row else 0.0

            # 2. Query all tickers and shares from portfolio_holdings
            cursor.execute("SELECT ticker, shares FROM portfolio_holdings")
            holdings = cursor.fetchall()
            
            if not holdings:
                return "ℹ️ Sentinel Check: No active portfolio holdings found."
            
            # Query stop losses from watchlist_targets for cross-referencing
            cursor.execute("SELECT ticker, stop_loss FROM watchlist_targets")
            stop_losses = {row['ticker']: float(row['stop_loss']) for row in cursor.fetchall() if row['stop_loss'] is not None}

            # 3. Loop through holdings to fetch live prices and calculate equity
            portfolio_value = 0.0
            live_prices = {}
            
            for holding in holdings:
                ticker = holding['ticker']
                shares = float(holding['shares'])
                
                # Ping dps.psx.com.pk directly for live spot prices
                price = fetch_live_price(ticker)
                live_prices[ticker] = price
                
                position_val = shares * price
                portfolio_value += position_val
                
            # Calculate total equity
            total_equity = liquid_cash + portfolio_value
            
            # 4. Evaluate strict triggers
            for holding in holdings:
                ticker = holding['ticker']
                shares = float(holding['shares'])
                price = live_prices[ticker]
                
                if price <= 0:
                    continue # Skip evaluation if price fetching failed
                    
                position_val = shares * price
                
                # Check Stop-Loss
                stop_loss = stop_losses.get(ticker)
                if stop_loss and price <= stop_loss:
                    summary_alerts.append(f"🚨 STOP-LOSS ALERT: {ticker} dropped to Rs.{price} (Threshold: Rs.{stop_loss})")
                    
                # Check Concentration Risk (>= 40% of total equity)
                if total_equity > 0:
                    concentration_ratio = position_val / total_equity
                    if concentration_ratio >= 0.40:
                        pct = concentration_ratio * 100
                        summary_alerts.append(f"⚠️ CONCENTRATION RISK: {ticker} represents {pct:.1f}% of total portfolio (Value: Rs.{position_val:.2f})")
                        
    except Exception as e:
        return f"🚨 Error running price sentinel: {e}"
    finally:
        conn.close()

    # 5. Return formatted text summary
    if not summary_alerts:
        return f"✅ Sentinel Check Complete. All {len(holdings)} positions are within safe thresholds (Total Equity: Rs.{total_equity:.2f})."
    else:
        header = f"🚨 SENTINEL ALERTS (Total Equity: Rs.{total_equity:.2f}) 🚨\n"
        return header + "\n".join(summary_alerts)


import psycopg2.pool
try:
    DB_POOL = psycopg2.pool.ThreadedConnectionPool(1, 20, os.getenv("SUPABASE_DB_URL"))
except Exception as e:
    DB_POOL = None

def execute_buy_fifo(user_id: str, ticker: str, buy_quantity: float, buy_price: float):
    conn = DB_POOL.getconn() if DB_POOL else get_db_connection()
    try:
        with conn.cursor() as cur:
            trade_value = float(buy_quantity) * float(buy_price)
            
            # Check liquid cash from cash_ledger
            cur.execute("SELECT COALESCE(SUM(amount), 0) FROM cash_ledger WHERE discord_user_id = %s", (user_id,))
            cash_row = cur.fetchone()
            liquid_cash = float(cash_row[0]) if cash_row else 0.0
            
            if liquid_cash < trade_value:
                return {"status": "error", "message": f"❌ Insufficient cash to buy {buy_quantity} shares of {ticker}. Needed: Rs.{trade_value:,.2f}, Available: Rs.{liquid_cash:,.2f}."}
            
            # Insert into tax_lots
            cur.execute("""
                INSERT INTO tax_lots (discord_user_id, ticker, buy_price, initial_quantity, remaining_quantity)
                VALUES (%s, %s, %s, %s, %s)
            """, (user_id, ticker, buy_price, buy_quantity, buy_quantity))
            
            # Update master holdings
            cur.execute("SELECT shares, avg_buy_price FROM portfolio_holdings WHERE discord_user_id = %s AND ticker = %s", (user_id, ticker))
            holding = cur.fetchone()
            
            if holding:
                old_shares = float(holding[0])
                old_avg = float(holding[1])
                total_shares = old_shares + float(buy_quantity)
                new_avg_price = ((old_shares * old_avg) + (float(buy_quantity) * float(buy_price))) / total_shares if total_shares > 0 else float(buy_price)
                
                cur.execute("""
                    UPDATE portfolio_holdings 
                    SET shares = %s, avg_buy_price = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE discord_user_id = %s AND ticker = %s
                """, (total_shares, new_avg_price, user_id, ticker))
            else:
                cur.execute("""
                    INSERT INTO portfolio_holdings (discord_user_id, ticker, shares, avg_buy_price) 
                    VALUES (%s, %s, %s, %s)
                """, (user_id, ticker, buy_quantity, buy_price))
                
            # Deduct from cash
            cur.execute("""
                INSERT INTO cash_ledger (discord_user_id, transaction_type, amount, ticker)
                VALUES (%s, 'BUY', %s, %s)
            """, (user_id, -trade_value, ticker))
            
            conn.commit()
            return {"status": "success", "message": f"✅ Ledger Updated: BOUGHT {buy_quantity} shares of {ticker} @ Rs.{buy_price:,.2f}. Deducted Rs.{trade_value:,.2f}"}
            
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        if DB_POOL: DB_POOL.putconn(conn)
        else: conn.close()

def execute_sell_fifo(user_id: str, ticker: str, sell_quantity: float, sell_price: float):
    conn = DB_POOL.getconn() if DB_POOL else get_db_connection()
    try:
        with conn.cursor() as cur:
            # 1. Fetch OPEN lots ordered by oldest first (FIFO)
            cur.execute("""
                SELECT lot_id, buy_price, remaining_quantity 
                FROM tax_lots 
                WHERE discord_user_id = %s AND ticker = %s AND status = 'OPEN'
                ORDER BY buy_date ASC
            """, (user_id, ticker))
            
            open_lots = cur.fetchall()
            if not open_lots:
                return {"status": "error", "message": f"❌ No open tax lots found for {ticker}."}

            qty_to_sell = float(sell_quantity)
            gross_pnl = 0.0
            
            # 2. Iterate and consume the lots
            for lot in open_lots:
                if qty_to_sell <= 0:
                    break
                    
                lot_id = lot[0]
                buy_price = float(lot[1])
                available_qty = float(lot[2])
                
                consumed_qty = min(qty_to_sell, available_qty)
                
                # Calculate PnL for this specific lot
                chunk_pnl = (float(sell_price) - buy_price) * consumed_qty
                gross_pnl += chunk_pnl
                
                # Update lot remaining quantity and status
                new_lot_qty = available_qty - consumed_qty
                lot_status = 'CLOSED' if new_lot_qty == 0 else 'OPEN'
                
                cur.execute("""
                    UPDATE tax_lots 
                    SET remaining_quantity = %s, status = %s 
                    WHERE lot_id = %s
                """, (new_lot_qty, lot_status, lot_id))
                
                qty_to_sell -= consumed_qty

            # 3. Halt and rollback if the user sold more than they own
            if qty_to_sell > 0:
                conn.rollback()
                return {"status": "error", "message": f"❌ Insufficient total shares. Missing {qty_to_sell} shares to complete execution."}

            # 4. Calculate CGT (15% on positive gains, 0 on losses)
            cgt_deducted = gross_pnl * 0.15 if gross_pnl > 0 else 0.0
            net_pnl = gross_pnl - cgt_deducted
            total_proceeds = (float(sell_quantity) * float(sell_price)) - cgt_deducted

            # 5. Log to the permanent CGT Ledger
            cur.execute("""
                INSERT INTO realized_cgt_ledger 
                (discord_user_id, ticker, sell_quantity, sell_price, gross_realized_pnl, cgt_deducted, net_realized_pnl)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (user_id, ticker, sell_quantity, sell_price, gross_pnl, cgt_deducted, net_pnl))

            # 6. Update master holdings and release cash to user ledger
            cur.execute("""
                UPDATE portfolio_holdings 
                SET shares = shares - %s 
                WHERE discord_user_id = %s AND ticker = %s
            """, (sell_quantity, user_id, ticker))

            # Delete if zero
            cur.execute("DELETE FROM portfolio_holdings WHERE shares <= 0 AND discord_user_id = %s AND ticker = %s", (user_id, ticker))

            cur.execute("""
                INSERT INTO cash_ledger (discord_user_id, transaction_type, amount, ticker)
                VALUES (%s, 'SELL', %s, %s)
            """, (user_id, total_proceeds, ticker))

            conn.commit()
            
            return {
                "status": "success",
                "message": f"✅ Ledger Updated: SOLD {sell_quantity} shares of {ticker} @ Rs.{sell_price:,.2f}. Gross PnL: Rs.{gross_pnl:,.2f} | CGT: Rs.{cgt_deducted:,.2f} | Net Proceeds: Rs.{total_proceeds:,.2f}",
                "gross_pnl": gross_pnl,
                "cgt_deducted": cgt_deducted,
                "net_pnl": net_pnl
            }

    except Exception as e:
        conn.rollback()
        raise e
    finally:
        if DB_POOL: DB_POOL.putconn(conn)
        else: conn.close()
if __name__ == "__main__":
    # Test executions
    print(run_price_sentinel())
