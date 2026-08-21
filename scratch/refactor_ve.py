import sys
import re

with open("valuation_engine.py", "r", encoding="utf-8") as f:
    content = f.read()

# We want to replace the `execute_trade_ledger` function entirely
# with `execute_buy_fifo` and `execute_sell_fifo`

new_funcs = '''
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
'''

# Find execute_trade_ledger and replace it.
pattern = re.compile(r"def execute_trade_ledger\(trade\).*?(?=if __name__ ==)", re.DOTALL)
new_content = pattern.sub(new_funcs, content)

with open("valuation_engine.py", "w", encoding="utf-8") as f:
    f.write(new_content)

print("valuation_engine patched")
