import os
import psycopg2
import yfinance as yf
import pandas as pd
import pandas_ta as ta
import logging

logger = logging.getLogger("PortfolioAgentTechnicalEngine")

def sanitize_ohlc_data(df: pd.DataFrame, ticker: str) -> pd.DataFrame:
    """
    Enforces chronological order and strict mathematical constraints on OHLC data.
    Purges corrupted rows to prevent indicator hallucination.
    """
    if df is None or df.empty:
        return df

    initial_len = len(df)
    
    # 1. Parse and sort strictly ascending (Oldest -> Newest)
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'])
        df = df.sort_values(by='Date', ascending=True).reset_index(drop=True)
    elif not df.index.is_monotonic_increasing:
        df = df.sort_index(ascending=True)

    # 2. Drop malformed PSX tape rows where Low > High or volume is invalid
    df = df[df['Low'] <= df['High']]
    df = df.dropna(subset=['Open', 'High', 'Low', 'Close'])
    
    dropped = initial_len - len(df)
    if dropped > 0:
        print(f"[SANITIZER] {ticker}: Purged {dropped} corrupted row(s) violating OHLC constraints.")
        
    return df


def fetch_technical_signals(ticker: str) -> dict:
    """
    Fetches the last 3 months of daily data for a PSX ticker via Yahoo Finance.
    Calculates the 14-day RSI and a 20-day Volume SMA surge multiplier.
    """
    try:
        yf_ticker = f"{ticker}.KA"
        # 3mo lookback guarantees enough data for 14-RSI and 20-Volume SMA smoothing
        df = yf.download(yf_ticker, period="3mo", interval="1d", progress=False)
        
        # yfinance sometimes returns a multi-index column DataFrame, flatten it if needed
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
            
        if df.empty or len(df) < 25:
            return None
            
        # Sanitize before returning to the math engine
        df = sanitize_ohlc_data(df, ticker)
        
        if df.empty or len(df) < 25:
            return None
            
        # Ensure we have valid numeric columns
        close_series = pd.to_numeric(df['Close'], errors='coerce')
        volume_series = pd.to_numeric(df['Volume'], errors='coerce')
        
        if close_series.isnull().all() or volume_series.isnull().all():
            return None

        # Add pandas-ta RSI calculation
        df['RSI_14'] = df.ta.rsi(length=14)
        
        # Calculate 20-day Volume SMA
        df['Volume_SMA_20'] = volume_series.rolling(window=20).mean()
        
        # Drop rows where we couldn't calculate indicators
        df_valid = df.dropna(subset=['RSI_14', 'Volume_SMA_20'])
        
        if df_valid.empty:
            return None
            
        latest_row = df_valid.iloc[-1]
        
        rsi = float(latest_row['RSI_14'])
        latest_volume = float(latest_row['Volume'])
        volume_sma_20 = float(latest_row['Volume_SMA_20'])
        
        volume_surge = latest_volume / volume_sma_20 if volume_sma_20 > 0 else 0
        current_price = float(latest_row['Close'])
        
        return {
            'rsi': rsi,
            'volume_surge': volume_surge,
            'current_price': current_price
        }
    except Exception as e:
        logger.error(f"Error fetching technical signals for {ticker}: {e}")
        return None

def run_technical_scanner() -> list:
    """
    Queries Supabase for active tickers, calculates technical signals, 
    and returns a list of alert messages if thresholds are met.
    """
    alerts = []
    conn = None
    try:
        db_url = os.getenv("SUPABASE_DB_URL")
        if not db_url:
            logger.error("SUPABASE_DB_URL not set in environment.")
            return alerts
            
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        
        # Only query tickers that we are tracking
        cur.execute("SELECT ticker FROM watchlist_targets")
        rows = cur.fetchall()
        
        for row in rows:
            ticker = row[0]
            signals = fetch_technical_signals(ticker)
            
            if not signals:
                continue
                
            rsi = signals['rsi']
            volume_surge = signals['volume_surge']
            current_price = signals['current_price']
            
            triggered = False
            condition_text = ""
            
            if rsi < 30:
                triggered = True
                condition_text = f"heavily oversold (RSI: {rsi:.1f})"
            elif rsi > 70:
                triggered = True
                condition_text = f"heavily overbought (RSI: {rsi:.1f})"
                
            if volume_surge > 2.0:
                if triggered:
                    condition_text += f" with a {volume_surge:.1f}x volume surge"
                else:
                    triggered = True
                    condition_text = f"experiencing a {volume_surge:.1f}x volume anomaly"
                    
            if triggered:
                alerts.append({
                    "ticker": ticker,
                    "message": f"⚡ **TECHNICAL ALERT**: {ticker} is {condition_text}. Current Price: Rs. {current_price:.2f}"
                })
                
    except Exception as e:
        logger.error(f"Error in run_technical_scanner: {e}")
    finally:
        if conn:
            conn.close()
            
    return alerts
