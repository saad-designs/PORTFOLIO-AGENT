import yfinance as yf
import pandas as pd
import datetime

data = yf.download("LUCK.KA", start="2024-01-01", progress=False)
print("Type of data:", type(data))
print("Columns:", data.columns)
if isinstance(data.columns, pd.MultiIndex):
    close_col = data['Close'].iloc[:, 0]
    print("MultiIndex extraction success. Shape:", close_col.shape)
else:
    close_col = data['Close']
    print("Single Index extraction success. Shape:", close_col.shape)
