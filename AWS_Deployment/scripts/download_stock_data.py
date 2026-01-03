import yfinance as yf
import os

# Make sure 'data/' directory exists
os.makedirs("data/raw", exist_ok=True)

# Download TSLA 1-minute data for the last 5 days
ticker = 'TSLA'
data = yf.download(ticker, interval='1m', period='5d')
data.to_csv("data/raw/TSLA_ohlc.csv")
print("Downloaded data saved to ./data/raw/TSLA_ohlc.csv")