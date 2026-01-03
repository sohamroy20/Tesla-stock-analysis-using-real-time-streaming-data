import pandas as pd
import time
from pathlib import Path

# Load full static dataset
df = pd.read_csv("data/raw/TSLA_ohlc.csv", skiprows=3)
df.columns = ['Datetime', 'Open', 'High', 'Low', 'Close', 'Volume']
df['Adj Close'] = df['Close']  # Create dummy Adj Close column
df = df[['Datetime', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']]
output_dir = Path("data/stream_input")

# Clear existing files
for f in output_dir.glob("*.csv"):
    f.unlink()

# Stream one row at a time
for i, row in df.iterrows():
    filename = output_dir / f"row_{i:04d}.csv"
    row.to_frame().T.to_csv(filename, index=False)
    print(f"Wrote: {filename.name}")
    time.sleep(2)  # Delay to simulate real-time feed