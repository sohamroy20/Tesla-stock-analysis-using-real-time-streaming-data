import pandas as pd
import time
from pathlib import Path
import subprocess

# Load full static dataset
df = pd.read_csv("data/raw/TSLA_ohlc.csv", skiprows=3)
df.columns = ['Datetime', 'Open', 'High', 'Low', 'Close', 'Volume']
df['Adj Close'] = df['Close']  # Create dummy Adj Close column
df = df[['Datetime', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']]
hdfs_dir = '/Workshop/data/stream_input'

# Write CSV row to HDFS
def write_row_to_hdfs(csv_string: str, hdfs_path: str):
    proc = subprocess.run(
        ['hdfs', 'dfs', '-put', '-', hdfs_path],
        input=csv_string.encode('utf-8'),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    if proc.returncode != 0:
        raise RuntimeError(f"HDFS write failed (exit {proc.returncode}):\n{proc.stderr.decode()}")

# Stream one row at a time
for i, row in df.iterrows():
    csv_str = row.to_frame().T.to_csv(index=False)
    hdfs_file = f"{hdfs_dir}/row_{i:04d}.csv"
    write_row_to_hdfs(csv_str, hdfs_file)
    print(f"Wrote to HDFS: {Path(hdfs_file).name}")