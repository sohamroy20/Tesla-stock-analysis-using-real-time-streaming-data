import pandas as pd
import os

# Reading the full dataset
df = pd.read_csv("data/raw/TSLA_ohlc.csv")

# Output folder for streaming chunks
output_dir = "data/stream_input"
os.makedirs(output_dir, exist_ok=True)

# Write each row as a single CSV file
for i, row in df.iterrows():
    row_df = row.to_frame().T
    row_df.to_csv(f"{output_dir}/part_{i:04}.csv", index=False)

print(f"{len(df)} streaming files created in {output_dir}/")