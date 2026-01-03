# Stock Price Drift and Anomaly Detection using Apache Spark Streaming

This project implements a **real-time anomaly detection pipeline** for stock prices using **Apache Spark Structured Streaming**. It detects **drift**, **volatility spikes**, and **statistical anomalies** in high-frequency stock data using a mix of **EWMA**, **Z-score**, and **Bollinger Bands** — visualized in an interactive dashboard.

---

## Project Objectives

- Ingest streaming stock data using Spark Structured Streaming
- Detect:
  - **Drift** using EWMA (Exponential Weighted Moving Average)
  - **Volatility boundaries** using **Bollinger Bands**
  - **Outliers** using **Z-score anomaly detection**
- Visualize trends, anomalies, and alerts using **Dash (Plotly)**
- Simulate data locally and deploy to **AWS EC2 cluster**

---

## Project Structure

```
DIS_Workshop/
├── data/
│   ├── raw/                      # Downloaded full TSLA CSV
│   ├── stream_input/             # Minute-sliced simulated stream
│   └── output/
│       ├── combined_anomalies/   # Final output
│       └── checkpoint_combined/  # Spark state checkpoints
├── scripts/
│   ├── download_stock_data.py    # Download OHLC data
│   ├── split_to_stream.py        # Split OHLC CSV into minute-based rows
│   ├── simulate_live_feed.py     # Simulated streaming writer
│   ├── streaming_ewma.py         # EWMA calculation, prints output     
│   ├── streaming_zscore.py       # Z-score calculation, prints output
│   ├── streaming_bollinger.py    # Bollinger calculation, prints output
│   └── streaming_combined.py     # Unified detection pipeline for dashboard
├── dashboard/
│   └── app.py                    # Interactive Dash dashboard
├── notebooks/
│   └── analysis.ipynb            # Jupyter-based visual analysis
├── requirements.txt
└── README.md
```

---

## Local Setup & Execution

### 1. Setup Environment

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

---

### 2. Download & Prepare TSLA Data

```bash
python scripts/download_stock_data.py
python scripts/split_to_stream.py
```

---
## Note: In three separate Terminals launch the following three steps sequentially

### 3. Simulate Live Stream Feed

```bash
python scripts/simulate_live_feed.py
```

---

### 4. Run Spark Stream Processor

```bash
python scripts/streaming_combined.py
```

---

### 5. Launch Dashboard

```bash
python dashboard/app.py
```

Then open: [http://localhost:8050](http://localhost:8050)

---

## Dashboard Features

- Live updating chart
- Anomaly markers
- Textual recommendations:
  - Bullish / Bearish trends
  - Overbought / Undervalued
  - Z-score anomaly alerts

---

## Interpretation

| Signal           | Meaning                                      |
|------------------|----------------------------------------------|
| 📈 Bullish       | Price above EWMA, upward trend               |
| 📉 Bearish       | Price below EWMA, downward trend             |
| ⚠️ Overbought    | Price exceeds upper Bollinger Band           |
| 🔻 Undervalued   | Price falls below lower Bollinger Band       |
| 🚨 Z > 2         | Outlier detected, possible anomaly           |

---

## Authors

- Soham Roy
- Scott Barcomb
