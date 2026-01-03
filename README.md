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

### Local Structure (on NameNode)

```
AWS_Deployment/
├── data/
│   ├── raw/                      # Downloaded full TSLA CSV
├── scripts/
│   ├── download_stock_data.py    # Download OHLC data
│   ├── simulate_live_feed.py     # Simulated streaming writer
│   └── streaming_combined.py     # Unified detection pipeline
├── dashboard/
│   └── app.py                    # Interactive Dash dashboard
├── requirements.txt
└── README.md
```

---

### HDFS Structure
```
/Workshop/
└── data/
    ├── stream_input/             # Minute-sliced simulated stream
    └── output/
        ├── combined_anomalies/   # Final enriched outputs
        └── checkpoint_combined/  # Spark state checkpoints
```

---

## Setup & Execution

### 1. Setup Environment on the NameNode

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

---

### 2. Download & Prepare TSLA Data

```bash
python scripts/download_stock_data.py
```

---

## Note: In three separate Terminals on the NameNode, launch the following three steps sequentially. Ensure that you have Hadoop DFS, YARN, and Apache Spark configured with at least one worker/data node.

### 3. Simulate Live Stream Feed

```bash
python scripts/simulate_live_feed.py
```

---

### 4. Run Spark Stream Processor

```bash
spark-submit --master yarn scripts/streaming_combined.py
```

---

### 5. Launch Dashboard

```bash
python dashboard/app.py
```

---

### 6. Access the Dashboard via SSH tunneling

```bash
ssh -i [yourkey.pem] -L 8050:localhost:8050 admin@[node-public-dns/ip-address]
```

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
