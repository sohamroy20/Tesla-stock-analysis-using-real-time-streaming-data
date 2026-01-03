import dash
from dash import dcc, html
import pandas as pd
from dash.dependencies import Input, Output
import plotly.graph_objs as go
from hdfs import InsecureClient

app = dash.Dash(__name__)
app.title = "TSLA Anomaly Dashboard"

HDFS_PATH = '/Workshop/data/output/combined_anomalies'

# Open an HDFS client
# Note: You must update the IP address to that of your Name node, which is known by
# Hadoop and the rest of your cluster. Additionally, make sure port 9870 is open for
# listening in your AWS security group.
client = InsecureClient('http://xxx.xx.xx.xxx:9870', user='hduser')

def load_data():
    try:
        # 1) List directory WITH status so we know each file's size/type
        entries = client.list(HDFS_PATH, status=True)

        # 2) Keep only real CSVs that are non-empty
        csv_files = [
            name for name, info in entries
            if info['type'] == 'FILE'
             and name.endswith('.csv')
             and info.get('length', 0) > 0
        ]
        if not csv_files:
            return pd.DataFrame()

        dfs = []
        for fname in csv_files:
            full_path = f"{HDFS_PATH}/{fname}"
            # 3) Read each file
            with client.read(full_path, encoding='utf-8') as reader:
                df_chunk = pd.read_csv(reader, header=None)
            # only keep well-formed chunks
            if df_chunk.shape[1] == 9:
                dfs.append(df_chunk)

        if not dfs:
            return pd.DataFrame()

        # 4) Concatenate & rename
        df = pd.concat(dfs, ignore_index=True)
        df.columns = [
            'window_start','window_end','latest_close','EWMA','z_score',
            'is_z_anomaly','upper_band','lower_band','is_band_anomaly'
        ]
        # Cast flags
        df['is_z_anomaly']    = df['is_z_anomaly'].astype(bool)
        df['is_band_anomaly'] = df['is_band_anomaly'].astype(bool)
        # Parse and dedupe
        df['window_end'] = pd.to_datetime(df['window_end'])
        df = (df
              .sort_values("window_end")
              .drop_duplicates("window_end", keep="last"))
        # Slow EWMA
        df['EWMA_slow'] = df['latest_close'].ewm(alpha=0.3, adjust=False).mean()
        return df

    except Exception as e:
        print("load_data error:", e)
        return pd.DataFrame()

# Layout
app.layout = html.Div([
    html.H2("📈 TSLA Anomaly Detection Dashboard"),
    dcc.Interval(id='interval', interval=10*1000, n_intervals=0),
    dcc.Graph(id='price_graph'),
    html.H3("💡 Suggested Action", style={"color": "#000099"}),
    html.Div(id="suggestion-box", style={"color": "orange", "fontSize": 18}),
])

# Graph Callback
@app.callback(
    Output("price_graph", "figure"),
    Input("interval", "n_intervals")
)
def update_graph(n):
    try:
        df = load_data()
        if df.empty:
            return go.Figure()

        fig = go.Figure()

        fig.add_trace(go.Scatter(x=df['window_end'], y=df['latest_close'],
                                mode='lines', name='Latest Close', line=dict(color='deepskyblue')))

        fig.add_trace(go.Scatter(x=df['window_end'], y=df['EWMA_slow'],
                                mode='lines', name='EWMA (α=0.3)', line=dict(color='orange', dash='dash')))

        fig.add_trace(go.Scatter(x=df['window_end'], y=df['upper_band'],
                                mode='lines', name='Upper Band', line=dict(color='gray', dash='dot')))
        fig.add_trace(go.Scatter(x=df['window_end'], y=df['lower_band'],
                                mode='lines', name='Lower Band', line=dict(color='gray', dash='dot')))

        anomalies = df[df['is_z_anomaly'] | df['is_band_anomaly']]
        fig.add_trace(go.Scatter(
            x=anomalies['window_end'], y=anomalies['latest_close'],
            mode='markers', name='Anomalies',
            marker=dict(color='red', size=10, symbol='x')
        ))

        fig.update_layout(
            title="TSLA Price, EWMA & Anomalies",
            xaxis_title="Time", yaxis_title="Price",
            template="plotly_dark"
        )

        return fig
    
    except Exception as e:
        print("update_graph error:", e)
        return fig

# Suggestion Text Callback
@app.callback(
    Output("suggestion-box", "children"),
    Input("interval", "n_intervals")
)
def update_suggestion(n):
    try:
        # reuse the same HDFS-backed loader
        df = load_data()
        if df.empty:
            return "Waiting for sufficient data…"

        # take the most recent timestamp
        last = df.iloc[-1]

        messages = []
        # Bullish vs. Bearish
        if last['latest_close'] > last['EWMA']:
            messages.append("📈 Bullish: Price above EWMA. Consider buying or holding the stock")
        else:
            messages.append("📉 Bearish: Price below EWMA. Consider selling, shorting, or avoiding the stock")

        # Band anomalies
        if last['is_band_anomaly']:
            if last['latest_close'] > last['upper_band']:
                messages.append("⚠️ Overbought (above upper band)")
            else:
                messages.append("🔻 Undervalued (below lower band)")

        # Z-score anomalies
        if abs(last['z_score']) > 2:
            messages.append(f"🚨 Strong anomaly detected (Z={last['z_score']:.2f})")

        return " | ".join(messages)

    except Exception as e:
        print("update_suggestion error:", e)
        return "Waiting for sufficient data…"

if __name__ == '__main__':
    app.run(debug=True)