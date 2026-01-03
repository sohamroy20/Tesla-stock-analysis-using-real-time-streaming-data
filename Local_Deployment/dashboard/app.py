import dash
from dash import dcc, html
import pandas as pd
from dash.dependencies import Input, Output
import plotly.graph_objs as go
from pathlib import Path

app = dash.Dash(__name__)
app.title = "TSLA Anomaly Dashboard"

csv_dir = Path("data/output/combined_anomalies")

def load_data():
    files = [f for f in csv_dir.glob("part-*.csv") if f.stat().st_size > 0]
    dfs = []
    for f in files:
        try:
            df_chunk = pd.read_csv(f, header=None)
            if df_chunk.shape[1] == 9:
                dfs.append(df_chunk)
        except:
            continue
    if not dfs:
        return pd.DataFrame()
    df = pd.concat(dfs, ignore_index=True)
    df.columns = ['window_start', 'window_end', 'latest_close', 'EWMA', 'z_score',
                  'is_z_anomaly', 'upper_band', 'lower_band', 'is_band_anomaly']
    # Cast flags
    df['is_z_anomaly']    = df['is_z_anomaly'].astype(bool)
    df['is_band_anomaly'] = df['is_band_anomaly'].astype(bool)
    df['window_end'] = pd.to_datetime(df['window_end'])
    df = df.sort_values("window_end")
    df = df.drop_duplicates(subset="window_end", keep="last")
    df['EWMA_slow'] = df['latest_close'].ewm(alpha=0.3, adjust=False).mean()
    return df

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
    Output('price_graph', 'figure'),
    Input('interval', 'n_intervals')
)
def update_graph(n):
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

# Suggestion Text Callback
@app.callback(
    Output("suggestion-box", "children"),
    Input("interval", "n_intervals")
)
def update_suggestion(n):
    try:
        folder = Path("data/output/combined_anomalies")
        files = sorted(folder.glob("*.csv"), key=lambda f: f.stat().st_mtime)
        df = pd.concat([pd.read_csv(f, header=None) for f in files[-5:] if f.stat().st_size > 0])
        df.columns = ['window_start', 'window_end', 'latest_close', 'EWMA', 'z_score',
                      'is_z_anomaly', 'upper_band', 'lower_band', 'is_band_anomaly']
        last = df.iloc[-1]

        messages = []

        if last['latest_close'] > last['EWMA']:
            messages.append("📈 Bullish: Price above EWMA. Consider buying or holding the stock")
        else:
            messages.append("📉 Bearish: Price below EWMA. Consider selling, shorting, or avoiding the stock")

        if last['is_band_anomaly']:
            if last['latest_close'] > last['upper_band']:
                messages.append("⚠️ Overbought (above upper band)")
            else:
                messages.append("🔻 Undervalued (below lower band)")

        if abs(last['z_score']) > 2:
            messages.append(f"🚨 Strong anomaly detected (Z={last['z_score']:.2f})")

        return " | ".join(messages)

    except Exception as e:
        return "Waiting for sufficient data..."

if __name__ == '__main__':
    app.run(debug=True)