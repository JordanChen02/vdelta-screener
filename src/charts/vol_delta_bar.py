from __future__ import annotations
import pandas as pd
import plotly.express as px

def vol_delta_top_bar(df: pd.DataFrame, top_n: int = 10):
    """
    Bar chart of top-N symbols by |latest vol_delta|.
    Requires columns: symbol, timestamp, vol_delta.
    """
    if df.empty or "vol_delta" not in df.columns:
        return px.bar(pd.DataFrame({"symbol": [], "abs_vol_delta": []}),
                      x="symbol", y="abs_vol_delta", title="Top |vol_delta|")

    latest = df.sort_values("timestamp").groupby("symbol", as_index=False).tail(1)
    latest = latest.assign(abs_vol_delta=latest["vol_delta"].abs()).sort_values("abs_vol_delta", ascending=False)
    top = latest.head(top_n)
    fig = px.bar(top, x="symbol", y="abs_vol_delta", title="Top symbols by |vol_delta| (latest bar)")
    fig.update_layout(height=420, xaxis_title=None, yaxis_title="|vol_delta|")
    return fig
