from __future__ import annotations

import pandas as pd
import plotly.express as px


def vol_delta_top_bar(df: pd.DataFrame, top_n: int = 10):
    """Bar chart showing top-N symbols by absolute volume delta."""

    if df.empty or "vol_delta" not in df.columns:
        empty_df = pd.DataFrame({"symbol": [], "abs_vol_delta": []})
        return px.bar(empty_df, x="symbol", y="abs_vol_delta", title="Top |vol_delta|")

    df_sorted = df.sort_values("timestamp")
    latest = df_sorted.groupby("symbol", as_index=False).tail(1)
    latest = latest.assign(abs_vol_delta=latest["vol_delta"].abs())
    top = latest.sort_values("abs_vol_delta", ascending=False).head(top_n)

    fig = px.bar(
        top,
        x="symbol",
        y="abs_vol_delta",
        title="Top Symbols by |vol_delta| (latest bar)",
    )
    fig.update_layout(height=420, xaxis_title=None, yaxis_title="|vol_delta|")
    return fig
