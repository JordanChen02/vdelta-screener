from __future__ import annotations

import streamlit as st
import pandas as pd

from src.data.load import load_prices
from src.features.deltas import compute_vdelta
from src.charts.vol_delta_bar import vol_delta_top_bar


def render_screener() -> None:
    st.header("Screener")

    # Sidebar controls
    with st.sidebar:
        st.subheader("Controls")
        symbols_text = st.text_input("Symbols (comma-separated)", "BTC,ETH,SOL")
        timeframe = st.selectbox("Timeframe", ["5m", "15m", "1h"], index=1)
        top_n = st.number_input("Top-N", min_value=3, max_value=50, value=10, step=1)
        refresh = st.button("Refresh")

    # Refresh clears cached data loaders
    if refresh:
        try:
            st.cache_data.clear()
        except Exception:
            pass

    # Load data (stub for now)
    df = load_prices(None)  # your loader should return columns: timestamp, symbol, close, volume

    # Guard if nothing loaded
    if df is None or df.empty or not {"symbol", "timestamp", "volume"}.issubset(df.columns):
        st.info("No data loaded yet. CSV/Live source will be wired next.")
        return

    # Compute vol_delta
    df = compute_vdelta(df)

    # Show latest row per symbol
    latest = (
        df.sort_values(["symbol", "timestamp"])
          .groupby("symbol", as_index=False)
          .tail(1)
          .sort_values("symbol")
    )
    st.caption("Latest bar per symbol")
    st.dataframe(latest, use_container_width=True, hide_index=True)

    # Chart: Top-N by |vol_delta| at latest bar
    st.plotly_chart(vol_delta_top_bar(df, top_n=top_n), use_container_width=True)

    # About
    with st.expander("About vDelta"):
        st.write(
            "Current proxy uses percent change of volume per symbol. "
            "Live taker-signed vDelta from trade streams and market-cap normalization are planned next."
        )
