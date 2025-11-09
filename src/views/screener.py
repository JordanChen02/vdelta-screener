# src/views/screener.py
from __future__ import annotations

import streamlit as st
import pandas as pd

from src.data.load import load_prices
from src.data.mcap import MCAP_USD
from src.features.deltas import compute_vdelta, normalize_by_mcap
from src.charts.vol_delta_bar import vol_delta_top_bar


def render_screener() -> None:
    st.header("Rotation Screener (proxy vΔ)")

    # ───────── Sidebar controls ─────────
    with st.sidebar:
        st.subheader("Controls")
        symbols_text = st.text_input("Symbols (comma-separated)", "BTC,ETH,SOL")
        timeframe = st.selectbox("Timeframe", ["5m", "15m", "1h"], index=1)
        top_n = st.number_input("Top-N", min_value=3, max_value=50, value=10, step=1)
        rank_mode = st.radio("Rank by", ["Raw (vol_delta)", "Normalized (vol_delta_norm)"], index=0)
        refresh = st.button("Refresh")

    # Clear cached data on demand
    if refresh:
        try:
            st.cache_data.clear()
        except Exception:
            pass

    # ───────── Load mock data ─────────
    df = load_prices(None)  # expects: timestamp, symbol, close, volume

    if df is None or df.empty or not {"symbol", "timestamp", "volume"}.issubset(df.columns):
        st.info("No data loaded yet. Add rows to data/mock_prices.csv")
        return

    # ───────── Compute proxy vΔ (pct-change of volume) ─────────
    df = compute_vdelta(df)  # adds 'vol_delta'

    # ───────── Normalize by market cap ─────────
    # Map CSV symbols (BTC→BTCUSDT, etc.) to the MCAP dictionary keys
    df_norm = df.assign(symbol_usdt=df["symbol"].astype(str).str.upper() + "USDT") \
                .rename(columns={"symbol_usdt": "symbol"})
    df_norm = normalize_by_mcap(df_norm, MCAP_USD, col="vol_delta")  # adds 'vol_delta_norm'

    # Which column to sort/plot
    value_col = "vol_delta" if rank_mode.startswith("Raw") else "vol_delta_norm"

    # ───────── Latest row per symbol table ─────────
    latest = (
        df_norm.sort_values(["symbol", "timestamp"])
               .groupby("symbol", as_index=False)
               .tail(1)
               .sort_values(value_col, ascending=False)
    )
    st.caption(f"Latest bar per symbol (ranked by {value_col})")

    cols_to_show: list[str] = ["symbol", "timestamp", "close", "volume", "vol_delta"]
    if "vol_delta_norm" in latest.columns:
        cols_to_show.append("vol_delta_norm")

    st.dataframe(latest[cols_to_show], use_container_width=True, hide_index=True)

    # ───────── Chart: Top-N by |metric| ─────────
    bar_df = df_norm.rename(columns={value_col: "metric"})
    st.plotly_chart(
        vol_delta_top_bar(bar_df, value_col="metric", top_n=top_n,
                          title=f"Top |{value_col}| (latest)"),
        use_container_width=True,
    )

    # ───────── About ─────────
    with st.expander("About"):
        st.write(
            "- **vol_delta** here is a proxy (percent change of volume per symbol).\n"
            "- **vol_delta_norm** divides that by market cap for fair cross-coin comparison.\n"
            "- Next steps: z-score toggle, spaghetti history, and live taker-signed vΔ."
        )
