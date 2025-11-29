import time

import streamlit as st
from streamlit_autorefresh import st_autorefresh

from src.data.binance_ws import TradeCollector
from src.data.mcap import get_mcap_map
from src.features.deltas import cross_sectional_zscore, normalize_by_mcap

# --------------------------
# Global WS Collector State
# --------------------------
if "collector" not in st.session_state:
    st.session_state.collector = None


def start_ws_if_needed(symbols):
    """Start the TradeCollector WebSocket only once."""
    col = st.session_state.collector

    if col is None:
        col = TradeCollector(window_seconds=300)  # 5-min rolling window
        col.start(symbols)
        st.session_state.collector = col
        time.sleep(1.0)  # allow websocket to warm up

    return st.session_state.collector


# ==========================
#        UI PAGE
# ==========================
def render_screener():
    # Smooth auto-refresh: once per 1 second
    st_autorefresh(interval=1000, key="live_refresh")

    st.title("Rotation Screener (Live - Binance)")
    st.caption("vDelta from WebSocket trade stream")

    # ---- Sidebar ----
    symbols_raw = st.sidebar.text_input("Symbols (comma-separated)", "BTC,ETH,SOL")
    symbols = [s.strip().upper() + "USDT" for s in symbols_raw.split(",")]

    top_n = st.sidebar.number_input("Top-N", min_value=1, max_value=50, value=10)

    rank_mode = st.sidebar.radio(
        "Rank by",
        ["Raw (vol_delta)", "Normalized (vol_delta_norm)"],
        index=0,
    )

    if st.sidebar.button("Clear Cached WebSocket"):
        st.session_state.collector = None
        st.success("WebSocket collector reset. Restarting…")
        st.stop()

    # ---- Start or reuse WS collector ----
    collector = start_ws_if_needed(symbols)

    # ---- Pull snapshot from memory ----
    df = (
        collector.snapshot()
    )  # cols: timestamp, symbol, price, volume, vdelta_qty, vdelta_notional
    df = df.rename(columns={"last_ts": "timestamp"})
    # Map WS fields to UI fields
    df["price"] = df["last_price"]

    # TEMP: use vdelta_notional as “volume” placeholder
    df["volume"] = df["vdelta_notional"]
    # st.write(df.head())
    # st.stop()

    if df.empty:
        st.warning("Waiting for live Binance trade data…")
        return

    # Rename fields to match UI
    df = df.rename(columns={"vdelta_qty": "vol_delta"})

    # ---- Normalize by market cap ----
    mcap_map = get_mcap_map(symbols)
    df = normalize_by_mcap(df, mcap_map, col="vol_delta")

    # ---- Add cross-sectional z-score ----
    df = cross_sectional_zscore(df, col="vol_delta_norm", out_col="vol_delta_z")

    # ---- Latest bar per symbol ----
    latest = df.sort_values("timestamp").groupby("symbol").tail(1)

    st.subheader("Latest bar per symbol (live vDelta)")
    st.dataframe(
        latest[
            ["symbol", "timestamp", "price", "volume", "vol_delta", "vol_delta_norm"]
        ],
        hide_index=True,
        use_container_width=True,
    )

    # ---- Choose ranking metric ----
    if rank_mode == "Raw (vol_delta)":
        value_col = "vol_delta"
    else:
        value_col = "vol_delta_norm"

    # ---- Top movers ----
    top = latest.sort_values(value_col, ascending=False).head(top_n)

    st.subheader(f"Top Symbols by |{value_col}| (latest bar)")
    st.bar_chart(data=top, x="symbol", y=value_col, use_container_width=True)


# REQUIRED BY app.py
render_page = render_screener
