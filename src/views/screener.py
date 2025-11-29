import time

import pandas as pd
import plotly.graph_objects as go
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
    # --- Ensure session_state keys exist ---
    if "collector" not in st.session_state:
        st.session_state.collector = None

    if "flow_history" not in st.session_state:
        st.session_state.flow_history = pd.DataFrame()

    st.title("Rotation Screener (Live - Binance)")
    st.caption("vDelta from WebSocket trade stream")

    # ---- Sidebar ----
    symbols_raw = st.sidebar.text_input("Symbols (comma-separated)", "BTC,ETH,SOL,HYPE")
    symbols = [s.strip().upper() + "USDT" for s in symbols_raw.split(",")]

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

    # ============================================================
    #              FLOW SHARE CALCULATION (CORRECTED)
    # ============================================================

    # df already has:
    #   vdelta_notional
    #   avg_1s_notional
    #   vdelta_eff  <-- our normalized flow-momentum metric

    # 1. Keep only symbols with liquidity baseline
    valid = df[df["avg_1s_notional"] > 0].copy()

    if valid.empty:
        st.warning("No recent trades — cannot compute rotation.")
        return

    # 2. Extract rotation momentum (efficiency)
    momentum = valid.set_index("symbol")["vdelta_eff"]

    # 3. Use absolute value for rotation pressure magnitude
    abs_momentum = momentum.abs()

    # 4. Normalize → flowshare
    flow_shares = (abs_momentum / abs_momentum.sum()).to_dict()
    # alias to avoid NameError

    # ============================================================
    #              MAINTAIN ROLLING HISTORY (CLEAN)
    # ============================================================

    hist = st.session_state.flow_history

    # latest timestamp available
    ts = df["timestamp"].max()

    # build one row with flowshare values
    row = {"timestamp": ts}
    for sym in symbols:
        row[sym] = flow_shares.get(sym, 0.0)

    # append + keep last 120 seconds
    hist = pd.concat([hist, pd.DataFrame([row])], ignore_index=True)
    hist = hist.tail(120)

    st.session_state.flow_history = hist

    # ============================================================
    #          SIDE-BY-SIDE CHARTS (STACKED BAR + SPAGHETTI)
    # ============================================================
    col1, col2 = st.columns(2)

    # ----------------------
    # COLORS
    # ----------------------
    COLORS = {
        "BTCUSDT": "#F7931A",  # BTC Orange
        "ETHUSDT": "#7FAAF2",  # Light ETH Blue
        "SOLUSDT": "#5B2EE8",  # Purple SOL
        "HYPEUSDT": "#00FF87",  # neon HYPE green (official)
    }

    # -------------------------
    #   STACKED HORIZONTAL BAR
    # -------------------------
    with col1:
        st.subheader("Flowshare (Now)")

        fig_now = go.Figure()

        for sym in symbols:
            fig_now.add_trace(
                go.Bar(
                    x=[flow_shares.get(sym, 0.0)],
                    y=[sym],
                    orientation="h",
                    name=sym,
                    marker_color=COLORS[sym],
                    hovertemplate=f"{sym}: {flow_shares.get(sym, 0.0):.2%}<extra></extra>",
                )
            )

        fig_now.update_layout(
            barmode="stack",
            height=180,
            showlegend=True,
            xaxis_title="Share",
            yaxis_title="",
            margin=dict(l=20, r=20, t=20, b=20),
        )

        st.plotly_chart(fig_now, use_container_width=True)

    # -------------------------
    #       SPAGHETTI CHART
    # -------------------------
    with col2:
        st.subheader("Flow Share (Last 2 Minutes)")
        if len(hist) > 1:
            spaghetti = st.session_state.flow_history.set_index("timestamp")
            st.line_chart(
                spaghetti,
                color=[COLORS.get(sym, "#999999") for sym in spaghetti.columns],
                use_container_width=True,
            )
        else:
            st.info("Collecting live history…")


# REQUIRED BY app.py
render_page = render_screener
