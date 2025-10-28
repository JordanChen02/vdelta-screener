from __future__ import annotations
import streamlit as st
from src.data.load import load_prices
from src.features.deltas import compute_vdelta
from src.charts.vol_delta_bar import vol_delta_top_bar

def render_screener():
    st.header("Screener")
    st.caption("Skeleton ready. Next step: wire data")
    df = load_prices()
    df = compute_vdelta(df)
    st.dataframe(df)
    st.plotly_chart(vol_delta_top_bar(df), use_container_width=True)
