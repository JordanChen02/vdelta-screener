# src/data/load.py
from __future__ import annotations

import pandas as pd
import streamlit as st


@st.cache_data
def load_prices(path_or_endpoint: str | None = None) -> pd.DataFrame:
    """
    Load price and volume data.
    For now uses a local CSV as placeholder.
    Expected columns: timestamp, symbol, close, volume
    """

    # Guard clause
    if path_or_endpoint is None:
        path_or_endpoint = "src/data/mock_prices.csv"

    try:
        df = pd.read_csv(path_or_endpoint)
    except FileNotFoundError:
        # Return an empty DataFrame if missing
        return pd.DataFrame(columns=["timestamp", "symbol", "close", "volume"])

    # Clean types
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    df["symbol"] = df["symbol"].astype(str).str.upper()
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")

    # Drop bad rows
    df = df.dropna(subset=["timestamp", "symbol", "close", "volume"])

    return df
