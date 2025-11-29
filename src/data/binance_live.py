from __future__ import annotations

from datetime import datetime

import pandas as pd
import requests

BASE_URL = "https://api.binance.com/api/v3/klines"


def fetch_klines(symbol: str, interval="1m", limit=20) -> pd.DataFrame:
    """Fetch last N klines for symbol."""
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    r = requests.get(BASE_URL, params=params, timeout=5)
    r.raise_for_status()

    data = r.json()
    rows = []

    for k in data:
        rows.append(
            {
                "timestamp": datetime.fromtimestamp(k[0] / 1000),
                "symbol": symbol,
                "price": float(k[4]),  # close
                "volume": float(k[5]),
            }
        )

    return pd.DataFrame(rows)


def load_binance_live(symbols: list[str]) -> pd.DataFrame:
    """Fetch latest 20 bars for each symbol and concatenate."""
    frames = []
    for sym in symbols:
        df = fetch_klines(sym)
        frames.append(df)

    return pd.concat(frames, ignore_index=True)
