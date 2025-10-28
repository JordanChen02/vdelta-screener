from __future__ import annotations
import pandas as pd

def load_prices(path_or_endpoint: str | None = None) -> pd.DataFrame:
    # placeholder empty frame with required columns
    return pd.DataFrame(columns=["timestamp", "symbol", "close", "volume"])