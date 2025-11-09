from __future__ import annotations
import pandas as pd
import numpy as np

def compute_vdelta(df): 
    """
    Takes a DataFrame with columns: symbol, timestamp, and volume.
    Adds a new column vol_delta showing percent change in volume for each symbol.
    """

    required_columns = {"symbol", "timestamp", "volume"}
    if df is None or df.empty or not required_columns.issubset(df.columns):
        return df
    
    out = df.copy()

    out = out.dropna(subset=["symbol", "timestamp", "volume"])
    out = out.sort_values(["symbol", "timestamp"])
    out["vol_delta"] = out.groupby("symbol")["volume"].pct_change()

    out["vol_delta"].replace([np.inf, -np.inf], np.nan, inplace=True)
    out["vol_delta"].fillna(0.0, inplace=True)

    return out

def normalize_by_mcap(snapshot: pd.DataFrame, mcap_map: dict, col: str) -> pd.DataFrame:
    """
    This function divides a metric column (named in col) by market cap
    Requires a 'symbol' column in input
    Adds a new column end with _norm
    """

    if snapshot is None or snapshot.empty or "symbols" not in snapshot.columns or col not in snapshot.columns:
        return snapshot
    
    df = snapshot.copy()

    df["symbol"] = df["symbol"].astype(str).str.upper()
    df["mcap_usd"] = df["symbol"].map(mcap_map).astype("float64")

    df[f"{col}_norm"] = df[col] / df["mcap_usd"]
    df[f"{col}_norm"].replace([np.inf, -np.inf], np.nan, inplace=True)
    return df

def cross_sectional_zscore(snapshot: pd.DataFrame, col: str, out_col: str) -> pd.DataFrame:
    if snapshot is None or snapshot.empty or col not in snapshot.columns:
        return snapshot
    
    df = snapshot.copy()

    # Pull the column as a numeric NumPy array (float64) for math
    x = df[col].to_numpy(dtype="float64")

    mu = np.nanmean(x) if x.size else np.nan
    sd = np.nanstd(x) if x.size else np.nan

    if sd == 0 or np.isnan(sd):
        df[out_col] = np.nan
    else:
        df[out_col] = (x - mu) / sd

    return df

