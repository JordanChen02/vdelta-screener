from __future__ import annotations

import numpy as np
import pandas as pd


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
    Take a snapshot DataFrame and divide column `col` by market cap.

    Expects columns:
      - symbol
      - `col`  (e.g. "vol_delta")

    Adds a new column: f"{col}_norm" (e.g. "vol_delta_norm").
    """

    if snapshot is None or snapshot.empty:
        return snapshot
    if "symbol" not in snapshot.columns or col not in snapshot.columns:
        return snapshot

    df = snapshot.copy()

    # Normalize symbol format so it matches keys in mcap_map (e.g. "BTCUSDT")
    df["symbol"] = df["symbol"].astype(str).str.upper()

    # Map each symbol to its USD market cap
    df["mcap_usd"] = df["symbol"].map(mcap_map).astype("float64")

    # Create normalized column (e.g. vol_delta_norm)
    norm_col = f"{col}_norm"
    df[norm_col] = df[col] / df["mcap_usd"]

    # Clean infinities and NaNs
    df[norm_col].replace([np.inf, -np.inf], np.nan, inplace=True)
    df[norm_col].fillna(0.0, inplace=True)

    return df


def cross_sectional_zscore(
    snapshot: pd.DataFrame, col: str, out_col: str
) -> pd.DataFrame:
    """
    Compute a cross-sectional z-score for column `col` across all symbols
    in the snapshot.

    Adds a new column `out_col` containing the z-scored values.
    """

    if snapshot is None or snapshot.empty or col not in snapshot.columns:
        return snapshot

    df = snapshot.copy()

    # convert to float array
    x = df[col].astype("float64").to_numpy()

    # compute mean & std
    mu = np.nanmean(x) if x.size else np.nan
    sd = np.nanstd(x) if x.size else np.nan

    # guard against division by zero
    if sd == 0 or np.isnan(sd):
        df[out_col] = np.nan
    else:
        df[out_col] = (x - mu) / sd

    return df
