from datetime import timedelta

import pandas as pd

# -----------------------------------
#      TIMEFRAME DEFINITIONS
# -----------------------------------
TF_WINDOWS = {
    "5m": 5 * 60,
    "15m": 15 * 60,
    "30m": 30 * 60,
    "1h": 60 * 60,
    "4h": 4 * 60 * 60,
    "1D": 24 * 60 * 60,
}


def _slice_window(df: pd.DataFrame, window_sec: int) -> pd.DataFrame:
    """
    Return rows in the last `window_sec` seconds based on `timestamp`.
    Expects df to have columns: timestamp, symbol, vdelta_eff.
    """
    if df.empty:
        return df

    now_ts = df["timestamp"].max()
    cutoff = now_ts - timedelta(seconds=window_sec)
    return df[df["timestamp"] >= cutoff].copy()


def _compute_bull_bear(df: pd.DataFrame, window_sec: int):
    """
    For a given window:
    - Sum vdelta_eff per symbol
    - Normalise by total absolute flow
    - Return strongest bull (max share) and strongest bear (min share)
    """
    sliced = _slice_window(df, window_sec)

    if sliced.empty:
        return None

    if "vdelta_eff" not in sliced.columns:
        # Safety: nothing useful to compute
        return None

    # Aggregate net flow per symbol over the window
    grouped = sliced.groupby("symbol")["vdelta_eff"].sum()

    if grouped.empty:
        return None

    total_abs = grouped.abs().sum()
    if total_abs == 0:
        return None

    # Signed share in [-1, 1]
    share = grouped / total_abs

    bull_sym = share.idxmax()
    bull_val = float(share.max())

    bear_sym = share.idxmin()
    bear_val = float(share.min())

    return bull_sym, bull_val, bear_sym, bear_val


def compute_multi_tf_table(df: pd.DataFrame):
    """
    Build multi-TF rows using vdelta_eff:
    For each TF:
      - Bull: symbol with highest positive share
      - Bear: symbol with most negative share
    """
    results = []

    for label, sec in TF_WINDOWS.items():
        res = _compute_bull_bear(df, sec)

        if res is None:
            results.append(
                {
                    "TF": label,
                    "Bull": "-",
                    "Bull %": "-",
                    "Bull Dir": "-",
                    "Bear": "-",
                    "Bear %": "-",
                    "Bear Dir": "-",
                }
            )
            continue

        bull_sym, bull_val, bear_sym, bear_val = res

        bull_dir = "↑" if bull_val >= 0 else "↓"
        bear_dir = "↓" if bear_val <= 0 else "↑"

        results.append(
            {
                "TF": label,
                "Bull": bull_sym,
                "Bull %": bull_val,
                "Bull Dir": bull_dir,
                "Bear": bear_sym,
                "Bear %": bear_val,
                "Bear Dir": bear_dir,
            }
        )

    return results


def render_dominance_table(df: pd.DataFrame, symbols=None):
    """
    Render the multi-TF dominance table in Streamlit.

    df must contain:
      - timestamp (datetime)
      - symbol (str)
      - vdelta_eff (float; signed flow metric)
    """
    import streamlit as st

    results = compute_multi_tf_table(df)
    table = pd.DataFrame(results)

    # Format percentage columns nicely
    for col in ["Bull %", "Bear %"]:
        table[col] = table[col].apply(
            lambda x: f"{x * 100:.1f}%" if isinstance(x, float) else x
        )

    st.subheader("Multi-Timeframe Dominance")
    st.dataframe(table, hide_index=True, use_container_width=True)
