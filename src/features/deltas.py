from __future__ import annotations
import pandas as pd

def compute_vdelta(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["vol_delta"]=pd.Series(dtype="float64")
    return df