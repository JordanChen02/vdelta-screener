from __future__ import annotations

from typing import Dict, List

# Static placeholder market caps (USD). Swap to live API later
MCAP_USD: Dict[str, float] = {
    "BTCUSDT": 1_300_000_000_000.0,
    "ETHUSDT":   450_000_000_000.0,
    "SOLUSDT":    90_000_000_000.0,
    "ASTERUSDT":   2_000_000_000.0,
    "HYPEUSDT":      800_000_000.0,
    "PUMPUSDT":      500_000_000.0,
}

def symbol_to_usdt(sym: str) -> str:
    """
    Normalize a symbol to its USDT pair
    e.g. 'btc' -> 'BTCUSDT', 'ETHUSDT' -> 'ETHUSDT'
    """

    s = sym.strip().upper()
    return s if s.endswith("USDT") else f"{s}USDT"

def get_mcap_map(symbols: List[str]) -> Dict[str, float]:
    """
    Build a {SYMBOL:USDT: mcap} dict for a given list of symbols.
    Missing entries will map to None (i.e., key exists with value None).
    """
    return {symbol_to_usdt(s): MCAP_USD.get(symbol_to_usdt(s)) for s in symbols}

