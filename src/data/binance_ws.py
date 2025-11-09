# src/data/binance_ws.py
from __future__ import annotations
import json
import threading
import time
from collections import deque, defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, Deque, Tuple, List, Optional
from urllib.parse import quote

import pandas as pd
import websocket  # from websocket-client


def _combined_stream_url(symbols: List[str]) -> str:
    # Binance wants lowercase pairs. Example: btcusdt@trade
    streams = "/".join(f"{s.lower()}@trade" for s in symbols)
    return f"wss://stream.binance.com:9443/stream?streams={quote(streams)}"


class TradeCollector:
    """
    Live trade collector for Binance Spot.
    Computes signed vDelta per symbol using trade 'm' flag:
      m == False -> taker is buyer -> +qty
      m == True  -> taker is seller -> -qty
    Keeps a rolling buffer of trades. Snapshot sums the last N seconds.
    """

    def __init__(self, window_seconds: int = 300):
        self.window_seconds = int(window_seconds)  # 5m default
        self._ws: Optional[websocket.WebSocketApp] = None
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.RLock()

        # Per-symbol ring buffer: deque[(ts_ms, qty_signed, notional_signed)]
        self._trades: Dict[str, Deque[Tuple[int, float, float]]] = defaultdict(deque)
        # Last known price per symbol
        self._last_price: Dict[str, float] = {}

        self._running = False
        self._symbols: List[str] = []

    # ---- public API ---------------------------------------------------------

    def start(self, symbols: List[str]) -> None:
        """Start WS for given symbols. Safe to call again with same symbols."""
        symbols = [s.strip().upper() for s in symbols if s.strip()]
        if not symbols:
            raise ValueError("symbols must be non-empty")

        with self._lock:
            if self._running:
                # If symbols changed, restart
                if set(symbols) != set(self._symbols):
                    self.stop()
                else:
                    return  # already running on same set
            self._symbols = symbols
            self._running = True

        url = _combined_stream_url(symbols)

        def on_message(ws, message: str):
            try:
                payload = json.loads(message)
                data = payload.get("data", {})
                # Binance trade fields:
                #  t: trade time (ms)
                #  p: price (string)
                #  q: quantity (string)
                #  m: buyer is maker (bool) -> taker is seller when True
                ts_ms = int(data.get("T") or data.get("t"))
                price = float(data["p"])
                qty = float(data["q"])
                buyer_is_maker = bool(data["m"])
                # stream name carries symbol: e.g., "btcusdt@trade"
                stream = payload.get("stream", "")
                symbol = stream.split("@", 1)[0].upper()

                # Sign:
                # taker = buyer when m == False -> +qty
                # taker = seller when m == True  -> -qty
                signed_qty = qty if not buyer_is_maker else -qty
                signed_notional = signed_qty * price

                with self._lock:
                    self._last_price[symbol] = price
                    dq = self._trades[symbol]
                    dq.append((ts_ms, signed_qty, signed_notional))
                    self._prune_locked(symbol, now_ms=ts_ms)
            except Exception:
                # swallow parsing errors to keep stream alive
                return

        def on_error(ws, err):
            # Keep minimal. Streamlit can show errors from caller if needed.
            pass

        def on_close(ws, code, reason):
            with self._lock:
                self._running = False

        def run():
            self._ws = websocket.WebSocketApp(
                url,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            try:
                self._ws.run_forever(ping_interval=20, ping_timeout=10)
            finally:
                with self._lock:
                    self._running = False

        self._thread = threading.Thread(target=run, name="binance-ws", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        with self._lock:
            self._running = False
            ws = self._ws
            self._ws = None
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass
        t = self._thread
        if t is not None and t.is_alive():
            t.join(timeout=2.0)

    def snapshot(self, now: Optional[datetime] = None) -> pd.DataFrame:
        """
        Return per-symbol vDelta snapshot for the last 'window_seconds'.
        Columns: symbol, vdelta_qty, vdelta_notional, last_price, last_ts
        """
        if now is None:
            now = datetime.now(timezone.utc)
        now_ms = int(now.timestamp() * 1000)
        cutoff_ms = now_ms - self.window_seconds * 1000

        rows = []
        with self._lock:
            for sym, dq in self._trades.items():
                # prune and sum
                self._prune_locked(sym, now_ms=now_ms)
                vq = 0.0
                vn = 0.0
                last_ts = None
                for ts_ms, sq, sn in dq:
                    if ts_ms >= cutoff_ms:
                        vq += sq
                        vn += sn
                        last_ts = ts_ms if last_ts is None or ts_ms > last_ts else last_ts
                rows.append(
                    {
                        "symbol": sym,
                        "vdelta_qty": vq,
                        "vdelta_notional": vn,
                        "last_price": self._last_price.get(sym),
                        "last_ts": pd.to_datetime(last_ts, unit="ms", utc=True) if last_ts else pd.NaT,
                    }
                )

        if not rows:
            return pd.DataFrame(columns=["symbol", "vdelta_qty", "vdelta_notional", "last_price", "last_ts"])
        df = pd.DataFrame(rows)
        # stable order by absolute notional desc
        df = df.sort_values("vdelta_notional", key=lambda s: s.abs(), ascending=False, na_position="last")
        return df.reset_index(drop=True)

    # ---- helpers ------------------------------------------------------------

    def _prune_locked(self, symbol: str, now_ms: int) -> None:
        """Remove old trades outside the window for a symbol. lock held by caller."""
        cutoff = now_ms - self.window_seconds * 1000
        dq = self._trades[symbol]
        while dq and dq[0][0] < cutoff:
            dq.popleft()
