from __future__ import annotations
from typing import Sequence, Optional, Iterable
import pandas as pd
import numpy as np

try:
    import talib as ta
    HAVE_TALIB = True
except Exception:
    HAVE_TALIB = False

def _to_series(x: Iterable) -> pd.Series:
    if isinstance(x, pd.Series):
        return pd.Series(x, dtype="float64")
    return pd.Series(list(map(float, x)), dtype="float64")

def ema(series: Sequence[float], period: int) -> pd.Series:
    s = _to_series(series)
    if HAVE_TALIB:
        out = ta.EMA(s.to_numpy(dtype="float64"), timeperiod=period)
        return pd.Series(out, index=s.index, dtype="float64")
    # fallback TA-style
    return s.ewm(span=period, adjust=False, min_periods=period).mean()

def exponential_moving_average(data: Sequence[float], period: int) -> Optional[float]:
    s = ema(_to_series(data), period)
    if s.empty or pd.isna(s.iloc[-1]):
        return None
    return float(s.iloc[-1])
