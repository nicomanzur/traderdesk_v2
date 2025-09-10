# app/services/market_monitor.py
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
import math
import os

import pandas as pd
from zoneinfo import ZoneInfo

from app.brokers.projectx_api import ProjectXClient

# ==============================
# Config por ENV (con defaults)
# ==============================
BAR_MINUTES: int = int(os.getenv("BAR_MINUTES", "15"))
REQUIRED_BARS: int = int(os.getenv("REQUIRED_BARS", "205"))   # EMA200 + margen
WARMUP_BARS: int = int(os.getenv("WARMUP_BARS", "1000"))      # semilla robusta
DEFAULT_LOOKBACK_DAYS: int = int(os.getenv("BARS_LOOKBACK_DAYS", "14"))
FORCE_LIVE: bool = os.getenv("FORCE_LIVE", "").lower() in ("1", "true", "yes")

# Sesión (ETH o RTH)
CHART_SESSION: str = os.getenv("CHART_SESSION", "ETH").upper()
RTH_START: str = os.getenv("RTH_START", "09:30")
RTH_END: str   = os.getenv("RTH_END", "16:15")
NY = ZoneInfo("America/New_York")

# Vela parcial para la semilla (histórico)
INCLUDE_PARTIAL_SEED: bool = os.getenv("INCLUDE_PARTIAL_BARS", "false").lower() in ("1", "true", "yes")

# Recalcular en cierre para “pixel match”
EXACT_MATCH_ON_CLOSE: bool = os.getenv("EXACT_MATCH_ON_CLOSE", "true").lower() in ("1", "true", "yes")

# Suavizado opcional de EMA200 (para mostrar; la lógica NO lo usa)
EMA200_SMOOTH_TYPE: str = os.getenv("EMA200_SMOOTH_TYPE", "").lower()   # "sma" o vacío
EMA200_SMOOTH_LENGTH: int = int(os.getenv("EMA200_SMOOTH_LENGTH", "9"))

# Resetear armado al cambiar la tendencia (recomendado)
ARM_RESET_ON_TREND_CHANGE: bool = os.getenv("ARM_RESET_ON_TREND_CHANGE", "true").lower() in ("1","true","yes")

# Contract IDs por ENV
ENV_CONTRACT_MNQ = (
    os.getenv("CONTRACT_ID_MNQ", "").strip()
    or os.getenv("CONTRACT_ID_NQ", "").strip()   # compat.
)
ENV_CONTRACT_ES = os.getenv("CONTRACT_ID_ES", "").strip()

SYNONYMS: Dict[str, List[str]] = {
    "MNQ": ["MNQ", "MICRO NASDAQ", "MICRO E-MINI NASDAQ", "NQ", "E-MINI NASDAQ"],
    "ES":  ["ES", "E-mini S&P", "S&P 500", "E-MINI S&P"],
}

# ==============================
# Snapshot para la GUI / Trader
# ==============================
@dataclass
class Snapshot:
    symbol: str
    contract_id: str
    as_of: str         # ISO UTC (última vela cerrada)
    close: float
    ema50: float               # valor base (sin suavizado)
    ema200: float              # mostrado (puede estar suavizado visualmente)
    trend: str                 # "LONG" | "SHORT" (según EMA50 vs EMA200)
    armed: bool                # si hay pullback visto contra EMA50 en la dirección de la tendencia
    color: str                 # "green" | "yellow" | "red" | "gray" (solo decorativo)
    signal: Optional[str]      # "LONG" | "SHORT" | None
    bars: int                  # cantidad de barras usadas
    message: str = ""          # texto auxiliar

    @property
    def label(self) -> str:
        return f"{self.symbol} · {self.contract_id}" if self.contract_id else self.symbol


# ==============================
# Utiles
# ==============================
def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def _needed_days(required_bars: int = REQUIRED_BARS,
                 bar_minutes: int = BAR_MINUTES,
                 buffer_days: int = 4) -> int:
    days = math.ceil(required_bars * bar_minutes / (24 * 60))
    return max(days + buffer_days, 7)

def _in_rth(ts_utc: pd.Timestamp) -> bool:
    if CHART_SESSION != "RTH":
        return True
    ts_ny = ts_utc.tz_convert(NY)
    hm = f"{ts_ny.hour:02d}:{ts_ny.minute:02d}"
    return (RTH_START <= hm <= RTH_END)

def _bars_to_df(bars: List[Dict]) -> pd.DataFrame:
    if not bars:
        return pd.DataFrame()
    df = pd.DataFrame(bars).rename(columns={
        "t": "datetime", "o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"
    })
    df["datetime"] = pd.to_datetime(df["datetime"], utc=True)
    return df.sort_values("datetime").reset_index(drop=True)

# EMA util: intenta usar tu indicador unificado si existe (respeta EMA_MODE)
try:
    from ..indicators.ema import ema as ema_ind  # type: ignore
except Exception:
    def ema_ind(series: pd.Series, n: int, mode: str | None = None) -> pd.Series:
        # fallback a pandas (recursivo; adjust=False)
        return pd.Series(series, dtype="float64").ewm(span=n, adjust=False, min_periods=n).mean()

def _color_from_zone(prev_close: float, prev_e50: float, prev_e200: float,
                     curr_close: float, curr_e50: float, curr_e200: float) -> str:
    """Decorativo para GUI (verde/amarillo/rojo) – NO afecta señal."""
    low_prev, high_prev = (min(prev_e50, prev_e200), max(prev_e50, prev_e200))
    low_curr, high_curr = (min(curr_e50, curr_e200), max(curr_e50, curr_e200))
    prev_in = (low_prev <= prev_close <= high_prev)
    curr_in = (low_curr <= curr_close <= high_curr)
    if prev_in:
        return "green"
    if (not prev_in) and curr_in:
        return "yellow"
    return "red"


# ==============================
# Monitor de mercado (pullback sobre EMA50)
# ==============================
class MarketMonitor:
    """
    Nueva lógica de señal (lo que pediste):
      - Tendencia LONG si EMA50 > EMA200; SHORT si EMA50 < EMA200.
      - 'Armar' cuando el cierre queda al lado contrario de EMA50 (pullback).
      - Disparar señal cuando vuelve a cerrar del lado de la tendencia respecto de EMA50.
      - Reset de armado al cambiar la tendencia.
    """

    def __init__(self, symbol_or_contract: str, px: Optional[ProjectXClient] = None) -> None:
        self.sym_raw = symbol_or_contract.strip().upper()
        self.px = px or ProjectXClient()
        if not self.px._token:
            self.px.login_with_key()

        # Resolver contractId
        self.contract_id: Optional[str] = self._resolve_contract_id(self.sym_raw)

        # Estado incremental
        self.state = {
            "seeded": False,
            "bars": 0,
            # prev bar (BASE)
            "prev_ts": None,
            "prev_close": None,
            "prev_e50": None,
            "prev_e200_base": None,
            # curr bar (BASE)
            "curr_ts": None,
            "curr_close": None,
            "curr_e50": None,
            "curr_e200_base": None,
            # mostrado (EMA200 suavizada opcional)
            "curr_e200_shown": None,
            # armado y tendencia actual
            "trend": None,          # "LONG" | "SHORT"
            "armed_dir": None,      # None | "LONG" | "SHORT"
        }

        # Coeficientes de EMA base
        self.alpha50  = 2.0 / (50.0 + 1.0)
        self.alpha200 = 2.0 / (200.0 + 1.0)

    # -------- Resolver contrato ----------
    def _resolve_contract_id(self, sym: str) -> Optional[str]:
        if sym.startswith("CON."):
            return sym
        if sym in ("MNQ", "NQ") and ENV_CONTRACT_MNQ:
            return ENV_CONTRACT_MNQ
        if sym in ("ES", "SP", "SP500") and ENV_CONTRACT_ES:
            return ENV_CONTRACT_ES

        texts = SYNONYMS.get(sym, [sym])
        live_flag = True if FORCE_LIVE else False
        for t in texts:
            try:
                contracts = self.px.search_contracts(t, live=live_flag)
                if not contracts:
                    continue
                active = [c for c in contracts if c.get("activeContract")] or contracts
                cand = active[0]
                if cand and cand.get("id"):
                    return cand["id"]
            except Exception:
                continue
        return None

    # -------- Seed: histórico suficiente ----------
    def _seed_from_history(self, contract_id: str) -> Tuple[bool, str]:
        lookback_days = int(os.getenv("BARS_LOOKBACK_DAYS", str(DEFAULT_LOOKBACK_DAYS)) or DEFAULT_LOOKBACK_DAYS)
        limit = max(WARMUP_BARS, REQUIRED_BARS + 200)
        live_flag = True if FORCE_LIVE else False

        bars = self.px.retrieve_bars(
            contract_id=contract_id,
            live=live_flag,
            unit=2,
            unit_number=BAR_MINUTES,
            include_partial=INCLUDE_PARTIAL_SEED,
            limit=limit,
            lookback_days=max(lookback_days, _needed_days()),
        )
        df = _bars_to_df(bars)

        if CHART_SESSION == "RTH" and not df.empty:
            local = df["datetime"].dt.tz_convert(NY)
            hm = local.dt.strftime("%H:%M")
            mask = (hm >= RTH_START) & (hm <= RTH_END)
            df = df[mask].reset_index(drop=True)

        n = len(df)
        if n < REQUIRED_BARS:
            return False, f"Datos insuficientes {n}/{REQUIRED_BARS} velas {BAR_MINUTES}m"

        # EMAs base
        df["ema50"]        = ema_ind(df["close"], 50)
        df["ema200_base"]  = ema_ind(df["close"], 200)

        # EMA200 mostrada (suavizada opcional)
        if EMA200_SMOOTH_TYPE == "sma" and EMA200_SMOOTH_LENGTH > 1:
            df["ema200_shown"] = df["ema200_base"].rolling(EMA200_SMOOTH_LENGTH).mean()
        else:
            df["ema200_shown"] = df["ema200_base"]

        if pd.isna(df["ema200_base"].iloc[-1]) or pd.isna(df["ema50"].iloc[-1]):
            return False, "EMA50/EMA200 aún NaN tras semilla (aumentar lookback)"

        prev, curr = df.iloc[-2], df.iloc[-1]

        # Tendencia actual por EMAs (base)
        trend = "LONG" if curr["ema50"] > curr["ema200_base"] else "SHORT"

        # Armado inicial según cierre vs EMA50
        armed_dir = None
        if trend == "LONG" and curr["close"] < curr["ema50"]:
            armed_dir = "LONG"
        elif trend == "SHORT" and curr["close"] > curr["ema50"]:
            armed_dir = "SHORT"

        self.state.update({
            "seeded": True,
            "bars": n,
            "prev_ts": prev["datetime"].to_pydatetime(),
            "prev_close": float(prev["close"]),
            "prev_e50": float(prev["ema50"]),
            "prev_e200_base": float(prev["ema200_base"]),
            "curr_ts": curr["datetime"].to_pydatetime(),
            "curr_close": float(curr["close"]),
            "curr_e50": float(curr["ema50"]),
            "curr_e200_base": float(curr["ema200_base"]),
            "curr_e200_shown": float(curr["ema200_shown"]),
            "trend": trend,
            "armed_dir": armed_dir,
        })
        return True, "ok"

    # -------- Recalcular tail en cierre (pixel match) ----------
    def _recalc_tail(self, contract_id: str, tail_bars: int = 1200) -> Tuple[bool, str]:
        live_flag = True if FORCE_LIVE else False
        bars = self.px.retrieve_bars(
            contract_id=contract_id,
            live=live_flag,
            unit=2,
            unit_number=BAR_MINUTES,
            include_partial=False,
            limit=tail_bars,
            lookback_days=max(DEFAULT_LOOKBACK_DAYS, 30),
        )
        df = _bars_to_df(bars)
        if df.empty:
            return False, "Sin barras para recalcular"

        if CHART_SESSION == "RTH":
            local = df["datetime"].dt.tz_convert(NY)
            hm = local.dt.strftime("%H:%M")
            mask = (hm >= RTH_START) & (hm <= RTH_END)
            df = df[mask].reset_index(drop=True)

        n = len(df)
        if n < REQUIRED_BARS:
            return False, f"Datos insuficientes {n}/{REQUIRED_BARS} tras recalcular"

        df["ema50"]        = ema_ind(df["close"], 50)
        df["ema200_base"]  = ema_ind(df["close"], 200)
        if EMA200_SMOOTH_TYPE == "sma" and EMA200_SMOOTH_LENGTH > 1:
            df["ema200_shown"] = df["ema200_base"].rolling(EMA200_SMOOTH_LENGTH).mean()
        else:
            df["ema200_shown"] = df["ema200_base"]

        prev, curr = df.iloc[-2], df.iloc
