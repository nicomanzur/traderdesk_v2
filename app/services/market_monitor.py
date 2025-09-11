# app/services/market_monitor.py
from __future__ import annotations

import os
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

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
RTH_END: str   = os.getenv("RTH_END", "16:15")  # CME equity index suele cerrar 16:15
NY = ZoneInfo("America/New_York")

# Vela parcial para la semilla (histórico)
INCLUDE_PARTIAL_SEED: bool = os.getenv("INCLUDE_PARTIAL_BARS", "false").lower() in ("1", "true", "yes")

# Recalcular en cada cierre para “pixel match”
EXACT_MATCH_ON_CLOSE: bool = os.getenv("EXACT_MATCH_ON_CLOSE", "true").lower() in ("1", "true", "yes")

# Suavizado de EMA200 para mostrar (no para bias/señal)
EMA200_SMOOTH_TYPE: str = os.getenv("EMA200_SMOOTH_TYPE", "").lower()   # "sma" o vacío
EMA200_SMOOTH_LENGTH: int = int(os.getenv("EMA200_SMOOTH_LENGTH", "9"))

# Tolerancia en cruce EMA50 (en puntos). 0.0 = cruce estricto
EPS: float = float(os.getenv("EMA_CROSS_EPS", "0.0"))

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
    as_of: str         # ISO UTC (última vela cerrada conocida)
    close: float
    ema50: float               # valor base (no suavizado)
    ema200: float              # mostrado (suavizado opcional)
    color: str                 # "green" | "yellow" | "red" | "gray"
    signal: Optional[str]      # "LONG" | "SHORT" | None
    bars: int                  # cantidad de barras acumuladas en el estado
    message: str = ""          # texto auxiliar (p.ej. "ok")

    @property
    def label(self) -> str:
        return f"{self.symbol} · {self.contract_id}" if self.contract_id else self.symbol

# ==============================
# Utils
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

# EMA util: intentar usar tu indicador unificado si existe (respeta tu configuración)
try:
    from ..indicators.ema import ema as ema_ind  # type: ignore
except Exception:
    def ema_ind(series: pd.Series, n: int, mode: str | None = None) -> pd.Series:
        # fallback a pandas ewm (ajustado, min_periods=n)
        return pd.Series(series, dtype="float64").ewm(span=n, adjust=False, min_periods=n).mean()

def _color_from_zone(prev_close: float, prev_e50: float, prev_e200: float,
                     curr_close: float, curr_e50: float, curr_e200: float) -> str:
    # Verde si la vela previa cerró dentro (entre e50/e200). Amarillo si no estaba y ahora está dentro. Rojo si no.
    low_prev, high_prev = (min(prev_e50, prev_e200), max(prev_e50, prev_e200))
    low_curr, high_curr = (min(curr_e50, curr_e200), max(curr_e50, curr_e200))
    prev_in = (low_prev <= prev_close <= high_prev)
    curr_in = (low_curr <= curr_close <= high_curr)
    if prev_in:
        return "green"
    if (not prev_in) and curr_in:
        return "yellow"
    return "red"

def _signal_cross50_with_bias(prev_close: float, prev_e50: float,
                              curr_close: float, curr_e50: float,
                              curr_e200_base: float) -> Optional[str]:
    """
    Estrategia simplificada (tu definición):
    - Bias LONG si EMA50 > EMA200 (base).
      Señal LONG si prev_close < EMA50 (prev) y curr_close > EMA50 (curr).
    - Bias SHORT si EMA50 < EMA200 (base).
      Señal SHORT si prev_close > EMA50 (prev) y curr_close < EMA50 (curr).
    Tolerancia EPS opcional en el cruce.
    """
    # Bias por EMAs actuales (BASE)
    if (curr_e50 - curr_e200_base) > EPS:
        # LONG bias
        if (prev_close < (prev_e50 - EPS)) and (curr_close > (curr_e50 + EPS)):
            return "LONG"
    elif (curr_e200_base - curr_e50) > EPS:
        # SHORT bias
        if (prev_close > (prev_e50 + EPS)) and (curr_close < (curr_e50 - EPS)):
            return "SHORT"
    return None

# ==============================
# Monitor de mercado
# ==============================
class MarketMonitor:
    """
    - Seed con histórico (>=205 velas) -> fija EMA50/EMA200 y prev/curr.
    - Luego consulta SOLO la última vela cerrada; si hay nueva, avanza EMA con α=2/(n+1).
    - En el cierre de cada vela (si EXACT_MATCH_ON_CLOSE=True) recalcula el tail
      de histórico reciente para “pixel match” con la plataforma.
    - Si EMA200_SMOOTH_TYPE="sma" y EMA200_SMOOTH_LENGTH>1, el valor mostrado de EMA200
      es SMA(k) sobre la EMA200 base. Señales/bias usan las EMAs BASE.
    """

    def __init__(self, symbol_or_contract: str, px: Optional[ProjectXClient] = None) -> None:
        self.sym_raw = symbol_or_contract.strip().upper()
        self.px = px or ProjectXClient()
        if not getattr(self.px, "_token", None):
            self.px.login_with_key()

        # Resolver contractId
        self.contract_id: Optional[str] = self._resolve_contract_id(self.sym_raw)

        # Estado incremental
        self.state = {
            "seeded": False,
            "bars": 0,                 # barras en el dataset que respalda las EMAs
            # prev bar (base):
            "prev_ts": None,
            "prev_close": None,
            "prev_e50": None,
            "prev_e200_base": None,    # base
            "prev_e200": None,         # mostrado (suavizado o base)
            # curr bar (última cerrada conocida, base):
            "curr_ts": None,
            "curr_close": None,
            "curr_e50": None,
            "curr_e200_base": None,    # base
            "curr_e200": None,         # mostrado (suavizado o base)
            # buffer para suavizado EMA200 (valores base recientes)
            "ema200_buf": [],          # list[float], máx EMA200_SMOOTH_LENGTH
        }

        # Coeficientes de EMA base
        self.alpha50  = 2.0 / (50.0 + 1.0)
        self.alpha200 = 2.0 / (200.0 + 1.0)

    # -------- Resolver contrato ----------
    def _resolve_contract_id(self, sym: str) -> Optional[str]:
        # Si ya viene como contractId
        if sym.startswith("CON."):
            return sym

        # Mapear por ENV directo si es MNQ o ES
        if sym in ("MNQ", "NQ") and ENV_CONTRACT_MNQ:
            return ENV_CONTRACT_MNQ
        if sym in ("ES", "SP", "SP500") and ENV_CONTRACT_ES:
            return ENV_CONTRACT_ES

        # Buscar por texto si no hay env (probamos sinónimos)
        texts = SYNONYMS.get(sym, [sym])
        live_flag = True if FORCE_LIVE else False

        for t in texts:
            try:
                contracts = self.px.search_contracts(t, live=live_flag)
                if not contracts:
                    continue
                # activo (front) si hay
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

        # Filtrar RTH si corresponde
        if CHART_SESSION == "RTH" and not df.empty:
            local = df["datetime"].dt.tz_convert(NY)
            hm = local.dt.strftime("%H:%M")
            mask = (hm >= RTH_START) & (hm <= RTH_END)
            df = df[mask].reset_index(drop=True)

        n = len(df)
        if n < REQUIRED_BARS:
            return False, f"Datos insuficientes {n}/{REQUIRED_BARS} velas {BAR_MINUTES}m"

        # Calcular EMAs base
        df["ema50"]        = ema_ind(df["close"], 50)
        df["ema200_base"]  = ema_ind(df["close"], 200)

        # EMA200 mostrada (suavizada opcionalmente)
        if EMA200_SMOOTH_TYPE == "sma" and EMA200_SMOOTH_LENGTH > 1:
            df["ema200"] = df["ema200_base"].rolling(EMA200_SMOOTH_LENGTH).mean()
        else:
            df["ema200"] = df["ema200_base"]

        if pd.isna(df["ema200"].iloc[-1]) or pd.isna(df["ema50"].iloc[-1]):
            return False, "EMAs aún NaN tras semilla (aumentar lookback)"

        # fijar prev/curr y buffer de suavizado
        prev, curr = df.iloc[-2], df.iloc[-1]
        buf_len = max(1, EMA200_SMOOTH_LENGTH if (EMA200_SMOOTH_TYPE == "sma") else 1)
        ema200_buf = df["ema200_base"].tail(buf_len).tolist()

        self.state.update({
            "seeded": True,
            "bars": n,
            "prev_ts": prev["datetime"].to_pydatetime(),
            "prev_close": float(prev["close"]),
            "prev_e50": float(prev["ema50"]),
            "prev_e200_base": float(prev["ema200_base"]),
            "prev_e200": float(prev["ema200"]),
            "curr_ts": curr["datetime"].to_pydatetime(),
            "curr_close": float(curr["close"]),
            "curr_e50": float(curr["ema50"]),
            "curr_e200_base": float(curr["ema200_base"]),
            "curr_e200": float(curr["ema200"]),
            "ema200_buf": ema200_buf,
        })
        return True, "ok"

    # -------- Recalcular tail en cierre (match exacto) ----------
    def _recalc_tail(self, contract_id: str, tail_bars: int = 1200) -> Tuple[bool, str]:
        """
        Recalcula EMA50/EMA200 desde ~tail_bars velas cerradas recientes.
        Se usa en el cierre de cada vela para “pixel match” con la plataforma.
        """
        live_flag = True if FORCE_LIVE else False
        bars = self.px.retrieve_bars(
            contract_id=contract_id,
            live=live_flag,
            unit=2,
            unit_number=BAR_MINUTES,
            include_partial=False,       # solo cerradas
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
            df["ema200"] = df["ema200_base"].rolling(EMA200_SMOOTH_LENGTH).mean()
        else:
            df["ema200"] = df["ema200_base"]

        prev, curr = df.iloc[-2], df.iloc[-1]
        buf_len = max(1, EMA200_SMOOTH_LENGTH if (EMA200_SMOOTH_TYPE == "sma") else 1)
        ema200_buf = df["ema200_base"].tail(buf_len).tolist()

        self.state.update({
            "seeded": True,
            "bars": n,
            "prev_ts":   prev["datetime"].to_pydatetime(),
            "prev_close": float(prev["close"]),
            "prev_e50":   float(prev["ema50"]),
            "prev_e200_base":  float(prev["ema200_base"]),
            "prev_e200":       float(prev["ema200"]),
            "curr_ts":    curr["datetime"].to_pydatetime(),
            "curr_close": float(curr["close"]),
            "curr_e50":   float(curr["ema50"]),
            "curr_e200_base":  float(curr["ema200_base"]),
            "curr_e200":       float(curr["ema200"]),
            "ema200_buf": ema200_buf,
        })
        return True, "ok"

    # -------- Obtener última vela cerrada ----------
    def _get_last_closed_bar(self, contract_id: str) -> Optional[Tuple[pd.Timestamp, float]]:
        live_flag = True if FORCE_LIVE else False
        bars = self.px.retrieve_bars(
            contract_id=contract_id,
            live=live_flag,
            unit=2,
            unit_number=BAR_MINUTES,
            include_partial=False,   # SOLO cerradas
            limit=2,
            lookback_days=2,
        )
        df = _bars_to_df(bars)
        if df.empty:
            return None
        last = df.iloc[-1]
        ts = pd.to_datetime(last["datetime"], utc=True)
        if not _in_rth(ts):
            return None
        return ts, float(last["close"])

    # -------- Avanzar EMA incremental ----------
    def _advance_incremental(self, ts: pd.Timestamp, close: float) -> None:
        st = self.state
        if st["curr_ts"] is None or ts > st["curr_ts"]:
            # mover curr -> prev (valores BASE y mostrados)
            st["prev_ts"]         = st["curr_ts"]
            st["prev_close"]      = st["curr_close"]
            st["prev_e50"]        = st["curr_e50"]
            st["prev_e200_base"]  = st["curr_e200_base"]
            st["prev_e200"]       = st["curr_e200"]

            # calcular nueva EMA base
            prev_e50  = st["prev_e50"]
            prev_e200_base = st["prev_e200_base"]
            if prev_e50 is None or prev_e200_base is None:
                return

            new_e50_base  = prev_e50  + self.alpha50  * (close - prev_e50)
            new_e200_base = prev_e200_base + self.alpha200 * (close - prev_e200_base)

            # suavizado opcional para mostrar EMA200
            if EMA200_SMOOTH_TYPE == "sma" and EMA200_SMOOTH_LENGTH > 1:
                buf = st.get("ema200_buf", [])
                buf.append(float(new_e200_base))
                if len(buf) > EMA200_SMOOTH_LENGTH:
                    buf.pop(0)
                st["ema200_buf"] = buf
                new_e200_shown = sum(buf) / len(buf)
            else:
                new_e200_shown = float(new_e200_base)

            # set curr
            st["curr_ts"]        = ts
            st["curr_close"]     = close
            st["curr_e50"]       = float(new_e50_base)
            st["curr_e200_base"] = float(new_e200_base)
            st["curr_e200"]      = float(new_e200_shown)
            st["bars"]           = int(st.get("bars", 0)) + 1

    # -------- API pública ----------
    def get_snapshot(self) -> Tuple[Optional[Snapshot], str]:
        """
        Devuelve (Snapshot|None, mensaje).
        - None si no hay contrato o no se pudo semillar.
        - Si no hay bar nueva, devuelve el último snapshot conocido (curr).
        """
        if not self.contract_id:
            return None, "Sin contractId (revisá .env o permisos de datos)"

        # Seed si falta
        if not self.state["seeded"]:
            ok, msg = self._seed_from_history(self.contract_id)
            if not ok:
                return None, msg

        # Buscar última vela cerrada
        last = self._get_last_closed_bar(self.contract_id)
        if last is not None:
            ts, close = last
            st = self.state
            if st["curr_ts"] is not None:
                gap_s = (ts - st["curr_ts"]).total_seconds()
                if gap_s > 20 * 60:  # gap grande: re-seed
                    ok, msg = self._seed_from_history(self.contract_id)
                    if not ok:
                        return None, msg
                elif ts > st["curr_ts"]:
                    # Avance rápido
                    self._advance_incremental(ts, close)
                    # En el cierre, recalcular tail para match exacto (opcional)
                    if EXACT_MATCH_ON_CLOSE:
                        ok2, msg2 = self._recalc_tail(self.contract_id, tail_bars=1200)
                        if not ok2:
                            print("[MarketMonitor][WARN] Recalc tail falló:", msg2)
            else:
                ok, msg = self._seed_from_history(self.contract_id)
                if not ok:
                    return None, msg

        st = self.state
        if st["curr_ts"] is None or st["curr_e50"] is None or st["curr_e200"] is None:
            return None, "Aún sin snapshot actual"

        # Color (visual; no afecta señal)
        color  = "gray"
        if None not in (st["prev_close"], st["prev_e50"], st["prev_e200_base"],
                        st["curr_close"], st["curr_e50"], st["curr_e200_base"]):
            color  = _color_from_zone(st["prev_close"], st["prev_e50"], st["prev_e200_base"],
                                      st["curr_close"], st["curr_e50"], st["curr_e200_base"])

        # Señal con tu regla (bias por EMA50 vs EMA200 base; cruce cierre vs EMA50)
        signal = None
        if None not in (st["prev_close"], st["prev_e50"], st["curr_close"], st["curr_e50"], st["curr_e200_base"]):
            signal = _signal_cross50_with_bias(
                prev_close=st["prev_close"],
                prev_e50=st["prev_e50"],
                curr_close=st["curr_close"],
                curr_e50=st["curr_e50"],
                curr_e200_base=st["curr_e200_base"],
            )

        snap = Snapshot(
            symbol=self.sym_raw,
            contract_id=self.contract_id,
            as_of=st["curr_ts"].replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z"),
            close=float(st["curr_close"]),
            ema50=float(st["curr_e50"]),        # base (no suavizada)
            ema200=float(st["curr_e200"]),      # mostrada (suavizada si aplica)
            color=color if color in ("green", "yellow", "red") else "gray",
            signal=signal,
            bars=int(st["bars"]),
            message="ok",
        )
        return snap, "ok"

    # -------- Diagnóstico extendido ----------
    def get_debug_state(self) -> dict:
        """Devuelve todos los valores que intervienen en la señal y por qué no disparó."""
        st = self.state
        out = {
            "seeded": st.get("seeded", False),
            "bars": st.get("bars"),
            "prev_ts": st.get("prev_ts"),
            "curr_ts": st.get("curr_ts"),
            "prev_close": st.get("prev_close"),
            "curr_close": st.get("curr_close"),
            "prev_e50": st.get("prev_e50"),
            "prev_e200_base": st.get("prev_e200_base"),
            "curr_e50": st.get("curr_e50"),
            "curr_e200_base": st.get("curr_e200_base"),
            "color": None,
            "signal": None,
            "conds": {}
        }
        try:
            # Color / bias / cruces
            if None not in (st["prev_close"], st["prev_e50"], st["prev_e200_base"],
                            st["curr_close"], st["curr_e50"], st["curr_e200_base"]):
                prev_close = st["prev_close"]; curr_close = st["curr_close"]
                prev_e50 = st["prev_e50"]; prev_e200 = st["prev_e200_base"]
                curr_e50 = st["curr_e50"]; curr_e200 = st["curr_e200_base"]

                # color
                low_prev, high_prev = (min(prev_e50, prev_e200), max(prev_e50, prev_e200))
                low_curr, high_curr = (min(curr_e50, curr_e200), max(curr_e50, curr_e200))
                prev_in = (low_prev <= prev_close <= high_prev)
                curr_in = (low_curr <= curr_close <= high_curr)
                out["color"] = "green" if prev_in else ("yellow" if (not prev_in and curr_in) else "red")

                # bias / cruces
                bias = "FLAT"
                if curr_e50 > curr_e200 + EPS: bias = "LONG"
                elif curr_e200 > curr_e50 + EPS: bias = "SHORT"

                cross_up   = (prev_close < (prev_e50 - EPS)) and (curr_close > (curr_e50 + EPS))
                cross_down = (prev_close > (prev_e50 + EPS)) and (curr_close < (curr_e50 - EPS))

                sig = None
                if bias == "LONG" and cross_up: sig = "LONG"
                if bias == "SHORT" and cross_down: sig = "SHORT"

                out["signal"] = sig
                out["conds"] = {
                    "bias": bias,
                    "cross_up": cross_up,
                    "cross_down": cross_down,
                }
        except Exception as e:
            out["error"] = f"debug_state: {e}"
        return out
