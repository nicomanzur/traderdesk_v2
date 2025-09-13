# app/trading/signal_trader.py
import os
import json
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional

# Cargar .env si está disponible
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

from app.brokers.projectx_api import ProjectXClient
from app.services.market_monitor import MarketMonitor, Snapshot

# ---------- Helpers de ENV ----------
def env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name, "")
    if v == "":
        return default
    return v.lower() in ("1", "true", "yes", "on")

def env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)).strip())
    except Exception:
        return default

def env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)).strip())
    except Exception:
        return default

def env_csv_ints(name: str, default: str) -> set[int]:
    raw = os.getenv(name, default)
    out: set[int] = set()
    for part in raw.split(","):
        s = part.strip()
        if s == "":
            continue
        try:
            out.add(int(s))
        except:
            pass
    return out

def _iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

# ---------- Beep (Windows y fallback) ----------
def _beep():
    try:
        import winsound  # Solo Windows
        winsound.Beep(1000, 300)  # 1000 Hz, 300 ms
    except Exception:
        # Bell ASCII como fallback multiplataforma
        print("\a", end="", flush=True)

# ---------- Notificador (Telegram / Email vía SMTP) ----------
class Notifier:
    def __init__(self) -> None:
        # Telegram
        self.tg_enabled = env_bool("NOTIFY_TELEGRAM", False) and \
                          bool(os.getenv("TELEGRAM_BOT_TOKEN")) and \
                          bool(os.getenv("TELEGRAM_CHAT_ID"))
        # Email
        self.mail_enabled = env_bool("NOTIFY_EMAIL", False) and \
                            bool(os.getenv("SMTP_HOST")) and \
                            bool(os.getenv("SMTP_TO"))

    def send(self, text: str) -> None:
        self.telegram(text)
        self.email(text)

    def telegram(self, text: str) -> None:
        if not self.tg_enabled:
            return
        try:
            import requests
            token = os.getenv("TELEGRAM_BOT_TOKEN")
            chat_id = os.getenv("TELEGRAM_CHAT_ID")
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
            requests.post(url, json=payload, timeout=8)
        except Exception as e:
            print("[NOTIFY][TG][WARN]", e)

    def email(self, text: str) -> None:
        if not self.mail_enabled:
            return
        try:
            import smtplib, ssl
            host = os.getenv("SMTP_HOST")
            port = int(os.getenv("SMTP_PORT", "587"))
            user = os.getenv("SMTP_USER", "")
            pwd  = os.getenv("SMTP_PASS", "")
            to   = os.getenv("SMTP_TO")
            sender = os.getenv("SMTP_FROM", user or f"bot@{host}")
            msg = (
                f"From: {sender}\r\n"
                f"To: {to}\r\n"
                f"Subject: TraderDesk alert\r\n"
                f"\r\n{text}"
            )
            ctx = ssl.create_default_context()
            with smtplib.SMTP(host, port, timeout=10) as srv:
                srv.starttls(context=ctx)
                if user:
                    srv.login(user, pwd)
                srv.sendmail(sender, [addr.strip() for addr in to.split(",")], msg.encode("utf-8"))
        except Exception as e:
            print("[NOTIFY][EMAIL][WARN]", e)

# ---------- Config desde tu .env ----------
CHECK_MINUTES        = env_csv_ints("CHECK_MINUTES", "0,15,30,45")
CLOSE_LAG_SEC        = env_float("CLOSE_LAG_SEC", 1.0)           # margen post-cierre
CLOSE_RETRY_COUNT    = env_int("CLOSE_RETRY_COUNT", 10)          # reintentos si no llegó la vela
CLOSE_RETRY_INTERVAL = env_float("CLOSE_RETRY_INTERVAL", 0.5)

DRY_RUN       = env_bool("DRY_RUN", True)
TRADE_SYMBOLS = [s.strip().upper() for s in os.getenv("TRADE_SYMBOLS", "MNQ,ES").split(",") if s.strip()]

# ---------- Idempotencia por vela ----------
SEEN_FILE = os.getenv("SEEN_SIGNALS_FILE", "seen_signals.json")

def _load_seen() -> set[str]:
    try:
        with open(SEEN_FILE, "r", encoding="utf-8") as f:
            return set(json.load(f))
    except Exception:
        return set()

def _save_seen(seen: set[str]) -> None:
    try:
        with open(SEEN_FILE, "w", encoding="utf-8") as f:
            json.dump(sorted(list(seen)), f, ensure_ascii=False)
    except Exception as e:
        print("[SEEN][WARN]", e)

def _event_id(sym: str, as_of: str, signal: Optional[str]) -> str:
    return f"{sym}|{as_of}|{signal}"

# ---------- Timing: siguiente :00/:15/:30/:45 ----------
def _next_quarter(dt: datetime) -> datetime:
    m = dt.minute
    q = (m // 15) * 15
    base = dt.replace(second=0, microsecond=0)
    target = base.replace(minute=q)
    if base >= target:
        target = target + timedelta(minutes=15)
    return target

# ---------- Main ----------
def main():
    px = ProjectXClient()
    if not px._token:
        px.login_with_key()

    notifier = Notifier()

    # Un MarketMonitor por símbolo (comparte el ProjectXClient ya logueado)
    monitors: Dict[str, MarketMonitor] = {}
    for sym in TRADE_SYMBOLS:
        monitors[sym] = MarketMonitor(sym, px=px)

    # Para rastrear cambios (bias, señal y relación cierre vs EMA50) entre velas
    # guardamos: {"as_of": str, "bias": str, "signal": str, "above50": bool}
    last_info: Dict[str, Dict[str, Optional[str] | bool]] = {}

    seen = _load_seen()
    print(f"[INIT] DRY_RUN={DRY_RUN} symbols={TRADE_SYMBOLS}")

    # --------- Snapshot inmediato al iniciar ---------
    for sym, mon in monitors.items():
        snap, msg = mon.get_snapshot()
        if not snap:
            print(f"[{_iso_z(datetime.now(timezone.utc))}] [{sym}] INIT WARN: {msg}")
            continue
        bias = "BUY" if snap.ema50 > snap.ema200 else "SELL"
        print(f"[{_iso_z(datetime.now(timezone.utc))}] [INIT {sym}] as_of={snap.as_of} "
              f"close={snap.close:.2f} ema50={snap.ema50:.2f} ema200={snap.ema200:.2f} "
              f"bias={bias} signal={snap.signal}")
        last_info[sym] = {
            "as_of": snap.as_of,
            "bias": bias,
            "signal": snap.signal or "None",
            "above50": bool(snap.close > snap.ema50),
        }

    # Loop de chequeo en cierres exactos
    while True:
        now = datetime.now(timezone.utc)

        # Chequeamos únicamente en los minutos de interés
        if (now.minute % 15) in CHECK_MINUTES and now.second == 0:
            # pequeño lag para permitir que cierre y aparezca la vela
            time.sleep(max(CLOSE_LAG_SEC, 0.1))

            # Control de “impreso una sola vez por símbolo”
            printed: set[str] = set()

            for attempt in range(CLOSE_RETRY_COUNT):
                for sym, mon in monitors.items():
                    if sym in printed:
                        continue  # ya mostramos/sonamos/notify para este símbolo en este cierre

                    snap, msg = mon.get_snapshot()
                    if not snap:
                        # solo informamos si es el último intento
                        if attempt == CLOSE_RETRY_COUNT - 1:
                            print(f"[{_iso_z(datetime.now(timezone.utc))}] [{sym}] WARN snapshot: {msg}")
                        continue

                    bias = "BUY" if snap.ema50 > snap.ema200 else "SELL"
                    curr_above50 = bool(snap.close > snap.ema50)

                    prev = last_info.get(sym)
                    is_new_bar = (prev is None) or (prev.get("as_of") != snap.as_of)

                    if not is_new_bar:
                        # todavía no cerró la vela nueva; reintentaremos
                        continue

                    # --------- LOG detallado UNA sola vez ---------
                    print(f"[{_iso_z(datetime.now(timezone.utc))}] [{sym}] as_of={snap.as_of} "
                          f"close={snap.close:.2f} ema50={snap.ema50:.2f} ema200={snap.ema200:.2f} "
                          f"bias={bias} signal={snap.signal}")

                    # --------- DETECCIÓN DE PULLBACK + BEEP + NOTIFY (solo una vez) ----------
                    prev_above50 = None if prev is None else bool(prev.get("above50"))
                    if prev_above50 is not None:
                        # BUY bias: cruce de ARRIBA->ABAJO (cerró debajo de EMA50)
                        if bias == "BUY" and prev_above50 and (not curr_above50):
                            _beep()
                            txt = (f"📉 <b>Pullback BUY</b> {sym}\n"
                                   f"as_of: {snap.as_of}\n"
                                   f"close: {snap.close:.2f}\n"
                                   f"EMA50: {snap.ema50:.2f}\n"
                                   f"EMA200:{snap.ema200:.2f}\n"
                                   f"Evento: cierre pasó de >EMA50 a <EMA50")
                            notifier.send(txt)
                            print(f"[BEEP][NOTIFY] {sym} pullback BUY")

                        # SELL bias: cruce de ABAJO->ARRIBA (cerró encima de EMA50)
                        if bias == "SELL" and (not prev_above50) and curr_above50:
                            _beep()
                            txt = (f"📈 <b>Pullback SELL</b> {sym}\n"
                                   f"as_of: {snap.as_of}\n"
                                   f"close: {snap.close:.2f}\n"
                                   f"EMA50: {snap.ema50:.2f}\n"
                                   f"EMA200:{snap.ema200:.2f}\n"
                                   f"Evento: cierre pasó de <EMA50 a >EMA50")
                            notifier.send(txt)
                            print(f"[BEEP][NOTIFY] {sym} pullback SELL")

                    # Cambios de bias / señal (informativos, también una vez)
                    if prev:
                        if prev.get("bias") != bias:
                            print(f"[{_iso_z(datetime.now(timezone.utc))}] [{sym}] BIAS CHANGE: {prev.get('bias')} -> {bias}")
                        if prev.get("signal") != (snap.signal or "None"):
                            print(f"[{_iso_z(datetime.now(timezone.utc))}] [{sym}] SIGNAL CHANGE: {prev.get('signal')} -> {snap.signal}")

                    # actualizar estado de última vela y marcar como impreso
                    last_info[sym] = {
                        "as_of": snap.as_of,
                        "bias": bias,
                        "signal": snap.signal or "None",
                        "above50": curr_above50,
                    }
                    printed.add(sym)

                    # Idempotencia (marcar vista esta vela-señal)
                    ev_id = _event_id(sym, snap.as_of, snap.signal)
                    # (ya no existe duplicidad porque imprimimos una vez por símbolo)
                    if ev_id not in seen:
                        seen.add(ev_id)
                        _save_seen(seen)

                # ¿ya imprimimos todos? cortar reintentos
                if len(printed) == len(monitors):
                    break
                # si faltan, esperamos y reintentamos
                time.sleep(CLOSE_RETRY_INTERVAL)

        # dormir hasta el siguiente segundo (liviano)
        time.sleep(1)

if __name__ == "__main__":
    main()
