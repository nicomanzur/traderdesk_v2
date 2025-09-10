# timed_signal_trader.py
from __future__ import annotations
import os, time, json, smtplib, ssl
from email.mime.text import MIMEText
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional
from zoneinfo import ZoneInfo

import requests

from app.brokers.projectx_api import ProjectXClient
from app.services.market_monitor import MarketMonitor

# ===================== ENV / Parámetros =====================
BAR_MINUTES = int(os.getenv("BAR_MINUTES", "15"))
BAR_TS_MODE = os.getenv("BAR_TIMESTAMP_MODE", "open").strip().lower()  # 'open' o 'close'
ALIGN_TZ_NAME = os.getenv("ALIGN_TIMEZONE", "UTC").strip() or "UTC"
CLOSE_LAG_SEC = float(os.getenv("CLOSE_LAG_SEC", "1.2"))
RETRY_COUNT = int(os.getenv("CLOSE_RETRY_COUNT", "15"))
RETRY_INTERVAL = float(os.getenv("CLOSE_RETRY_INTERVAL", "0.5"))

DRY_RUN = os.getenv("DRY_RUN", "true").lower() in ("1", "true", "yes")
SIZE_MNQ = int(os.getenv("ORDER_SIZE_MNQ", os.getenv("ORDER_SIZE", "1")))
SIZE_ES  = int(os.getenv("ORDER_SIZE_ES",  os.getenv("ORDER_SIZE", "1")))
TP_MNQ   = float(os.getenv("TP_POINTS_MNQ", "60"))
SL_MNQ   = float(os.getenv("SL_POINTS_MNQ", "30"))
TP_ES    = float(os.getenv("TP_POINTS_ES",  "8"))
SL_ES    = float(os.getenv("SL_POINTS_ES",  "4"))
EXPLICIT_ACCT = os.getenv("PRACTICE_ACCOUNT_ID")

TRADES_LOG = os.getenv("TRADES_LOG", "trades.log")

USE_TG = os.getenv("NOTIFY_TELEGRAM", "false").lower() in ("1", "true", "yes")
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TG_CHAT  = os.getenv("TELEGRAM_CHAT_ID", "").strip()

USE_MAIL = os.getenv("NOTIFY_EMAIL", "false").lower() in ("1", "true", "yes")
SMTP_HOST = os.getenv("SMTP_HOST", "").strip()
SMTP_PORT = int(os.getenv("SMTP_PORT", "0") or 0)
SMTP_USER = os.getenv("SMTP_USER", "").strip()
SMTP_PASS = os.getenv("SMTP_PASS", "").strip()
EMAIL_FROM = (os.getenv("EMAIL_FROM", "").strip() or SMTP_USER)
EMAIL_TO = os.getenv("EMAIL_TO", "").strip()
NOTIFY_ON_DRY = os.getenv("NOTIFY_ON_DRY_RUN", "false").lower() in ("1", "true", "yes")

DEBUG_TRADE = os.getenv("DEBUG_TRADE", "false").lower() in ("1", "true", "yes")
def _dbg(*a, **k):
    if DEBUG_TRADE:
        print("[DEBUG]", *a, **k)

# ===================== Utilidades de tiempo =====================
def _get_align_tz() -> ZoneInfo:
    try:
        return ZoneInfo(ALIGN_TZ_NAME)
    except Exception:
        return ZoneInfo("UTC")

def _next_quarter_close(now_tz: datetime) -> datetime:
    """Próximo cierre :00/:15/:30/:45 (segundo 0) en el huso elegido."""
    m = now_tz.minute
    next_q = ((m // 15) + 1) * 15
    if next_q >= 60:
        return (now_tz.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
    return now_tz.replace(minute=next_q, second=0, microsecond=0)

def _sleep_until(ts: datetime):
    while True:
        now = datetime.now(ts.tzinfo)
        delta = (ts - now).total_seconds()
        if delta <= 0: break
        time.sleep(min(delta, 0.5))

# ===================== Utilidades de órdenes =====================
def _round_to_tick(price: float, tick: float) -> float:
    return round(round(price / tick) * tick, 10)

def _pick_practice_account(px: ProjectXClient, explicit: Optional[int]) -> int:
    if explicit: return int(explicit)
    accts = px.search_accounts(only_active=True)
    for a in accts:
        if "PRACTICE" in str(a.get("name", "")).upper():
            return int(a["id"])
    for a in accts:
        if a.get("canTrade"):
            return int(a["id"])
    raise RuntimeError("No encontré una cuenta PRACTICE ni operable (canTrade).")

def _get_tick_size(px: ProjectXClient, contract_id: str, default: float = 0.25) -> float:
    try:
        det = px.search_contracts_by_id(contract_id) or []
        if det and det[0].get("tickSize") is not None:
            return float(det[0]["tickSize"])
    except Exception as e:
        print(f"[WARN] tickSize fallback para {contract_id}: {e}")
    return default

def _poll_fill_price(px: ProjectXClient, account_id: int, contract_id: str, max_wait_sec: int = 30) -> Optional[float]:
    start_iso = (datetime.now(timezone.utc) - timedelta(minutes=2)).isoformat()
    waited = 0
    while waited < max_wait_sec:
        trades = px.search_trades(account_id, start_iso)
        for tr in reversed(trades):
            if tr.get("contractId") == contract_id:
                try:
                    return float(tr.get("price"))
                except Exception:
                    pass
        time.sleep(1); waited += 1
    return None

# ===================== Logging / Notificaciones =====================
def _write_log(entry: dict) -> None:
    try:
        with open(TRADES_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception as e:
        print("[LOG][WARN]", e)

def _notify_telegram(text: str) -> None:
    if not (USE_TG and TG_TOKEN and TG_CHAT): return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT, "text": text, "disable_web_page_preview": True},
            timeout=10
        )
    except Exception as e:
        print("[TEL][WARN]", e)

def _notify_email(subject: str, body: str) -> None:
    if not (USE_MAIL and SMTP_HOST and SMTP_PORT and SMTP_USER and SMTP_PASS and EMAIL_TO and EMAIL_FROM):
        return
    try:
        msg = MIMEText(body, _charset="utf-8")
        msg["Subject"] = subject
        msg["From"] = EMAIL_FROM
        msg["To"] = EMAIL_TO

        if SMTP_PORT == 465:
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=context, timeout=15) as s:
                s.login(SMTP_USER, SMTP_PASS)
                s.sendmail(EMAIL_FROM, [x.strip() for x in EMAIL_TO.split(",") if x.strip()], msg.as_string())
        else:
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15) as s:
                s.ehlo(); s.starttls(context=ssl.create_default_context())
                s.login(SMTP_USER, SMTP_PASS)
                s.sendmail(EMAIL_FROM, [x.strip() for x in EMAIL_TO.split(",") if x.strip()], msg.as_string())
    except Exception as e:
        print("[MAIL][WARN]", e)

def _notify_trade(event: dict, dry_run: bool) -> None:
    if dry_run and not NOTIFY_ON_DRY: return
    sym = event.get("symbol"); sig = event.get("signal"); qty = event.get("qty")
    asof = event.get("as_of"); fill = event.get("fill_price")
    tp   = event.get("tp_price"); sl = event.get("sl_price")
    acct = event.get("account_id")
    subject = f"[{sym}] {sig} x{qty} {'(DRY)' if dry_run else ''}"
    body = (
        f"Símbolo: {sym}\nSeñal: {sig}\nCantidad: {qty}\nCuenta: {acct}\n"
        f"as_of (vela): {asof}\nFill: {fill}\nTP: {tp}\nSL: {sl}\n"
        f"Tag: {event.get('tag')}\nParentId: {event.get('parent_order_id')}\n"
        f"TP_Id: {event.get('tp_order_id')}\nSL_Id: {event.get('sl_order_id')}\n"
        f"Contrato: {event.get('contract_id')}\nFecha evento: {event.get('ts')}\n"
    )
    _notify_telegram(subject + "\n" + body)
    _notify_email(subject, body)

# ===================== Brackets =====================
def _place_market_and_brackets(px: ProjectXClient,
                               account_id: int,
                               contract_id: str,
                               side: int,  # 0=Buy, 1=Sell
                               qty: int,
                               tp_points: float,
                               sl_points: float,
                               tick_size: float,
                               tag_prefix: str) -> dict:
    out = {
        "parent_order_id": None, "fill_price": None,
        "tp_order_id": None, "sl_order_id": None,
        "tp_price": None, "sl_price": None,
    }
    parent_tag = f"{tag_prefix}-parent"
    placed = px.place_order(
        account_id=account_id, contract_id=contract_id,
        side=side, size=qty, order_type=2, customTag=parent_tag  # Market
    )
    out["parent_order_id"] = placed.get("orderId")
    print(f"[ORDER] parent MARKET id={out['parent_order_id']} side={side} qty={qty}")

    fill = _poll_fill_price(px, account_id, contract_id, max_wait_sec=30)
    out["fill_price"] = fill
    if not fill:
        print("[WARN] No vi fill aún; no coloco TP/SL.")
        return out

    if side == 0:  # LONG
        tp_price = _round_to_tick(fill + tp_points, tick_size)
        sl_price = _round_to_tick(fill - sl_points, tick_size)
        tp_side, sl_side = 1, 1
    else:          # SHORT
        tp_price = _round_to_tick(fill - tp_points, tick_size)
        sl_price = _round_to_tick(fill + sl_points, tick_size)
        tp_side, sl_side = 0, 0

    out["tp_price"] = tp_price; out["sl_price"] = sl_price

    res_tp = px.place_order(
        account_id=account_id, contract_id=contract_id,
        side=tp_side, size=qty, order_type=1,  # Limit
        limit_price=tp_price, linked_order_id=out["parent_order_id"],
        customTag=f"{tag_prefix}-TP"
    )
    out["tp_order_id"] = res_tp.get("orderId")
    print(f"[ORDER] TP id={out['tp_order_id']} @ {tp_price}")

    res_sl = px.place_order(
        account_id=account_id, contract_id=contract_id,
        side=sl_side, size=qty, order_type=4,  # Stop (market)
        stop_price=sl_price, linked_order_id=out["parent_order_id"],
        customTag=f"{tag_prefix}-SL"
    )
    out["sl_order_id"] = res_sl.get("orderId")
    print(f"[ORDER] SL id={out['sl_order_id']} @ {sl_price}")
    return out

# ===================== Main alineado a cierres =====================
def main():
    align_tz = _get_align_tz()

    px = ProjectXClient()
    px.login_with_key()
    account_id = _pick_practice_account(px, int(EXPLICIT_ACCT) if EXPLICIT_ACCT else None)
    print(f"[TimedTrader] PRACTICE account id={account_id} DRY_RUN={DRY_RUN} "
          f"TZ={align_tz.key} LAG={CLOSE_LAG_SEC}s TS_MODE={BAR_TS_MODE}")

    mons: Dict[str, MarketMonitor] = {
        "MNQ": MarketMonitor("MNQ", px=px),
        "ES":  MarketMonitor("ES",  px=px),
    }

    tick_size: Dict[str, float] = {}
    for sym, mon in mons.items():
        if not mon.contract_id:
            raise RuntimeError(f"Sin contractId para {sym}")
        tsz = _get_tick_size(px, mon.contract_id, default=0.25)
        tick_size[sym] = tsz
        print(f"[{sym}] contract_id={mon.contract_id} tickSize={tsz}")

    last_sent_by_symbol: Dict[str, str] = {}

    while True:
        try:
            now_tz = datetime.now(align_tz)
            target_close = _next_quarter_close(now_tz)
            _sleep_until(target_close)
            time.sleep(CLOSE_LAG_SEC)  # margen de publicación

            expected_close_utc = target_close.astimezone(timezone.utc).replace(microsecond=0)
            expected_open_utc  = expected_close_utc - timedelta(minutes=BAR_MINUTES)

            ts_now_utc = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

            _dbg(f"Expecting {('open' if BAR_TS_MODE=='open' else 'close')} >= "
                 f"{(expected_open_utc if BAR_TS_MODE=='open' else expected_close_utc).isoformat().replace('+00:00','Z')}, "
                 f"lag={CLOSE_LAG_SEC}s, retries={RETRY_COUNT}x{RETRY_INTERVAL}s")

            for sym, mon in mons.items():
                snap = None
                last_got = None
                # reintentos breves hasta que aparezca la vela esperada
                for _ in range(RETRY_COUNT):
                    s, msg = mon.get_snapshot()
                    if s:
                        last_got = s.as_of
                        got = datetime.fromisoformat(s.as_of.replace("Z", "+00:00"))
                        ok = (got >= expected_open_utc) if BAR_TS_MODE == "open" else (got >= expected_close_utc)
                        if ok:
                            snap = s
                            break
                    time.sleep(RETRY_INTERVAL)

                if not snap:
                    want = (expected_open_utc if BAR_TS_MODE == "open" else expected_close_utc)
                    print(f"[{ts_now_utc}] [{sym}] WARN: no llegó la vela de {want.isoformat().replace('+00:00','Z')} "
                          f"(última disponible: {last_got})")
                    _dbg(f"{sym} still waiting: last_got={last_got}, BAR_TS_MODE={BAR_TS_MODE}")
                    continue

                print(f"[{ts_now_utc}] [{sym}] as_of={snap.as_of} close={snap.close:.2f} "
                      f"ema50={snap.ema50:.2f} ema200={snap.ema200:.2f} signal={snap.signal}")

                # ---------- DEBUG detallado ----------
                d = {}
                try:
                    if hasattr(mon, "get_debug_state"):
                        d = mon.get_debug_state()  # requiere que lo hayas agregado en MarketMonitor
                        _dbg(f"{sym} prev_ts={d.get('prev_ts')} curr_ts={d.get('curr_ts')}")
                        _dbg(f"{sym} prev_close={d.get('prev_close')} prev_e50={d.get('prev_e50')} prev_e200={d.get('prev_e200_base')}")
                        _dbg(f"{sym} curr_close={d.get('curr_close')} curr_e50={d.get('curr_e50')} curr_e200={d.get('curr_e200_base')}")
                        _dbg(f"{sym} color={d.get('color')} signal={d.get('signal')} conds={d.get('conds')}")
                except Exception as e:
                    _dbg(f"{sym} debug_state error: {e}")

                if snap.signal not in ("LONG", "SHORT"):
                    continue
                if last_sent_by_symbol.get(sym) == snap.as_of:
                    _dbg(f"{sym} SKIP duplicated as_of={snap.as_of}")
                    continue  # ya enviamos por esta vela

                if sym == "MNQ":
                    qty, tp_pts, sl_pts = SIZE_MNQ, TP_MNQ, SL_MNQ
                else:
                    qty, tp_pts, sl_pts = SIZE_ES, TP_ES, SL_ES

                side = 0 if snap.signal == "LONG" else 1
                tag  = f"timed-{sym}-{int(time.time())}"

                base_log = {
                    "ts": ts_now_utc, "as_of": snap.as_of, "symbol": sym,
                    "contract_id": snap.contract_id, "signal": snap.signal,
                    "qty": qty, "tp_points": tp_pts, "sl_points": sl_pts,
                    "account_id": account_id, "dry_run": DRY_RUN, "tag": tag
                }

                if DRY_RUN:
                    print(f"[{sym}] DRY_RUN -> MARKET {snap.signal} x{qty} "
                          f"TP={tp_pts} SL={sl_pts} (tick={tick_size[sym]}) tag={tag}")
                    _write_log({**base_log, "event": "DRY_RUN_SIGNAL"})
                    _notify_trade({**base_log}, dry_run=True)
                else:
                    try:
                        details = _place_market_and_brackets(
                            px=px, account_id=account_id, contract_id=snap.contract_id,
                            side=side, qty=qty, tp_points=tp_pts, sl_points=sl_pts,
                            tick_size=tick_size[sym], tag_prefix=tag
                        )
                        full = {**base_log, "event": "ORDER_SENT", **details}
                        _write_log(full)
                        _notify_trade(full, dry_run=False)
                    except Exception as e:
                        err = {**base_log, "event": "ORDER_ERROR", "error": str(e)}
                        print(f"[{sym}] ERROR placing bracket: {e}")
                        _write_log(err)
                        _notify_trade(err, dry_run=False)

                last_sent_by_symbol[sym] = snap.as_of

        except KeyboardInterrupt:
            print("Bye!")
            break
        except Exception as e:
            print("[LOOP][WARN]", e)
            time.sleep(2)

if __name__ == "__main__":
    main()
