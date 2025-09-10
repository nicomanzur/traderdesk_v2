# signal_trader.py
from __future__ import annotations
import os, time
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional

from app.brokers.projectx_api import ProjectXClient
from app.services.market_monitor import MarketMonitor

def _round_to_tick(price: float, tick: float) -> float:
    return round(round(price / tick) * tick, 10)

def _pick_practice_account(px: ProjectXClient, explicit: Optional[int]) -> int:
    if explicit:
        return int(explicit)
    accts = px.search_accounts(only_active=True)
    for a in accts:
        if "PRACTICE" in str(a.get("name","")).upper():
            return int(a["id"])
    for a in accts:
        if a.get("canTrade"):
            return int(a["id"])
    raise RuntimeError("No encontré una cuenta PRACTICE ni operable (canTrade).")

def _get_tick_size(px: ProjectXClient, contract_id: str, default: float = 0.25) -> float:
    try:
        det = px.search_contracts_by_id(contract_id) or []
        if det:
            ts = det[0].get("tickSize")
            if ts is not None:
                return float(ts)
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
        time.sleep(1)
        waited += 1
    return None

def _place_market_and_brackets(px: ProjectXClient,
                               account_id: int,
                               contract_id: str,
                               side: int, qty: int,
                               tp_points: float, sl_points: float,
                               tick_size: float, tag_prefix: str) -> None:
    parent_tag = f"{tag_prefix}-parent"
    placed = px.place_order(
        account_id=account_id,
        contract_id=contract_id,
        side=side,
        size=qty,
        order_type=2,  # Market
        customTag=parent_tag
    )
    parent_id = placed.get("orderId")
    print(f"[ORDER] parent MARKET id={parent_id} side={side} qty={qty}")

    fill = _poll_fill_price(px, account_id, contract_id, max_wait_sec=30)
    if not fill:
        print("[WARN] No vi fill aún; no coloco TP/SL.")
        return

    if side == 0:  # LONG
        tp_price = _round_to_tick(fill + tp_points, tick_size)
        sl_price = _round_to_tick(fill - sl_points, tick_size)
        tp_side, sl_side = 1, 1
    else:          # SHORT
        tp_price = _round_to_tick(fill - tp_points, tick_size)
        sl_price = _round_to_tick(fill + sl_points, tick_size)
        tp_side, sl_side = 0, 0

    res_tp = px.place_order(
        account_id=account_id, contract_id=contract_id,
        side=tp_side, size=qty,
        order_type=1, limit_price=tp_price,
        linked_order_id=parent_id, customTag=f"{tag_prefix}-TP"
    )
    print(f"[ORDER] TP id={res_tp.get('orderId')} @ {tp_price}")

    res_sl = px.place_order(
        account_id=account_id, contract_id=contract_id,
        side=sl_side, size=qty,
        order_type=4, stop_price=sl_price,
        linked_order_id=parent_id, customTag=f"{tag_prefix}-SL"
    )
    print(f"[ORDER] SL id={res_sl.get('orderId')} @ {sl_price}")

def main():
    # ENV
    dry_run = os.getenv("DRY_RUN", "true").lower() in ("1","true","yes")
    debug   = os.getenv("DEBUG_TRADER","0").lower() in ("1","true","yes")

    size_mnq = int(os.getenv("ORDER_SIZE_MNQ", os.getenv("ORDER_SIZE", "1")))
    size_es  = int(os.getenv("ORDER_SIZE_ES",  os.getenv("ORDER_SIZE", "1")))
    tp_mnq   = float(os.getenv("TP_POINTS_MNQ", "60"))
    sl_mnq   = float(os.getenv("SL_POINTS_MNQ", "30"))
    tp_es    = float(os.getenv("TP_POINTS_ES",  "8"))
    sl_es    = float(os.getenv("SL_POINTS_ES",  "4"))
    explicit_acct = os.getenv("PRACTICE_ACCOUNT_ID")

    px = ProjectXClient()
    px.login_with_key()
    account_id = _pick_practice_account(px, int(explicit_acct) if explicit_acct else None)
    print(f"[Trader] PRACTICE account id={account_id} DRY_RUN={dry_run} DEBUG={debug}")

    mons: Dict[str, MarketMonitor] = {
        "MNQ": MarketMonitor("MNQ", px=px),
        "ES":  MarketMonitor("ES",  px=px),
    }

    # tickSize por contrato
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
            nowz = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            for sym, mon in mons.items():
                snap, msg = mon.get_snapshot()
                if not snap:
                    print(f"[{nowz}] [{sym}] WARN: {msg}")
                    continue

                # logs estándar
                print(f"[{nowz}] [{sym}] as_of={snap.as_of} "
                      f"close={snap.close:.2f} ema50={snap.ema50:.2f} ema200={snap.ema200:.2f} "
                      f"color={snap.color} signal={snap.signal}")

                # modo diagnóstico: muestra prev/curr internos del monitor
                if debug:
                    st = mon.state  # dict interno expuesto
                    print(f"[DBG][{sym}] prev_close={st.get('prev_close')} "
                          f"prev_e50={st.get('prev_e50')} prev_e200_base={st.get('prev_e200_base')}")
                    print(f"[DBG][{sym}] curr_close={st.get('curr_close')} "
                          f"curr_e50={st.get('curr_e50')} curr_e200_base={st.get('curr_e200_base')}")
                    # por qué no dispara
                    reasons = []
                    if snap.signal not in ("LONG","SHORT"):
                        reasons.append("no-signal")
                    if last_sent_by_symbol.get(sym) == snap.as_of:
                        reasons.append("dup-same-bar")
                    if dry_run:
                        reasons.append("dry-run")
                    if reasons:
                        print(f"[DBG][{sym}] SKIP REASON={reasons}")

                # chequeo de disparo
                if snap.signal not in ("LONG", "SHORT"):
                    continue
                if last_sent_by_symbol.get(sym) == snap.as_of:
                    # ya enviamos por esta vela
                    continue

                # config por símbolo
                if sym == "MNQ":
                    qty, tp_pts, sl_pts = size_mnq, tp_mnq, sl_mnq
                else:
                    qty, tp_pts, sl_pts = size_es, tp_es, sl_es

                side = 0 if snap.signal == "LONG" else 1  # 0=Buy, 1=Sell
                tag  = f"sig-{sym}-{int(time.time())}"

                if dry_run:
                    print(f"[{sym}] DRY_RUN -> MARKET {snap.signal} x{qty} "
                          f"TP={tp_pts} SL={sl_pts} (tick={tick_size[sym]}) tag={tag}")
                else:
                    try:
                        _place_market_and_brackets(
                            px=px, account_id=account_id, contract_id=snap.contract_id,
                            side=side, qty=qty, tp_points=tp_pts, sl_points=sl_pts,
                            tick_size=tick_size[sym], tag_prefix=tag
                        )
                    except Exception as e:
                        print(f"[{sym}] ERROR placing bracket: {e}")
                        # si falló, no marques last_sent

                last_sent_by_symbol[sym] = snap.as_of

            time.sleep(5)
        except KeyboardInterrupt:
            print("Bye!")
            break
        except Exception as e:
            print("[LOOP][WARN]", e)
            time.sleep(5)

if __name__ == "__main__":
    main()
