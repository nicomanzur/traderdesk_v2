from __future__ import annotations
import time
from app.services.market_monitor import MarketMonitor

def run_once():
    for sym in ("MNQ", "ES"):
        mon = MarketMonitor(sym)
        snap, msg = mon.get_snapshot()
        if not snap:
            print(f"[{sym}] WARN:", msg); continue
        print(f"[{sym}] {snap.as_of} close={snap.close:.2f} "
              f"ema50={snap.ema50:.2f} ema200={snap.ema200:.2f} color={snap.color}")

if __name__ == "__main__":
    while True:
        try:
            run_once()
            time.sleep(5)
        except KeyboardInterrupt:
            break
