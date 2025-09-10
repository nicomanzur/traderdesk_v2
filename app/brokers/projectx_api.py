# app/brokers/projectx_api.py
from __future__ import annotations
import os, datetime as dt
from typing import Any, Dict, List, Optional
import requests
from urllib.parse import urlparse, urlunparse
from dotenv import load_dotenv

load_dotenv()

def _https_base(url: str) -> str:
    u = urlparse(url.strip())
    scheme = "https"
    netloc = u.netloc or u.path
    return urlunparse((scheme, netloc, "", "", "", ""))

class ProjectXClient:
    def __init__(self) -> None:
        base = os.getenv("PROJECTX_API_BASE", "https://api.topstepx.com")
        self.base_api = _https_base(base)
        self.user = os.getenv("PROJECTX_USER", "").strip()
        self.api_key = os.getenv("PROJECTX_API_KEY", "").strip()
        self.session = requests.Session()
        self.token: Optional[str] = None
        print(f"[ProjectXClient] base_api={self.base_api} user={self.user[:2]}***")

    def _headers(self) -> Dict[str, str]:
        h = {"Content-Type": "application/json"}
        if self.token:
            h["Authorization"] = f"Bearer {self.token}"
        return h

    def _post(self, path: str, payload: Dict[str, Any], timeout: int = 30) -> Dict[str, Any]:
        url = f"{self.base_api}{path}"

        def do():
            return self.session.post(url, headers=self._headers(), json=payload, timeout=timeout)

        r = do()
        if r.status_code == 401:
            # token ausente/vencido → re-login y reintento 1 vez
            try:
                self.login_with_key()
                r = do()
            except Exception:
                pass

        try:
            data = r.json()
        except Exception:
            data = {"raw": r.text}

        if not r.ok:
            print("resp:", r.status_code, data)
            r.raise_for_status()
        return data

    # -------- Auth --------
    def login_with_key(self) -> str:
        payload = {"userName": self.user, "apiKey": self.api_key}
        # endpoint correcto: /api/Auth/loginKey
        data = self._post("/api/Auth/loginKey", payload, timeout=20)
        if not data.get("success"):
            raise RuntimeError(f"Auth failed: {data}")
        token = data.get("token")
        if not token:
            raise RuntimeError(f"No token in response: {data}")
        self.token = token
        return token

    def validate_token(self) -> bool:
        data = self._post("/api/Auth/validate", {}, timeout=10)
        return bool(data.get("success"))

    # -------- Accounts --------
    def search_accounts(self, only_active: bool = True) -> List[Dict[str, Any]]:
        data = self._post("/api/Account/search", {"onlyActiveAccounts": only_active}, timeout=20)
        if not data.get("success"):
            raise RuntimeError(f"Account.search failed: {data}")
        return data.get("accounts", []) or data.get("items", [])

    # -------- Contracts --------
    def search_contracts(self, text: str, live: bool = False) -> List[Dict[str, Any]]:
        payload = {"searchText": text, "liveSubscription": live}
        data = self._post("/api/Contract/search", payload, timeout=20)
        if not data.get("success"):
            raise RuntimeError(f"Contract.search failed: {data}")
        return data.get("contracts", []) or data.get("items", [])

    def search_contracts_by_id(self, contract_id: str) -> List[Dict[str, Any]]:
        data = self._post("/api/Contract/searchById", {"contractId": contract_id}, timeout=15)
        if not data.get("success"):
            raise RuntimeError(f"Contract.searchById failed: {data}")
        return data.get("contracts", []) or data.get("items", [])

    # -------- History (bars) --------
    def retrieve_bars(
        self,
        contract_id: str,
        live: bool = False,
        unit: int = 2,
        unit_number: int = 15,
        include_partial: bool = False,
        limit: int = 400,
        lookback_days: Optional[int] = None,
        start: Optional[dt.datetime] = None,
        end: Optional[dt.datetime] = None,
    ) -> List[Dict[str, Any]]:
        now = dt.datetime.now(dt.timezone.utc)
        if end is None:
            end = now
        if start is None:
            days = lookback_days or 7
            start = end - dt.timedelta(days=days)
        payload = {
            "contractId": contract_id,
            "live": bool(live),
            "unit": unit,
            "unitNumber": unit_number,
            "startTime": start.replace(microsecond=0).isoformat().replace("+00:00","Z"),
            "endTime":   end.replace(microsecond=0).isoformat().replace("+00:00","Z"),
            "limit": int(limit),
            "includePartialBar": bool(include_partial),
        }
        data = self._post("/api/History/retrieveBars", payload, timeout=30)
        if not data.get("success"):
            raise RuntimeError(f"retrieveBars failed: {data}")
        return data.get("bars") or []

    # -------- Orders --------
    def place_order(
        self,
        account_id: int,
        contract_id: str,
        side: int,                 # 0=Buy, 1=Sell
        size: int,
        order_type: int = 2,       # 2=Market, 1=Limit, 4=Stop
        limit_price: Optional[float] = None,
        stop_price: Optional[float] = None,
        linked_order_id: Optional[int] = None,
        customTag: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "accountId": account_id,
            "contractId": contract_id,
            "type": order_type,
            "side": side,
            "size": size,
        }
        if limit_price is not None:
            payload["limitPrice"] = float(limit_price)
        if stop_price is not None:
            payload["stopPrice"] = float(stop_price)
        if linked_order_id is not None:
            payload["linkedOrderId"] = linked_order_id
        if customTag:
            payload["customTag"] = customTag

        data = self._post("/api/Order/place", payload, timeout=20)
        if not data.get("success"):
            raise RuntimeError(f"order.place failed: {data}")
        return data

    def search_trades(self, account_id: int, start_iso: str, end_iso: Optional[str] = None) -> List[Dict[str, Any]]:
        payload: Dict[str, Any] = {"accountId": account_id, "startTimestamp": start_iso}
        if end_iso:
            payload["endTimestamp"] = end_iso
        data = self._post("/api/Trade/search", payload, timeout=20)
        if not data.get("success"):
            raise RuntimeError(f"Trade.search failed: {data}")
        return data.get("trades", []) or []
