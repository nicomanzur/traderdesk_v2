# app/brokers/projectx_api.py
from __future__ import annotations

import os
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, urlunparse

import requests


def _iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name, "")
    if v == "":
        return default
    return v.lower() in ("1", "true", "yes", "on")


class ProjectXClient:
    """
    Cliente ligero para TopstepX/ProjectX Gateway API.
    Lee credenciales desde .env:
      - PROJECTX_API_BASE (p.ej. https://api.topstepx.com)
      - PROJECTX_USER
      - PROJECTX_API_KEY
    """

    def __init__(self, base_api: Optional[str] = None, user: Optional[str] = None, api_key: Optional[str] = None):
        # Base URL (forzar https)
        base = base_api or os.getenv("PROJECTX_API_BASE", "https://api.topstepx.com").strip()
        u = urlparse(base)
        if u.scheme != "https":
            base = urlunparse(("https", u.netloc, u.path, u.params, u.query, u.fragment))
        self.base_api: str = base.rstrip("/")

        # Credenciales
        self.user: str = user or os.getenv("PROJECTX_USER", "").strip()
        self.api_key: str = api_key or os.getenv("PROJECTX_API_KEY", "").strip()

        # Sesión HTTP
        self.session = requests.Session()
        self._token: Optional[str] = None

        # Debug HTTP
        self.debug_http: bool = _env_bool("DEBUG_HTTP", False)

        print(f"[ProjectXClient] base_api={self.base_api} user={self.user or '(env?)'}")

    # ------------- HTTP helpers -------------

    def _headers(self) -> Dict[str, str]:
        h = {"Content-Type": "application/json"}
        if self._token:
            h["Authorization"] = f"Bearer {self._token}"
        return h

    def _post(self, path: str, payload: Dict[str, Any], timeout: float = 30.0) -> Dict[str, Any]:
        url = f"{self.base_api}{path}"
        if self.debug_http:
            try:
                print("POST", url)
                print("payload:", json.dumps(payload, ensure_ascii=False))
            except Exception:
                print("POST", url)
                print("payload:(no-dump)")
        r = self.session.post(url, headers=self._headers(), json=payload, timeout=timeout)
        if not r.ok:
            # intentar mostrar body decodificado
            try:
                body = r.json()
            except Exception:
                body = {"raw": r.text[:500]}
            print("resp:", r.status_code, body)
            r.raise_for_status()
        try:
            return r.json()
        except Exception:
            return {"raw": r.text}

    # ------------- Auth -------------

    def login_with_key(self) -> str:
        """
        POST /api/Auth/loginKey   (prod)
        payload: { "userName": <str>, "apiKey": <str> }
        """
        if not self.user or not self.api_key:
            raise requests.HTTPError("Missing PROJECTX_USER / PROJECTX_API_KEY", response=requests.Response())

        payload = {"userName": self.user, "apiKey": self.api_key}

        # preferido: loginKey
        try:
            data = self._post("/api/Auth/loginKey", payload, timeout=20)
        except requests.HTTPError as e:
            # compatibilidad con ambientes viejos que usaban loginWithKey
            if e.response is not None and e.response.status_code == 404:
                data = self._post("/api/Auth/loginWithKey", payload, timeout=20)
            else:
                raise

        token = (data or {}).get("token")
        if not token:
            raise requests.HTTPError("Auth failed", response=requests.Response())

        self._token = token
        self.session.headers.update({"Authorization": f"Bearer {self._token}"})
        return token


    def validate_token(self) -> bool:
        """
        POST /api/Auth/validate
        """
        data = self._post("/api/Auth/validate", {}, timeout=10)
        return bool((data or {}).get("success", False))

    # ------------- Accounts -------------

    def search_accounts(self, only_active: bool = True) -> List[Dict[str, Any]]:
        """
        POST /api/Account/search
        payload: { "onlyActiveAccounts": true/false }
        """
        payload = {"onlyActiveAccounts": bool(only_active)}
        data = self._post("/api/Account/search", payload, timeout=15)
        if not data.get("success", False):
            raise RuntimeError(f"Account.search failed: {data}")
        return data.get("accounts", []) or []

    # ------------- Contracts -------------

    def search_contracts(self, text: str, live: bool = False) -> List[Dict[str, Any]]:
        """
        POST /api/Contract/search
        payload: { "text": <str>, "live": <bool> }
        """
        payload = {"text": text, "live": bool(live)}
        data = self._post("/api/Contract/search", payload, timeout=20)
        if not data.get("success", False):
            raise RuntimeError(f"Contract.search failed: {data}")
        return data.get("contracts", []) or []

    def search_contracts_by_id(self, contract_id: str) -> List[Dict[str, Any]]:
        """
        POST /api/Contract/searchById
        payload: { "contractId": <str> }
        """
        payload = {"contractId": contract_id}
        data = self._post("/api/Contract/searchById", payload, timeout=15)
        if not data.get("success", False):
            raise RuntimeError(f"Contract.searchById failed: {data}")
        return data.get("contracts", []) or []

    # ------------- History / Bars -------------

    def retrieve_bars(
        self,
        contract_id: str,
        live: bool,
        unit: int,
        unit_number: int,
        include_partial: bool = False,
        limit: int = 400,
        lookback_days: Optional[int] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """
        POST /api/History/retrieveBars
        payload (sin wrapper "request"):
          {
            "contractId": "CON.F.US.MNQ.U25",
            "live": false,
            "unit": 2,                    # 2=Minute
            "unitNumber": 15,             # 15-minute bars
            "startTime": "...Z",
            "endTime":   "...Z",
            "limit": 400,
            "includePartialBar": false
          }
        - Si no se pasan start_time/end_time, se calculan por lookback_days (por defecto 7-14d).
        """
        if end_time is None:
            end_time = datetime.now(timezone.utc)
        if start_time is None:
            days = lookback_days if (lookback_days and lookback_days > 0) else 7
            start_time = end_time - timedelta(days=days)

        payload = {
            "contractId": contract_id,
            "live": bool(live),
            "unit": int(unit),
            "unitNumber": int(unit_number),
            "startTime": _iso_z(start_time),
            "endTime": _iso_z(end_time),
            "limit": int(limit),
            "includePartialBar": bool(include_partial),
        }
        data = self._post("/api/History/retrieveBars", payload, timeout=30)
        # formato esperado: { success: bool, bars: [...] }
        if not data.get("success", False):
            raise RuntimeError(f"retrieveBars failed: {data}")
        bars = data.get("bars") or []
        return bars

    # ------------- Orders / Trades -------------

    def place_order(self, **kwargs) -> Dict[str, Any]:
        """
        POST /api/Order/place
        Acepta kwargs en snake_case y los mapea a camelCase. Soporta campos:
          - account_id / accountId (int)
          - contract_id / contractId (str)
          - type / order_type (int)    # 1=Limit, 2=Market, 4=Stop, 5=Trailing...
          - side (int)                 # 0=Buy, 1=Sell
          - size (int)
          - limit_price / limitPrice (float)
          - stop_price  / stopPrice  (float)
          - trail_price / trailPrice (float)
          - custom_tag  / customTag  (str)
          - linked_order_id / linkedOrderId (int)
        """
        # normalizar nombres
        mapping = {
            "account_id": "accountId",
            "contract_id": "contractId",
            "order_type": "type",
            "type": "type",
            "side": "side",
            "size": "size",
            "limit_price": "limitPrice",
            "stop_price": "stopPrice",
            "trail_price": "trailPrice",
            "custom_tag": "customTag",
            "linked_order_id": "linkedOrderId",
        }
        payload: Dict[str, Any] = {}
        for k, v in kwargs.items():
            kk = mapping.get(k, k)
            payload[kk] = v

        # sanity required
        req = ["accountId", "contractId", "type", "side", "size"]
        for k in req:
            if k not in payload:
                raise ValueError(f"place_order missing field: {k}")

        data = self._post("/api/Order/place", payload, timeout=15)
        if not data.get("success", False):
            raise RuntimeError(f"order.place failed: {data}")
        return data

    def cancel_order(self, account_id: int, order_id: int) -> Dict[str, Any]:
        """
        POST /api/Order/cancel
        payload: { "accountId": <int>, "orderId": <int> }
        """
        payload = {"accountId": int(account_id), "orderId": int(order_id)}
        data = self._post("/api/Order/cancel", payload, timeout=10)
        if not data.get("success", False):
            raise RuntimeError(f"order.cancel failed: {data}")
        return data

    def search_open_orders(self, account_id: int) -> List[Dict[str, Any]]:
        """
        POST /api/Order/searchOpen
        payload: { "accountId": <int> }
        """
        payload = {"accountId": int(account_id)}
        data = self._post("/api/Order/searchOpen", payload, timeout=15)
        if not data.get("success", False):
            raise RuntimeError(f"order.searchOpen failed: {data}")
        return data.get("orders", []) or []

    def search_orders(self, account_id: int, start_iso: str, end_iso: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        POST /api/Order/search
        payload: { "accountId": <int>, "startTimestamp": <iso>, ["endTimestamp": <iso>] }
        """
        payload = {"accountId": int(account_id), "startTimestamp": start_iso}
        if end_iso:
            payload["endTimestamp"] = end_iso
        data = self._post("/api/Order/search", payload, timeout=20)
        if not data.get("success", False):
            raise RuntimeError(f"order.search failed: {data}")
        return data.get("orders", []) or []

    def search_trades(self, account_id: int, start_iso: str, end_iso: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        POST /api/Trade/search
        payload: { "accountId": <int>, "startTimestamp": <iso>, ["endTimestamp": <iso>] }
        """
        payload = {"accountId": int(account_id), "startTimestamp": start_iso}
        if end_iso:
            payload["endTimestamp"] = end_iso
        data = self._post("/api/Trade/search", payload, timeout=20)
        if not data.get("success", False):
            raise RuntimeError(f"trade.search failed: {data}")
        return data.get("trades", []) or []
