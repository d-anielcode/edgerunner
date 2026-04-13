"""
Synchronous Kalshi API client for the EdgeRunner dashboard.

The main trading agent uses async (httpx.AsyncClient), but Flask needs
synchronous calls. This module mirrors the auth logic from
execution/kalshi_client.py using httpx.Client (sync).

Auth: RSA-PSS signature over (timestamp_ms + METHOD + full_path).
"""

import base64
import os
import time
from pathlib import Path
from urllib.parse import urlparse

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Config — load from the agent's .env (one directory up from dashboard/)
# ---------------------------------------------------------------------------
load_dotenv(Path(__file__).parent.parent / ".env")

TRADING_MODE = os.getenv("TRADING_MODE", "paper")
if TRADING_MODE == "live":
    BASE_URL = os.getenv(
        "KALSHI_BASE_URL", "https://api.elections.kalshi.com/trade-api/v2"
    )
else:
    BASE_URL = os.getenv("KALSHI_BASE_URL", "https://demo-api.kalshi.co/trade-api/v2")

API_KEY = os.getenv("KALSHI_API_KEY_ID", "")
KEY_PATH = Path(os.getenv("KALSHI_PRIVATE_KEY_PATH", "keys/demo_private_key.pem"))
# Handle relative paths — resolve against project root
if not KEY_PATH.is_absolute():
    KEY_PATH = Path(__file__).parent.parent / KEY_PATH


class KalshiClient:
    """Synchronous Kalshi REST client with RSA-PSS authentication."""

    def __init__(self) -> None:
        self._base_url = BASE_URL.rstrip("/")
        self._api_key_id = API_KEY
        self._client = httpx.Client(timeout=httpx.Timeout(15.0))

        # Cache the RSA private key once at init
        self._private_key = None
        if KEY_PATH.is_file():
            key_data = KEY_PATH.read_bytes()
            self._private_key = serialization.load_pem_private_key(
                key_data, password=None
            )

    # ------------------------------------------------------------------
    # RSA-PSS signing (identical to execution/kalshi_client.py)
    # ------------------------------------------------------------------

    def _sign_request(self, timestamp_ms: str, method: str, path: str) -> str:
        """
        Sign: timestamp_ms + METHOD + full_path (with /trade-api/v2 prefix).
        Query params are stripped before signing.
        """
        if self._private_key is None:
            raise RuntimeError(
                "RSA private key not loaded. Check KALSHI_PRIVATE_KEY_PATH."
            )

        path_without_query = path.split("?")[0]
        base_path = urlparse(self._base_url).path  # e.g. "/trade-api/v2"
        full_path = base_path + path_without_query

        message = (timestamp_ms + method.upper() + full_path).encode("utf-8")
        signature = self._private_key.sign(
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(signature).decode("utf-8")

    def _build_headers(self, method: str, path: str) -> dict[str, str]:
        """Build the three Kalshi auth headers + content type."""
        timestamp_ms = str(int(time.time() * 1000))
        signature = self._sign_request(timestamp_ms, method, path)
        return {
            "KALSHI-ACCESS-KEY": self._api_key_id,
            "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    # ------------------------------------------------------------------
    # HTTP helper
    # ------------------------------------------------------------------

    def _request(self, method: str, path: str) -> dict:
        """Make an authenticated request; raise on HTTP errors."""
        url = f"{self._base_url}{path}"
        headers = self._build_headers(method, path)
        response = self._client.request(method=method, url=url, headers=headers)

        if response.status_code >= 400:
            raise RuntimeError(
                f"Kalshi API {response.status_code}: {response.text[:300]}"
            )
        return response.json() if response.text else {}

    # ------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------

    def get_balance(self) -> float:
        """GET /portfolio/balance -> available balance in dollars."""
        data = self._request("GET", "/portfolio/balance")
        balance_cents = data.get("balance", 0)
        return round(balance_cents / 100.0, 2)

    def get_positions(self) -> list[dict]:
        """GET /portfolio/positions -> list of open positions."""
        data = self._request("GET", "/portfolio/positions")
        return data.get("market_positions", data.get("positions", []))

    def get_fills(self, limit: int = 100, paginate_all: bool = False) -> list[dict]:
        """GET /portfolio/fills -> list of executed fills.
        If paginate_all=True, fetches ALL fills using cursor pagination.
        """
        if not paginate_all:
            data = self._request("GET", f"/portfolio/fills?limit={limit}")
            return data.get("fills", [])

        all_fills = []
        cursor = None
        for _ in range(50):  # Safety cap at 50 pages
            url = f"/portfolio/fills?limit=200"
            if cursor:
                url += f"&cursor={cursor}"
            data = self._request("GET", url)
            fills = data.get("fills", [])
            if not fills:
                break
            all_fills.extend(fills)
            cursor = data.get("cursor")
            if not cursor:
                break
        return all_fills

    def get_settlements(self, limit: int = 100, paginate_all: bool = False) -> list[dict]:
        """GET /portfolio/settlements -> list of settlement results.
        If paginate_all=True, fetches ALL settlements using cursor pagination.
        """
        if not paginate_all:
            data = self._request("GET", f"/portfolio/settlements?limit={limit}")
            return data.get("settlements", [])

        all_settlements = []
        cursor = None
        for _ in range(50):
            url = f"/portfolio/settlements?limit=200"
            if cursor:
                url += f"&cursor={cursor}"
            data = self._request("GET", url)
            settlements = data.get("settlements", [])
            if not settlements:
                break
            all_settlements.extend(settlements)
            cursor = data.get("cursor")
            if not cursor:
                break
        return all_settlements

    def get_market(self, ticker: str) -> dict:
        """GET /markets/{ticker} -> market details."""
        data = self._request("GET", f"/markets/{ticker}")
        return data.get("market", data)
