"""
Kalshi REST API client for EdgeRunner.

Async HTTP client for placing orders, fetching positions, and checking
balances on Kalshi. Uses RSA-PSS signing for authentication (same crypto
scheme as the WebSocket feed in data/feeds.py).

Key details:
- Uses httpx async client (better for REST with auth headers than aiohttp)
- Prices are FIXED-POINT DOLLAR STRINGS ("0.6500"), not floats
- Paper mode routes to demo-api.kalshi.co (via KALSHI_BASE_URL from settings)
- Handles 429 rate limits with tenacity exponential backoff
- Basic tier: 20 reads/sec, 10 writes/sec

IMPORTANT: This client can only READ data and PLACE/CANCEL orders.
It has NO withdrawal capability — this is a security design choice.
"""

import base64
import time
import uuid
from decimal import Decimal

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from rich.console import Console
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from config.settings import (
    DEBUG_MODE,
    KALSHI_API_KEY_ID,
    KALSHI_BASE_URL,
    KALSHI_PRIVATE_KEY_PATH,
    TRADING_MODE,
)

console = Console()


class KalshiApiError(Exception):
    """Raised when the Kalshi API returns an error response."""

    def __init__(self, status_code: int, message: str) -> None:
        self.status_code = status_code
        self.message = message
        super().__init__(f"Kalshi API {status_code}: {message}")


class KalshiRateLimitError(KalshiApiError):
    """Raised specifically for 429 rate limit responses."""

    pass


class KalshiClient:
    """
    Async REST client for the Kalshi trading API.

    Handles RSA-PSS authentication, order placement, position queries,
    and balance checks. Designed for use with asyncio.

    Usage:
        client = KalshiClient()
        balance = await client.get_balance()
        order = await client.place_order("KXNBA-LEBRON-PTS-O25", "yes", "buy", 5, Decimal("0.42"))
    """

    def __init__(self) -> None:
        self._base_url = KALSHI_BASE_URL.rstrip("/")
        self._api_key_id = KALSHI_API_KEY_ID
        self._key_path = KALSHI_PRIVATE_KEY_PATH
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(15.0))

    # --- RSA-PSS Authentication ---

    def _sign_request(self, timestamp_ms: str, method: str, path: str) -> str:
        """
        Sign a request with RSA-PSS.

        Kalshi REST API requires:
          signature = RSA-PSS-Sign(timestamp_ms + method + path)
        Where method is uppercase (GET, POST, etc.) and path starts with /.

        The private key is loaded from disk on each call. This avoids
        keeping key material in memory longer than necessary.
        """
        key_data = self._key_path.read_bytes()
        private_key = serialization.load_pem_private_key(key_data, password=None)

        message = (timestamp_ms + method.upper() + path).encode("utf-8")
        signature = private_key.sign(
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(signature).decode("utf-8")

    def _build_headers(self, method: str, path: str) -> dict[str, str]:
        """
        Build authenticated headers for a Kalshi API request.

        Three required headers:
        - KALSHI-ACCESS-KEY: your API key ID
        - KALSHI-ACCESS-TIMESTAMP: millisecond timestamp
        - KALSHI-ACCESS-SIGNATURE: RSA-PSS signature
        """
        timestamp_ms = str(int(time.time() * 1000))
        signature = self._sign_request(timestamp_ms, method, path)

        return {
            "KALSHI-ACCESS-KEY": self._api_key_id,
            "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    # --- HTTP Helpers ---

    async def _request(
        self, method: str, path: str, json_data: dict | None = None
    ) -> dict:
        """
        Make an authenticated request to the Kalshi API.

        Raises KalshiRateLimitError on 429, KalshiApiError on other errors.
        """
        url = f"{self._base_url}{path}"
        headers = self._build_headers(method, path)

        if DEBUG_MODE:
            console.print(f"[dim]Kalshi API: {method} {path}[/dim]")

        response = await self._client.request(
            method=method,
            url=url,
            headers=headers,
            json=json_data,
        )

        if response.status_code == 429:
            raise KalshiRateLimitError(429, "Rate limited. Back off.")

        if response.status_code >= 400:
            error_text = response.text[:200]
            raise KalshiApiError(response.status_code, error_text)

        return response.json() if response.text else {}

    @retry(
        retry=retry_if_exception_type(KalshiRateLimitError),
        wait=wait_exponential(multiplier=1, min=1, max=30),
        stop=stop_after_attempt(5),
    )
    async def _request_with_retry(
        self, method: str, path: str, json_data: dict | None = None
    ) -> dict:
        """Request with automatic retry on 429 rate limits."""
        return await self._request(method, path, json_data)

    # --- Trading Operations ---

    async def place_order(
        self,
        ticker: str,
        side: str,
        action: str,
        count: int,
        price: Decimal,
    ) -> dict | None:
        """
        Place an order on Kalshi.

        Args:
            ticker: Market ticker (e.g., "KXNBA-LEBRON-PTS-O25")
            side: "yes" or "no"
            action: "buy" or "sell"
            count: Number of contracts
            price: Price per contract as Decimal (e.g., Decimal("0.42"))

        Returns:
            Order response dict on success, None on failure.

        IMPORTANT: Prices must be fixed-point dollar strings ("0.6500").
        Kalshi rejects requests with float prices.
        """
        # Format price as fixed-point 4-decimal string
        price_str = f"{price:.4f}"

        order_data = {
            "ticker": ticker,
            "side": side,
            "action": action,
            "count": count,
            "type": "limit",
            "yes_price" if side == "yes" else "no_price": price_str,
            "client_order_id": str(uuid.uuid4()),
        }

        console.print(
            f"[green]Kalshi: Placing {action.upper()} {side.upper()} "
            f"x{count} @ ${price_str} on {ticker} "
            f"[{TRADING_MODE.upper()}][/green]"
        )

        try:
            result = await self._request_with_retry("POST", "/portfolio/orders", order_data)
            console.print(f"[green]Kalshi: Order placed successfully.[/green]")
            return result
        except KalshiApiError as e:
            console.print(f"[red]Kalshi order failed: {e}[/red]")
            return None
        except Exception as e:
            console.print(f"[red]Kalshi order error: {type(e).__name__}: {e}[/red]")
            return None

    async def cancel_order(self, order_id: str) -> dict | None:
        """Cancel an open order by ID."""
        try:
            result = await self._request_with_retry("DELETE", f"/portfolio/orders/{order_id}")
            console.print(f"[blue]Kalshi: Order {order_id} cancelled.[/blue]")
            return result
        except KalshiApiError as e:
            console.print(f"[red]Kalshi cancel failed: {e}[/red]")
            return None

    # --- Read Operations ---

    async def get_balance(self) -> Decimal | None:
        """
        Fetch the current account balance.

        Returns the available balance as Decimal, or None on failure.
        """
        try:
            result = await self._request_with_retry("GET", "/portfolio/balance")
            # Kalshi returns balance in cents as integer
            balance_cents = result.get("balance", 0)
            balance = Decimal(str(balance_cents)) / Decimal("100")
            if DEBUG_MODE:
                console.print(f"[dim]Kalshi balance: ${balance}[/dim]")
            return balance
        except KalshiApiError as e:
            console.print(f"[red]Kalshi balance fetch failed: {e}[/red]")
            return None
        except Exception as e:
            console.print(f"[red]Kalshi balance error: {type(e).__name__}: {e}[/red]")
            return None

    async def get_positions(self) -> list[dict]:
        """
        Fetch all current positions.

        Returns a list of position dicts, or empty list on failure.
        """
        try:
            result = await self._request_with_retry("GET", "/portfolio/positions")
            positions = result.get("market_positions", result.get("positions", []))
            if DEBUG_MODE:
                console.print(f"[dim]Kalshi positions: {len(positions)} found[/dim]")
            return positions
        except KalshiApiError as e:
            console.print(f"[red]Kalshi positions fetch failed: {e}[/red]")
            return []
        except Exception as e:
            console.print(f"[red]Kalshi positions error: {type(e).__name__}: {e}[/red]")
            return []

    async def get_market(self, ticker: str) -> dict | None:
        """
        Fetch details for a specific market.

        Returns the market dict, or None on failure.
        """
        try:
            result = await self._request_with_retry("GET", f"/markets/{ticker}")
            return result.get("market", result)
        except KalshiApiError as e:
            if e.status_code == 404:
                console.print(f"[yellow]Kalshi: Market {ticker} not found.[/yellow]")
            else:
                console.print(f"[red]Kalshi market fetch failed: {e}[/red]")
            return None
        except Exception as e:
            console.print(f"[red]Kalshi market error: {type(e).__name__}: {e}[/red]")
            return None

    async def get_markets(
        self, status: str = "open", limit: int = 100, cursor: str | None = None
    ) -> dict:
        """
        Fetch a list of markets with optional filters.

        Returns dict with 'markets' list and 'cursor' for pagination.
        """
        path = f"/markets?status={status}&limit={limit}"
        if cursor:
            path += f"&cursor={cursor}"
        try:
            return await self._request_with_retry("GET", path)
        except Exception as e:
            console.print(f"[red]Kalshi markets error: {type(e).__name__}: {e}[/red]")
            return {"markets": [], "cursor": None}

    # --- Cleanup ---

    async def close(self) -> None:
        """Close the HTTP client."""
        await self._client.aclose()
        console.print("[blue]Kalshi client: Closed.[/blue]")


# =============================================================================
# Standalone test
# =============================================================================

if __name__ == "__main__":
    import asyncio

    async def _test() -> None:
        """
        Test the Kalshi REST client.

        Requires real Kalshi demo API credentials and RSA private key.
        With placeholder credentials, verifies initialization and
        graceful error handling.
        """
        console.print("[bold]Testing execution/kalshi_client.py...[/bold]\n")

        client = KalshiClient()

        # Test 1: Initialization
        console.print("[cyan]1. Initialization:[/cyan]")
        console.print(f"   Base URL: {client._base_url}")
        console.print(f"   API Key: {client._api_key_id[:10]}...")
        console.print(f"   Trading Mode: {TRADING_MODE}")
        console.print("   [green]Initialization OK.[/green]")

        # Test 2: RSA signing (if key exists)
        console.print("\n[cyan]2. RSA-PSS signing test:[/cyan]")
        if client._key_path.is_file():
            try:
                timestamp = str(int(time.time() * 1000))
                sig = client._sign_request(timestamp, "GET", "/portfolio/balance")
                console.print(f"   Signature: {sig[:40]}...")
                console.print("   [green]RSA signing works![/green]")
            except Exception as e:
                console.print(f"   [red]Signing failed: {e}[/red]")
        else:
            console.print(f"   [yellow]Key not found at {client._key_path}. Skipping.[/yellow]")

        # Test 3: Fetch balance
        console.print("\n[cyan]3. Fetch balance (requires real credentials):[/cyan]")
        balance = await client.get_balance()
        if balance is not None:
            console.print(f"   [green]Balance: ${balance}[/green]")
        else:
            console.print("   [yellow]Balance fetch failed (expected with placeholder creds).[/yellow]")

        # Test 4: Fetch positions
        console.print("\n[cyan]4. Fetch positions:[/cyan]")
        positions = await client.get_positions()
        console.print(f"   Positions returned: {len(positions)}")

        # Test 5: Fetch a market
        console.print("\n[cyan]5. Fetch market details:[/cyan]")
        market = await client.get_market("KXNBA-TEST")
        if market:
            console.print(f"   Market: {market.get('title', '?')}")
        else:
            console.print("   [yellow]Market fetch failed (expected with placeholder creds).[/yellow]")

        await client.close()
        console.print("\n[green]execution/kalshi_client.py: Test complete.[/green]")

    asyncio.run(_test())
