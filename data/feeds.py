"""
Kalshi WebSocket consumer for EdgeRunner.

Connects to Kalshi's WebSocket feed, authenticates using RSA-PSS
signing, and streams live orderbook updates and trades. This is the
agent's "eyes" — it sees every price change in real time.

Architecture:
- Outer loop: handles reconnection with exponential backoff
- Inner loop: reads messages, updates cache, pushes to queue
- Watchdog: if no message in 30s, force reconnect

IMPORTANT:
- Kalshi WebSocket is READ-ONLY — you cannot place orders via WS.
- The exact JSON message format may vary between demo and production.
  First connection logs raw messages at DEBUG level for verification.
- RSA-PSS auth signs a timestamp with your private key. The private
  key is loaded from disk on each connection (not cached in memory).
"""

import asyncio
import base64
import json
import time
from datetime import datetime, timezone
from decimal import Decimal

import websockets
from websockets.exceptions import ConnectionClosed
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from rich.console import Console

from config.markets import is_nba_market, is_supported_market
from config.settings import (
    DEBUG_MODE,
    KALSHI_API_KEY_ID,
    KALSHI_PRIVATE_KEY_PATH,
    KALSHI_WS_URL,
)
from data.cache import AgentCache, OrderbookUpdate, QueueMsg, TradeUpdate

console = Console()
UTC = timezone.utc


class KalshiFeed:
    """
    Persistent WebSocket connection to Kalshi's live market data feed.

    Handles authentication, subscription, message parsing, OFI calculation,
    and auto-reconnect with exponential backoff. Designed to run forever
    as one of the asyncio.gather() tasks in main.py.

    Usage:
        feed = KalshiFeed(queue=queue, cache=cache, tracked_tickers=[...])
        await feed.run()  # runs until stop() is called
    """

    def __init__(
        self,
        queue: asyncio.Queue[QueueMsg],
        cache: AgentCache,
        tracked_tickers: list[str] | None = None,
    ) -> None:
        self._queue = queue
        self._cache = cache
        self._tracked_tickers = tracked_tickers or []
        self._ws: websockets.ClientConnection | None = None
        self._backoff: float = 1.0
        self._max_backoff: float = 60.0
        self._watchdog_timeout: float = 30.0
        self._last_message_time: float = 0.0
        self._running: bool = False
        self._msg_count: int = 0

    # --- RSA-PSS Authentication ---

    def _sign_timestamp(self, timestamp_ms: str) -> str:
        """
        Sign the WebSocket auth message with RSA-PSS.

        Kalshi WS auth signs: timestamp + "GET" + "/trade-api/ws/v2"
        This follows the same pattern as REST API requests.
        Auth is done via HTTP headers at connection time (not a login message).
        """
        key_data = KALSHI_PRIVATE_KEY_PATH.read_bytes()
        private_key = serialization.load_pem_private_key(key_data, password=None)

        # Parse the WS path from the URL (e.g., "/trade-api/ws/v2")
        from urllib.parse import urlparse
        ws_path = urlparse(KALSHI_WS_URL).path

        message = (timestamp_ms + "GET" + ws_path).encode("utf-8")
        signature = private_key.sign(
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(signature).decode("utf-8")

    def _build_ws_headers(self) -> dict[str, str]:
        """
        Build authenticated headers for the WebSocket handshake.

        Kalshi authenticates WebSocket connections via HTTP headers
        during the initial handshake — NOT via a post-connection login message.
        """
        timestamp_ms = str(int(time.time() * 1000))
        signature = self._sign_timestamp(timestamp_ms)

        return {
            "KALSHI-ACCESS-KEY": KALSHI_API_KEY_ID,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
        }

    # --- Connection + Auth ---

    async def _connect_and_auth(self) -> websockets.ClientConnection:
        """
        Open an authenticated WebSocket connection to Kalshi.

        Auth happens at connection time via HTTP headers in the handshake.
        No login message needed after connecting.
        """
        headers = self._build_ws_headers()

        ws = await websockets.connect(
            KALSHI_WS_URL,
            additional_headers=headers,
            ping_interval=20,
            ping_timeout=10,
            close_timeout=5,
        )

        if DEBUG_MODE:
            console.print("[dim]DEBUG WS: Connected with auth headers.[/dim]")

        return ws

    # --- Subscription ---

    async def _subscribe(self, ws: websockets.ClientConnection) -> None:
        """
        Subscribe to orderbook and ticker channels for tracked markets.

        Rate limit awareness: Kalshi Basic tier = 20 reads/sec.
        We send subscriptions with 50ms gaps (max 20/sec).
        """
        for i, ticker in enumerate(self._tracked_tickers):
            sub_msg = {
                "id": i + 10,
                "cmd": "subscribe",
                "params": {
                    "channels": ["orderbook_delta", "ticker"],
                    "market_ticker": ticker,
                },
            }
            await ws.send(json.dumps(sub_msg))
            await asyncio.sleep(0.05)  # 50ms gap to respect rate limits

        console.print(
            f"[blue]Kalshi WS: Subscribed to {len(self._tracked_tickers)} markets.[/blue]"
        )

    # --- Message Handling ---

    async def _handle_message(self, data: dict) -> None:
        """
        Route incoming WS messages to the appropriate handler.

        Known Kalshi WS message types:
        - orderbook_snapshot / orderbook_delta: top-of-book changes
        - trade: executed trades on the exchange
        - ticker: market price/status updates
        - subscribed: subscription confirmation
        - error: server-side error

        Unknown types are ignored (heartbeats, etc.)
        """
        msg_type = data.get("type", "")

        if msg_type in ("orderbook_snapshot", "orderbook_delta"):
            await self._handle_orderbook(data)
        elif msg_type == "trade":
            await self._handle_trade(data)
        elif msg_type == "ticker":
            await self._handle_ticker(data)
        elif msg_type == "subscribed":
            channel = data.get("msg", {}).get("channel", "?")
            if DEBUG_MODE:
                console.print(f"[dim]DEBUG WS subscribed: {channel}[/dim]")
        elif msg_type == "error":
            console.print(f"[red]Kalshi WS server error: {data}[/red]")
        else:
            if DEBUG_MODE and self._msg_count < 10:
                console.print(f"[dim]DEBUG WS unknown msg type '{msg_type}': {data}[/dim]")

    async def _handle_orderbook(self, data: dict) -> None:
        """
        Process orderbook snapshot or delta.

        Extracts top bid/ask, computes OFI via cache, pushes to queue.
        The exact field names here are based on Kalshi docs — adjust
        after first live connection if the format differs.
        """
        msg = data.get("msg", {})
        ticker = msg.get("market_ticker", "")

        if not ticker or not is_nba_market(ticker):
            return

        # Extract best bid/ask from orderbook data
        # Kalshi format may vary — defensive parsing with defaults
        yes_book = msg.get("yes", [])
        no_book = msg.get("no", [])

        best_bid = Decimal(str(yes_book[0][0])) if yes_book else None
        best_ask = Decimal(str(no_book[0][0])) if no_book else None
        bid_volume = Decimal(str(sum(row[1] for row in yes_book[:5]))) if yes_book else Decimal("0")
        ask_volume = Decimal(str(sum(row[1] for row in no_book[:5]))) if no_book else Decimal("0")

        # Update cache (which computes OFI) and get queue message
        update = self._cache.update_orderbook(
            ticker=ticker,
            best_bid=best_bid,
            best_ask=best_ask,
            bid_volume=bid_volume,
            ask_volume=ask_volume,
        )

        await self._queue.put(update)

    async def _handle_trade(self, data: dict) -> None:
        """Process a trade execution on the exchange."""
        msg = data.get("msg", {})
        ticker = msg.get("market_ticker", "")

        if not ticker or not is_nba_market(ticker):
            return

        trade = TradeUpdate(
            timestamp=datetime.now(UTC),
            ticker=ticker,
            price=Decimal(str(msg.get("price", 0))),
            count=msg.get("count", 0),
            taker_side=msg.get("taker_side", ""),
        )

        await self._queue.put(trade)

    async def _handle_ticker(self, data: dict) -> None:
        """
        Process a ticker update (market price/status change).

        Ticker updates are lighter than full orderbook snapshots —
        they contain the latest yes/no prices and volume.
        """
        msg = data.get("msg", {})
        ticker = msg.get("market_ticker", "")

        if not ticker or not is_nba_market(ticker):
            return

        yes_price = msg.get("yes_price")
        no_price = msg.get("no_price")

        if yes_price is not None:
            # Use ticker price as a simplified orderbook update
            update = self._cache.update_orderbook(
                ticker=ticker,
                best_bid=Decimal(str(yes_price)),
                best_ask=Decimal(str(no_price)) if no_price else None,
                bid_volume=Decimal(str(msg.get("volume", 0))),
                ask_volume=Decimal(str(msg.get("volume", 0))),
            )
            await self._queue.put(update)

    # --- Main Loop ---

    async def _message_loop(self, ws: websockets.ClientConnection) -> None:
        """
        Read messages until disconnect or watchdog timeout.

        The watchdog fires if no message arrives within 30 seconds.
        This catches silent disconnects where the TCP connection stays
        open but Kalshi stops sending data.
        """
        self._ws = ws
        while self._running:
            try:
                raw = await asyncio.wait_for(
                    ws.recv(), timeout=self._watchdog_timeout
                )
                self._last_message_time = time.monotonic()
                self._msg_count += 1

                data = json.loads(raw)
                await self._handle_message(data)

                # Log first few raw messages at DEBUG for format verification
                if DEBUG_MODE and self._msg_count <= 5:
                    console.print(f"[dim]DEBUG WS raw msg #{self._msg_count}: {raw[:200]}[/dim]")

            except asyncio.TimeoutError:
                console.print(
                    "[red]Kalshi WS: Watchdog timeout (30s no data). Reconnecting...[/red]"
                )
                return  # exits to reconnect loop

    async def run(self) -> None:
        """
        Main entry point. Runs forever with auto-reconnect.

        This method is designed to be passed to asyncio.gather() in main.py.
        It never returns unless stop() is called.
        """
        self._running = True

        while self._running:
            try:
                ws = await self._connect_and_auth()
                self._backoff = 1.0  # reset on successful connect
                self._last_message_time = time.monotonic()
                console.print("[blue]Kalshi WS: Connected and authenticated.[/blue]")

                await self._subscribe(ws)
                await self._message_loop(ws)

            except ConnectionError as e:
                console.print(f"[red]Kalshi WS: Auth failed — {e}[/red]")
            except (ConnectionClosed, OSError) as e:
                console.print(
                    f"[red]Kalshi WS: Disconnected — {type(e).__name__}: {e}[/red]"
                )
            except Exception as e:
                console.print(
                    f"[red]Kalshi WS: Unexpected error — {type(e).__name__}: {e}[/red]"
                )
            finally:
                if self._ws:
                    try:
                        await self._ws.close()
                    except Exception:
                        pass
                    self._ws = None

            if not self._running:
                break

            # Exponential backoff before reconnect
            console.print(
                f"[yellow]Kalshi WS: Reconnecting in {self._backoff:.0f}s...[/yellow]"
            )
            await asyncio.sleep(self._backoff)
            self._backoff = min(self._backoff * 2, self._max_backoff)

    async def stop(self) -> None:
        """Signal the feed to stop and close the WebSocket."""
        self._running = False
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None
        console.print("[blue]Kalshi WS: Stopped.[/blue]")


# =============================================================================
# Standalone test
# =============================================================================

if __name__ == "__main__":

    async def _test() -> None:
        """
        Test the Kalshi WebSocket feed.

        This test requires:
        1. Valid KALSHI_API_KEY_ID in .env
        2. Valid RSA private key at KALSHI_PRIVATE_KEY_PATH
        3. Network access to the Kalshi demo API

        If credentials are placeholders, the test verifies the signing
        logic works but the connection will fail at auth — that's expected.
        """
        from data.cache import get_cache

        console.print("[bold]Testing data/feeds.py...[/bold]\n")

        cache = get_cache()
        queue: asyncio.Queue[QueueMsg] = asyncio.Queue(maxsize=100)

        # Test 1: RSA signing (if key file exists)
        console.print("[cyan]1. RSA-PSS signing test:[/cyan]")
        feed = KalshiFeed(
            queue=queue,
            cache=cache,
            tracked_tickers=["KXNBA-TEST"],
        )

        if KALSHI_PRIVATE_KEY_PATH.is_file():
            try:
                timestamp = str(int(time.time() * 1000))
                sig = feed._sign_timestamp(timestamp)
                console.print(f"   Timestamp: {timestamp}")
                console.print(f"   Signature: {sig[:40]}...")
                console.print("   [green]RSA-PSS signing works![/green]")
            except Exception as e:
                console.print(f"   [red]Signing failed: {type(e).__name__}: {e}[/red]")
        else:
            console.print(
                f"   [yellow]Key file not found at {KALSHI_PRIVATE_KEY_PATH}. "
                f"Skipping signing test.[/yellow]"
            )

        # Test 2: WebSocket connection (requires real credentials)
        console.print("\n[cyan]2. WebSocket connection test:[/cyan]")
        console.print(f"   URL: {KALSHI_WS_URL}")

        try:
            ws = await feed._connect_and_auth()
            console.print("   [green]Connected and authenticated![/green]")

            # Read a few messages
            console.print("\n[cyan]3. Reading first 5 messages:[/cyan]")
            for i in range(5):
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                    data = json.loads(raw)
                    console.print(f"   [{i+1}] type={data.get('type', '?')}: {str(raw)[:100]}")
                except asyncio.TimeoutError:
                    console.print(f"   [{i+1}] No message within 5s (normal if no active markets)")
                    break

            await ws.close()
            console.print("   [green]Connection closed cleanly.[/green]")

        except ConnectionError as e:
            console.print(f"   [yellow]Auth failed: {e}[/yellow]")
            console.print(
                "   [yellow]This is expected with placeholder credentials. "
                "Set real Kalshi demo API keys in .env to test.[/yellow]"
            )
        except Exception as e:
            console.print(f"   [yellow]Connection failed: {type(e).__name__}: {e}[/yellow]")
            console.print(
                "   [yellow]This is expected with placeholder credentials.[/yellow]"
            )

        console.print("\n[green]data/feeds.py: Test complete.[/green]")

    asyncio.run(_test())
