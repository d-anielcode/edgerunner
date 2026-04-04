"""
Kalshi market REST poller for EdgeRunner.

Periodically fetches market prices via Kalshi's REST API as a fallback
when the WebSocket orderbook feed is empty or unavailable. This is
especially important on the demo environment where WS data is sparse.

Polls tracked tickers every 30 seconds, extracts bid/ask/volume,
updates the cache, and pushes OrderbookUpdate messages to the queue.

Works alongside data/feeds.py (WebSocket) — whichever source provides
data first wins. The cache just stores the latest state regardless of source.
"""

import asyncio
from datetime import datetime, timezone
from decimal import Decimal

from rich.console import Console

from config.settings import DEBUG_MODE
from data.cache import AgentCache, OrderbookUpdate, QueueMsg
from execution.kalshi_client import KalshiClient

console = Console()
UTC = timezone.utc

# Poll interval in seconds (60s balances freshness vs API rate limits)
MARKET_POLL_INTERVAL: float = 60.0


class MarketPoller:
    """
    Periodically fetches market prices via Kalshi REST API.

    Fallback data source when the WebSocket feed is empty.
    Pushes OrderbookUpdate messages to the shared queue, same
    as the WebSocket feed would.

    Usage:
        poller = MarketPoller(queue=queue, cache=cache, kalshi=client, tickers=[...])
        await poller.run()
    """

    def __init__(
        self,
        queue: asyncio.Queue[QueueMsg],
        cache: AgentCache,
        kalshi_client: KalshiClient,
        tracked_tickers: list[str],
    ) -> None:
        self._queue = queue
        self._cache = cache
        self._kalshi = kalshi_client
        self._tracked_tickers = tracked_tickers
        self._running: bool = False

    async def _poll_market(self, ticker: str) -> None:
        """
        Fetch a single market's price data via REST and push to queue.

        Extracts yes_bid, yes_ask, and sizes from the market response.
        """
        market = await self._kalshi.get_market(ticker)
        if market is None:
            return

        # Extract price data — Kalshi returns dollar strings
        yes_bid_str = market.get("yes_bid_dollars", market.get("yes_bid"))
        yes_ask_str = market.get("yes_ask_dollars", market.get("yes_ask"))
        yes_bid_size = market.get("yes_bid_size_fp", market.get("yes_bid_size", "0"))
        yes_ask_size = market.get("yes_ask_size_fp", market.get("yes_ask_size", "0"))

        if not yes_bid_str and not yes_ask_str:
            return

        best_bid = Decimal(str(yes_bid_str)) if yes_bid_str else None
        best_ask = Decimal(str(yes_ask_str)) if yes_ask_str else None
        bid_volume = Decimal(str(yes_bid_size)) if yes_bid_size else Decimal("0")
        ask_volume = Decimal(str(yes_ask_size)) if yes_ask_size else Decimal("0")

        # Skip if no meaningful price data
        if best_bid is None and best_ask is None:
            return
        if best_bid == Decimal("0") and best_ask == Decimal("0"):
            return

        # Update cache (computes OFI) and push to queue
        update = self._cache.update_orderbook(
            ticker=ticker,
            best_bid=best_bid,
            best_ask=best_ask,
            bid_volume=bid_volume,
            ask_volume=ask_volume,
        )

        await self._queue.put(update)

        if DEBUG_MODE:
            console.print(
                f"[dim]Market poll: {ticker[:35]} "
                f"Bid=${best_bid} Ask=${best_ask} "
                f"OFI={update.ofi:+.2f}[/dim]"
            )

    async def _poll_cycle(self) -> None:
        """Poll all tracked markets in sequence with small delays."""
        updated = 0
        for ticker in self._tracked_tickers:
            try:
                await self._poll_market(ticker)
                updated += 1
            except Exception as e:
                if DEBUG_MODE:
                    console.print(
                        f"[dim]Market poll error for {ticker[:30]}: "
                        f"{type(e).__name__}: {e}[/dim]"
                    )
            # Small delay between requests to respect rate limits (20 reads/sec)
            await asyncio.sleep(0.1)

        console.print(
            f"[blue]Market Poller: {updated}/{len(self._tracked_tickers)} "
            f"markets updated via REST.[/blue]"
        )

    async def run(self) -> None:
        """
        Main entry point. Polls on a fixed interval forever.

        Designed to be passed to asyncio.gather() in main.py.
        """
        self._running = True
        console.print(
            f"[blue]Market Poller: Starting "
            f"(interval={MARKET_POLL_INTERVAL}s, "
            f"tickers={len(self._tracked_tickers)}).[/blue]"
        )

        while self._running:
            try:
                await self._poll_cycle()
            except Exception as e:
                console.print(
                    f"[red]Market Poller cycle error: {type(e).__name__}: {e}[/red]"
                )
            await asyncio.sleep(MARKET_POLL_INTERVAL)

    async def stop(self) -> None:
        """Signal the poller to stop."""
        self._running = False
        console.print("[blue]Market Poller: Stopped.[/blue]")


# =============================================================================
# Standalone test
# =============================================================================

if __name__ == "__main__":

    async def _test() -> None:
        from data.cache import get_cache

        console.print("[bold]Testing data/market_poller.py...[/bold]\n")

        cache = get_cache()
        queue: asyncio.Queue[QueueMsg] = asyncio.Queue(maxsize=100)
        kalshi = KalshiClient()

        tickers = [
            "KXNBASPREAD-26APR03CHINYK-CHI2",
            "KXNBASPREAD-26APR03ATLBKN-ATL33",
            "KXNBA1HSPREAD-26APR03NOPSAC-SAC1",
        ]

        poller = MarketPoller(
            queue=queue, cache=cache, kalshi_client=kalshi, tracked_tickers=tickers
        )

        console.print("[cyan]1. Single poll cycle:[/cyan]")
        await poller._poll_cycle()

        console.print(f"\n[cyan]2. Results:[/cyan]")
        console.print(f"   Queue size: {queue.qsize()}")
        for ticker in tickers:
            ob = cache.get_orderbook(ticker)
            if ob:
                console.print(
                    f"   [green]{ticker[:40]} | "
                    f"Bid=${ob.best_bid} Ask=${ob.best_ask} "
                    f"Spread=${ob.spread} OFI={ob.ofi:+.2f}[/green]"
                )
            else:
                console.print(f"   [yellow]{ticker[:40]} | No data[/yellow]")

        await kalshi.close()
        console.print("\n[green]data/market_poller.py: Test complete.[/green]")

    asyncio.run(_test())
