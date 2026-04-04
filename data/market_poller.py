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
        Fetch a single market's orderbook via REST and push to queue.

        Uses the /markets/{ticker}/orderbook endpoint which returns the
        full depth (yes and no side arrays of [price, size] pairs).
        The /markets/{ticker} metadata endpoint does NOT reliably return
        real-time bid/ask on production.
        """
        try:
            orderbook_data = await self._kalshi._request_with_retry(
                "GET", f"/markets/{ticker}/orderbook"
            )
        except Exception:
            return

        if not orderbook_data:
            return

        ob = orderbook_data.get("orderbook_fp", orderbook_data.get("orderbook", {}))
        if not ob:
            return

        # Kalshi orderbook format:
        # "yes_dollars": [[price_str, size_str], ...] sorted LOW to HIGH
        # "no_dollars": [[price_str, size_str], ...] sorted LOW to HIGH
        # Best YES bid = HIGHEST yes price (LAST element)
        # Best YES ask = 1 - HIGHEST no price (LAST element of no side)
        yes_levels = ob.get("yes_dollars", ob.get("yes", []))
        no_levels = ob.get("no_dollars", ob.get("no", []))

        # Best YES bid = highest price on yes side (last element, since sorted low→high)
        best_bid = Decimal(yes_levels[-1][0]) if yes_levels else None
        bid_volume = Decimal(str(sum(float(level[1]) for level in yes_levels[-5:]))) if yes_levels else Decimal("0")

        # Best YES ask = 1 - highest no bid (last element of no side)
        # If someone bids $0.28 for NO, that's asking $0.72 for YES
        best_ask = (Decimal("1") - Decimal(no_levels[-1][0])) if no_levels else None
        ask_volume = Decimal(str(sum(float(level[1]) for level in no_levels[-5:]))) if no_levels else Decimal("0")

        # If one side is missing, infer from the other
        # YES bid missing but ask exists → bid = ask - 0.01 (minimum spread)
        if best_bid is None and best_ask is not None:
            best_bid = max(best_ask - Decimal("0.01"), Decimal("0.01"))
        # YES ask missing but bid exists → ask = bid + 0.01
        if best_ask is None and best_bid is not None:
            best_ask = min(best_bid + Decimal("0.01"), Decimal("0.99"))

        # Skip if no meaningful data
        if best_bid is None and best_ask is None:
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
