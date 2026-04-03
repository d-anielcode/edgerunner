"""
Polymarket smart money tracker for EdgeRunner.

Monitors top Polymarket sports traders' positions via the public Data API
and generates consensus signals when multiple top traders converge on the
same side of an NBA market.

Why this matters: Top Polymarket traders have proven track records (visible
on the leaderboard). When 3+ of them are all betting the same side on an
NBA market, the implied probability is likely mispriced. Since the underlying
events are identical across platforms, these signals apply directly to Kalshi.

Architecture:
- Polls every 10 minutes (configurable via SMART_MONEY_POLL_INTERVAL)
- Step 1: Fetch top 20 sports traders from the leaderboard
- Step 2: For each trader, fetch their current NBA positions
- Step 3: Aggregate positions by market — generate signal when consensus exists
- Step 4: Push SmartMoneySignal to queue and cache

API details:
- Base URL: https://data-api.polymarket.com
- Auth: None required (fully public)
- Rate limit: 1,000 requests / 10 seconds (extremely generous)
- US access: Read-only is NOT geoblocked. Only order placement is blocked.
"""

import asyncio
import re
from collections import defaultdict
from datetime import datetime, timezone

import aiohttp
from rich.console import Console
from rich.table import Table

from config.settings import (
    DEBUG_MODE,
    POLYMARKET_DATA_API,
    SMART_MONEY_MIN_TRADERS,
    SMART_MONEY_POLL_INTERVAL,
)
from data.cache import AgentCache, QueueMsg, SmartMoneySignal

console = Console()
UTC = timezone.utc

# NBA team names and keywords for filtering positions
NBA_KEYWORDS: list[str] = [
    "nba", "basketball",
    # Teams
    "hawks", "celtics", "nets", "hornets", "bulls", "cavaliers", "mavericks",
    "nuggets", "pistons", "warriors", "rockets", "pacers", "clippers", "lakers",
    "grizzlies", "heat", "bucks", "timberwolves", "pelicans", "knicks", "thunder",
    "magic", "76ers", "sixers", "suns", "trail blazers", "blazers", "kings",
    "spurs", "raptors", "jazz", "wizards",
    # Common player last names (top stars most likely in prop markets)
    "lebron", "james", "curry", "durant", "giannis", "antetokounmpo", "jokic",
    "luka", "doncic", "tatum", "embiid", "booker", "morant", "edwards",
    "brunson", "haliburton", "shai", "gilgeous", "wembanyama",
]

# Compiled regex for efficient matching
NBA_PATTERN: re.Pattern[str] = re.compile(
    "|".join(re.escape(kw) for kw in NBA_KEYWORDS),
    re.IGNORECASE,
)


def _is_nba_market(title: str) -> bool:
    """Check if a Polymarket market title is NBA-related."""
    return bool(NBA_PATTERN.search(title))


class SmartMoneyTracker:
    """
    Tracks top Polymarket sports traders' NBA positions.

    Polls the public Polymarket Data API on a fixed interval, identifies
    NBA positions held by top traders, and generates consensus signals
    when multiple traders converge on the same side.

    Usage:
        tracker = SmartMoneyTracker(queue=queue, cache=cache)
        await tracker.run()  # runs until stop() is called
    """

    def __init__(
        self,
        queue: asyncio.Queue[QueueMsg],
        cache: AgentCache,
    ) -> None:
        self._queue = queue
        self._cache = cache
        self._poll_interval = SMART_MONEY_POLL_INTERVAL
        self._min_traders = SMART_MONEY_MIN_TRADERS
        self._session: aiohttp.ClientSession | None = None
        self._running: bool = False
        self._top_wallets: list[dict] = []  # cached leaderboard results

    async def _ensure_session(self) -> aiohttp.ClientSession:
        """Lazily create the aiohttp session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    # --- API Calls ---

    async def _fetch_leaderboard(self) -> list[dict]:
        """
        Fetch top sports traders from the Polymarket leaderboard.

        Endpoint: GET /v1/leaderboard?category=SPORTS&timePeriod=MONTH&orderBy=PNL&limit=20
        Returns list of trader dicts with proxyWallet, userName, pnl, vol.
        """
        session = await self._ensure_session()
        url = f"{POLYMARKET_DATA_API}/v1/leaderboard"
        params = {
            "category": "SPORTS",
            "timePeriod": "MONTH",
            "orderBy": "PNL",
            "limit": 20,
        }

        try:
            async with session.get(
                url, params=params, timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                if resp.status != 200:
                    console.print(
                        f"[yellow]Smart Money: Leaderboard HTTP {resp.status}[/yellow]"
                    )
                    return []
                data = await resp.json()
                # Response may be a list directly or nested under a key
                if isinstance(data, list):
                    return data
                return data.get("data", data.get("leaderboard", []))
        except asyncio.TimeoutError:
            console.print("[yellow]Smart Money: Leaderboard request timed out.[/yellow]")
            return []
        except Exception as e:
            console.print(
                f"[red]Smart Money leaderboard error: {type(e).__name__}: {e}[/red]"
            )
            return []

    async def _fetch_trader_positions(self, wallet: str) -> list[dict]:
        """
        Fetch current positions for a specific trader.

        Endpoint: GET /positions?user={wallet}&limit=50&sizeThreshold=10
        Returns list of position dicts with title, outcome, size, avgPrice, etc.
        """
        session = await self._ensure_session()
        url = f"{POLYMARKET_DATA_API}/positions"
        params = {
            "user": wallet,
            "limit": 50,
            "sizeThreshold": 10,  # minimum $10 position size
            "sortBy": "CURRENT",
            "sortDirection": "DESC",
        }

        try:
            async with session.get(
                url, params=params, timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status != 200:
                    if DEBUG_MODE:
                        console.print(
                            f"[dim]Smart Money: Positions HTTP {resp.status} for {wallet[:10]}...[/dim]"
                        )
                    return []
                data = await resp.json()
                if isinstance(data, list):
                    return data
                return data.get("data", data.get("positions", []))
        except asyncio.TimeoutError:
            return []
        except Exception as e:
            if DEBUG_MODE:
                console.print(
                    f"[dim]Smart Money positions error for {wallet[:10]}...: "
                    f"{type(e).__name__}: {e}[/dim]"
                )
            return []

    # --- Aggregation ---

    def _aggregate_nba_positions(
        self, all_positions: list[tuple[str, str, list[dict]]]
    ) -> list[SmartMoneySignal]:
        """
        Aggregate NBA positions across all tracked traders.

        Groups positions by market title and outcome (Yes/No), then
        generates a SmartMoneySignal when enough traders converge.

        Args:
            all_positions: List of (wallet, username, positions_list) tuples.

        Returns:
            List of SmartMoneySignal for markets with consensus.
        """
        # Group: market_title -> outcome -> list of (username, size, avg_price)
        market_groups: dict[str, dict[str, list[tuple[str, float, float]]]] = defaultdict(
            lambda: defaultdict(list)
        )

        for wallet, username, positions in all_positions:
            for pos in positions:
                title = pos.get("title", "")
                if not title or not _is_nba_market(title):
                    continue

                outcome = pos.get("outcome", "").lower()  # "Yes" or "No"
                if outcome not in ("yes", "no"):
                    continue

                size = float(pos.get("currentValue", pos.get("size", 0)))
                avg_price = float(pos.get("avgPrice", 0))

                market_groups[title][outcome].append((username, size, avg_price))

        # Generate signals for markets with consensus
        signals: list[SmartMoneySignal] = []

        for title, sides in market_groups.items():
            for side, traders in sides.items():
                if len(traders) >= self._min_traders:
                    total_size = sum(t[1] for t in traders)
                    avg_price = (
                        sum(t[2] for t in traders) / len(traders) if traders else 0.0
                    )
                    names = [t[0] for t in traders]

                    signal = SmartMoneySignal(
                        timestamp=datetime.now(UTC),
                        market_title=title,
                        consensus_side=side,
                        trader_count=len(traders),
                        total_size_usd=round(total_size, 2),
                        avg_entry_price=round(avg_price, 4),
                        top_trader_names=names,
                    )
                    signals.append(signal)

        return signals

    # --- Polling Cycle ---

    async def _poll_cycle(self) -> None:
        """
        One full smart money polling cycle.

        1. Fetch leaderboard (reuse cached if <30 min old)
        2. For each top trader, fetch their current positions
        3. Filter for NBA positions
        4. Aggregate and generate consensus signals
        5. Push signals to queue and cache
        """
        # Step 1: Fetch leaderboard
        leaderboard = await self._fetch_leaderboard()
        if not leaderboard:
            return

        self._top_wallets = leaderboard

        if DEBUG_MODE:
            console.print(
                f"[dim]Smart Money: {len(leaderboard)} top sports traders loaded.[/dim]"
            )

        # Step 2: Fetch positions for each trader
        all_positions: list[tuple[str, str, list[dict]]] = []

        for trader in leaderboard:
            wallet = trader.get("proxyWallet", trader.get("wallet", ""))
            username = trader.get("userName", wallet[:10] + "...")

            if not wallet:
                continue

            positions = await self._fetch_trader_positions(wallet)
            if positions:
                all_positions.append((wallet, username, positions))

            # Small delay between API calls to be respectful
            await asyncio.sleep(0.2)

        # Step 3-4: Aggregate and generate signals
        self._cache.clear_smart_money()
        signals = self._aggregate_nba_positions(all_positions)

        # Step 5: Push to queue and cache
        for signal in signals:
            self._cache.update_smart_money(signal)
            await self._queue.put(signal)

        if signals:
            console.print(
                f"[yellow]Smart Money: {len(signals)} NBA consensus signals detected![/yellow]"
            )
            for sig in signals:
                console.print(
                    f"  [yellow]{sig.market_title}: {sig.consensus_side.upper()} "
                    f"({sig.trader_count} traders, ${sig.total_size_usd:,.0f})[/yellow]"
                )
        else:
            console.print(
                f"[blue]Smart Money: Scanned {len(all_positions)} traders — "
                f"no NBA consensus signals this cycle.[/blue]"
            )

    # --- Main Loop ---

    async def run(self) -> None:
        """
        Main entry point. Polls on a fixed interval forever.

        Designed to be passed to asyncio.gather() in main.py.
        """
        self._running = True
        console.print(
            f"[blue]Smart Money: Starting (interval={self._poll_interval}s, "
            f"min_traders={self._min_traders}).[/blue]"
        )

        try:
            while self._running:
                try:
                    await self._poll_cycle()
                except Exception as e:
                    console.print(
                        f"[red]Smart Money cycle error: {type(e).__name__}: {e}[/red]"
                    )

                await asyncio.sleep(self._poll_interval)
        finally:
            if self._session and not self._session.closed:
                await self._session.close()

    async def stop(self) -> None:
        """Signal the tracker to stop and close the HTTP session."""
        self._running = False
        if self._session and not self._session.closed:
            await self._session.close()
        console.print("[blue]Smart Money: Stopped.[/blue]")


# =============================================================================
# Standalone test
# =============================================================================

if __name__ == "__main__":

    async def _test() -> None:
        """
        Test the Polymarket smart money tracker against the live API.

        This test hits the real Polymarket Data API (public, no auth).
        It will show actual top sports traders and their NBA positions.
        """
        from data.cache import get_cache

        console.print("[bold]Testing data/smart_money.py...[/bold]\n")

        cache = get_cache()
        queue: asyncio.Queue[QueueMsg] = asyncio.Queue(maxsize=100)

        tracker = SmartMoneyTracker(queue=queue, cache=cache)

        # Test 1: Fetch leaderboard
        console.print("[cyan]1. Polymarket Sports Leaderboard (Top 10):[/cyan]")
        leaderboard = await tracker._fetch_leaderboard()

        if leaderboard:
            table = Table(title="Top Sports Traders (Monthly PnL)")
            table.add_column("Rank", style="dim")
            table.add_column("Username", style="cyan")
            table.add_column("PnL", style="green")
            table.add_column("Volume", style="yellow")
            table.add_column("Wallet", style="dim")

            for i, trader in enumerate(leaderboard[:10], 1):
                username = trader.get("userName", "???")
                pnl = trader.get("pnl", 0)
                vol = trader.get("vol", 0)
                wallet = trader.get("proxyWallet", trader.get("wallet", "???"))

                pnl_str = f"${pnl:,.0f}" if isinstance(pnl, (int, float)) else str(pnl)
                vol_str = f"${vol:,.0f}" if isinstance(vol, (int, float)) else str(vol)

                table.add_row(str(i), username, pnl_str, vol_str, wallet[:12] + "...")

            console.print(table)
        else:
            console.print("   [yellow]Could not fetch leaderboard.[/yellow]")

        # Test 2: Fetch positions for top trader
        if leaderboard:
            top_trader = leaderboard[0]
            wallet = top_trader.get("proxyWallet", top_trader.get("wallet", ""))
            username = top_trader.get("userName", "???")

            console.print(f"\n[cyan]2. Positions for #{1} trader ({username}):[/cyan]")
            positions = await tracker._fetch_trader_positions(wallet)

            if positions:
                nba_count = 0
                for pos in positions[:15]:
                    title = pos.get("title", "???")
                    outcome = pos.get("outcome", "?")
                    size = pos.get("currentValue", pos.get("size", 0))
                    is_nba = _is_nba_market(title)

                    if is_nba:
                        nba_count += 1
                        console.print(
                            f"   [green]NBA: {title[:60]} | {outcome} | ${size:,.0f}[/green]"
                        )
                    elif DEBUG_MODE:
                        console.print(
                            f"   [dim]{title[:60]} | {outcome} | ${size:,.0f}[/dim]"
                        )

                console.print(
                    f"   Total positions: {len(positions)}, NBA-related: {nba_count}"
                )
            else:
                console.print("   [yellow]No positions found.[/yellow]")

        # Test 3: Full poll cycle
        console.print(f"\n[cyan]3. Full poll cycle (scanning all traders):[/cyan]")
        await tracker._poll_cycle()

        signals = cache.get_smart_money_signals()
        console.print(f"   Queue size: {queue.qsize()}")
        console.print(f"   Smart money signals in cache: {len(signals)}")

        if signals:
            for title, sig in signals.items():
                console.print(
                    f"   [green]{title}: {sig.consensus_side.upper()} "
                    f"({sig.trader_count} traders, ${sig.total_size_usd:,.0f})[/green]"
                )

        # Cleanup
        await tracker.stop()
        console.print("\n[green]data/smart_money.py: Test complete.[/green]")

    asyncio.run(_test())
