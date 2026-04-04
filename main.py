"""
EdgeRunner — Main Orchestrator.

This is the entry point. It wires all modules together and runs the
autonomous trading agent as a set of concurrent asyncio tasks:

1. Kalshi WebSocket feed (orderbook updates)
2. NBA data poller (player stats, live games)
3. Smart money tracker (Polymarket top trader positions)
4. Signal evaluator (checks queue for opportunities, calls Claude)
5. Watchdog (monitors health of all systems)

Run with: python main.py

The agent runs until stopped with Ctrl+C. On shutdown, it:
- Closes all WebSocket and HTTP connections
- Sends a shutdown alert via Telegram
- Logs final status
"""

import asyncio
import signal
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal

from rich.console import Console

from alerts.discord import DiscordAlerter
from config.settings import (
    DEBUG_MODE,
    ORDERBOOK_STALE_THRESHOLD,
    TRADING_MODE,
    console as settings_console,
)
from data.cache import (
    AgentCache,
    NbaGameUpdate,
    NbaStatsUpdate,
    OrderbookUpdate,
    QueueMsg,
    SmartMoneySignal,
    StaleDataAlert,
    get_cache,
)
from data.feeds import KalshiFeed
from data.market_poller import MarketPoller
from data.nba_poller import NbaPoller
from data.smart_money import SmartMoneyTracker
from execution.kalshi_client import KalshiClient
from execution.order_manager import OrderManager
from signals.analyzer import MarketAnalyzer

console = Console()
UTC = timezone.utc

# How often the signal evaluator checks the queue (seconds)
EVAL_INTERVAL: float = 5.0

# How often the watchdog runs health checks (seconds)
WATCHDOG_INTERVAL: float = 10.0

# Tracked NBA market tickers — will be populated from Kalshi API
# For MVP, these are manually configured. In V2, auto-discovered.
DEFAULT_TRACKED_TICKERS: list[str] = [
    # Apr 3 2026 — High-volume game winners and key spreads
    # ORL at DAL (game starting now)
    "KXNBAGAME-26APR03ORLDAL-ORL",
    "KXNBAGAME-26APR03ORLDAL-DAL",
    "KXNBASPREAD-26APR03ORLDAL-ORL7",
    "KXNBASPREAD-26APR03ORLDAL-ORL4",
    # NOP at SAC (game at 7pm PT)
    "KXNBAGAME-26APR03NOPSAC-SAC",
    "KXNBAGAME-26APR03NOPSAC-NOP",
    "KXNBASPREAD-26APR03NOPSAC-SAC7",
    "KXNBASPREAD-26APR03NOPSAC-SAC4",
    # UTA at HOU
    "KXNBAGAME-26APR03UTAHOU-HOU",
    "KXNBAGAME-26APR03UTAHOU-UTA",
]

# Tracked NBA players for stats polling
DEFAULT_TRACKED_PLAYERS: list[dict] = [
    # Key players from tonight's games
    {"name": "Trae Young"},      # ATL
    {"name": "Cam Thomas"},      # BKN
    {"name": "Zach LaVine"},     # CHI
    {"name": "Jalen Brunson"},   # NYK
    {"name": "De'Aaron Fox"},    # SAC
]


class EdgeRunner:
    """
    The main agent orchestrator.

    Initializes all modules, runs them as concurrent asyncio tasks,
    and handles graceful shutdown on Ctrl+C.
    """

    def __init__(self) -> None:
        # Core state
        self._cache: AgentCache = get_cache()
        self._queue: asyncio.Queue[QueueMsg] = asyncio.Queue(maxsize=1000)
        self._running: bool = False
        # Track last analyzed price and time per ticker to avoid redundant Claude calls
        self._last_analyzed_price: dict[str, Decimal] = {}
        self._last_analyzed_time: dict[str, float] = {}
        # Minimum price change (in dollars) before re-analyzing a market
        self._min_price_change: Decimal = Decimal("0.01")
        # Re-analyze even without price change after this many seconds
        self._max_stale_analysis: float = 300.0  # 5 minutes

        # Modules
        self._feed: KalshiFeed = KalshiFeed(
            queue=self._queue,
            cache=self._cache,
            tracked_tickers=DEFAULT_TRACKED_TICKERS,
        )
        self._nba_poller: NbaPoller = NbaPoller(
            queue=self._queue,
            cache=self._cache,
            tracked_players=DEFAULT_TRACKED_PLAYERS,
        )
        self._smart_money: SmartMoneyTracker = SmartMoneyTracker(
            queue=self._queue,
            cache=self._cache,
        )
        self._analyzer: MarketAnalyzer = MarketAnalyzer()
        self._kalshi_client: KalshiClient = KalshiClient()
        self._market_poller: MarketPoller = MarketPoller(
            queue=self._queue,
            cache=self._cache,
            kalshi_client=self._kalshi_client,
            tracked_tickers=DEFAULT_TRACKED_TICKERS,
        )
        self._order_manager: OrderManager = OrderManager(kalshi_client=self._kalshi_client)
        self._alerter: DiscordAlerter = DiscordAlerter()

    async def _signal_evaluator(self) -> None:
        """
        Continuously pull messages from the queue and evaluate for trading opportunities.

        This is the core decision loop:
        1. Pull a message from the queue
        2. If it's an orderbook update with a potential edge, send to Claude
        3. If Claude says trade, pass to order manager
        4. If order executes, send Telegram alert
        """
        console.print("[blue]Signal evaluator: Started.[/blue]")

        while self._running:
            try:
                # Pull from queue with timeout (so we can check self._running)
                try:
                    msg = await asyncio.wait_for(self._queue.get(), timeout=EVAL_INTERVAL)
                except asyncio.TimeoutError:
                    continue

                # Dispatch based on message type
                if isinstance(msg, OrderbookUpdate) and msg.best_bid is not None:
                    await self._evaluate_orderbook_update(msg)
                elif isinstance(msg, SmartMoneySignal):
                    console.print(
                        f"[yellow]Smart money signal: {msg.market_title} — "
                        f"{msg.consensus_side.upper()} ({msg.trader_count} traders)[/yellow]"
                    )
                elif isinstance(msg, StaleDataAlert):
                    await self._alerter.send_error_alert(
                        component=f"Data Feed ({msg.source})",
                        detail=f"No updates since {msg.last_update}",
                        status="PASSing trades until data refreshes.",
                    )

            except Exception as e:
                console.print(
                    f"[red]Signal evaluator error: {type(e).__name__}: {e}[/red]"
                )
                await asyncio.sleep(1)

    async def _evaluate_orderbook_update(self, update: OrderbookUpdate) -> None:
        """
        Evaluate a single orderbook update for a potential trade.

        Only calls Claude when:
        1. Price has actually changed since last analysis (saves ~90% of API calls)
        2. Data is fresh (not stale)
        3. Spread is reasonable (< $0.05)

        This is the key cost optimization — without it, the agent calls Claude
        on every 30-second poll even when nothing has changed.
        """
        orderbook = self._cache.get_orderbook(update.ticker)
        if orderbook is None:
            return

        # Skip if data is stale
        if self._cache.is_orderbook_stale(update.ticker):
            return

        # Skip if spread is too wide (pre-filter before spending API budget)
        if orderbook.spread is not None and orderbook.spread > Decimal("0.05"):
            return

        # Skip if price hasn't changed AND analysis is recent (cost optimization)
        current_price = update.best_bid or Decimal("0")
        last_price = self._last_analyzed_price.get(update.ticker, Decimal("-1"))
        last_time = self._last_analyzed_time.get(update.ticker, 0.0)
        time_since = time.monotonic() - last_time

        price_changed = abs(current_price - last_price) >= self._min_price_change
        analysis_stale = time_since >= self._max_stale_analysis

        if not price_changed and not analysis_stale:
            return

        self._last_analyzed_price[update.ticker] = current_price
        self._last_analyzed_time[update.ticker] = time.monotonic()

        # Get all available player stats
        all_stats_dict = self._cache.get_all_player_stats()
        all_stats_list = list(all_stats_dict.values()) if all_stats_dict else []

        # Get live game data for context
        live_games = self._cache.get_live_games()
        game_data = None
        if live_games:
            # Build game context from any available live game data
            game_info = {}
            for gid, game in live_games.items():
                game_info[f"{game.away_team} @ {game.home_team}"] = (
                    f"{game.status} {game.game_time} | "
                    f"{game.away_team} {game.away_score} - {game.home_team} {game.home_score}"
                )
            if game_info:
                game_data = game_info

        # Get smart money signal if available
        smart_money = None
        signals = self._cache.get_smart_money_signals()
        for title, sig in signals.items():
            # Match by team abbreviation in ticker
            ticker_lower = update.ticker.lower()
            title_lower = title.lower()
            if any(word in ticker_lower for word in title_lower.split() if len(word) > 2):
                smart_money = sig
                break

        # Call Claude with ALL available context
        decision = await self._analyzer.analyze_market(
            ticker=update.ticker,
            title=update.ticker,
            cache=self._cache,
            orderbook=orderbook,
            player_stats=all_stats_list[0] if all_stats_list else None,
            smart_money=smart_money,
            game_data=game_data,
        )

        if not decision.is_actionable:
            return

        # Execute the trade
        trade = await self._order_manager.execute_trade(
            decision=decision,
            cache=self._cache,
            orderbook=orderbook,
        )

        if trade is not None:
            # Send Telegram alert
            bankroll = self._cache.get_bankroll()
            bankroll_pct = (
                float(trade.price * trade.quantity / bankroll * 100)
                if bankroll > 0
                else 0.0
            )
            await self._alerter.send_trade_alert(
                ticker=trade.kalshi_ticker,
                side=trade.side,
                price=trade.price,
                bet_amount=trade.price * trade.quantity,
                bankroll_pct=bankroll_pct,
                rationale=trade.claude_reasoning or "",
                bankroll=bankroll,
                latency_ms=trade.execution_latency_ms or 0,
            )

    async def _watchdog(self) -> None:
        """
        Periodic health check for all systems.

        Monitors:
        - Orderbook data freshness
        - Queue depth
        - Circuit breaker states
        - Budget usage
        """
        console.print("[blue]Watchdog: Started.[/blue]")

        while self._running:
            await asyncio.sleep(WATCHDOG_INTERVAL)

            try:
                # Check for stale orderbook data
                stale = self._cache.get_stale_tickers()
                if stale:
                    console.print(
                        f"[yellow]Watchdog: {len(stale)} stale tickers: "
                        f"{', '.join(stale[:5])}[/yellow]"
                    )

                # Check queue depth
                qsize = self._queue.qsize()
                if qsize > 500:
                    console.print(
                        f"[yellow]Watchdog: Queue depth high ({qsize}/1000). "
                        f"Signal evaluator may be falling behind.[/yellow]"
                    )

                # Check Claude API budget
                analyzer_status = self._analyzer.get_status()
                if analyzer_status["is_over_budget"]:
                    console.print(
                        f"[red]Watchdog: Claude API budget exceeded! "
                        f"${analyzer_status['cumulative_cost']:.2f} spent.[/red]"
                    )

                if DEBUG_MODE:
                    positions = self._cache.get_position_count()
                    bankroll = self._cache.get_bankroll()
                    console.print(
                        f"[dim]Watchdog: Queue={qsize} | Positions={positions} | "
                        f"Bankroll=${bankroll} | "
                        f"Claude=${analyzer_status['cumulative_cost']:.2f} | "
                        f"Cache={analyzer_status['cache_rate_pct']:.0f}%[/dim]"
                    )

            except Exception as e:
                console.print(
                    f"[red]Watchdog error: {type(e).__name__}: {e}[/red]"
                )

    async def _startup(self) -> None:
        """Initialize state from Kalshi and send startup alert."""
        console.print("[blue]EdgeRunner: Initializing...[/blue]")

        # Sync bankroll from Kalshi
        await self._order_manager.sync_bankroll(self._cache)

        # If bankroll is still 0 (API failed), set a default for paper trading
        if self._cache.get_bankroll() == Decimal("0"):
            self._cache.set_bankroll(Decimal("100.00"))
            console.print(
                "[yellow]Bankroll: Using default $100.00 (Kalshi sync failed).[/yellow]"
            )

        # Sync positions from Kalshi
        await self._order_manager.sync_positions(self._cache)

        # Send startup alert
        await self._alerter.send_startup(TRADING_MODE, self._cache.get_bankroll())

        console.print(
            f"[green]EdgeRunner: Ready. Mode={TRADING_MODE.upper()} "
            f"Bankroll=${self._cache.get_bankroll()} "
            f"Tickers={len(DEFAULT_TRACKED_TICKERS)} "
            f"Players={len(DEFAULT_TRACKED_PLAYERS)}[/green]"
        )

    async def _shutdown(self) -> None:
        """Gracefully shut down all modules."""
        console.print("\n[yellow]EdgeRunner: Shutting down...[/yellow]")
        self._running = False

        # Stop all modules
        await self._feed.stop()
        await self._market_poller.stop()
        await self._nba_poller.stop()
        await self._smart_money.stop()
        await self._kalshi_client.close()

        # Send shutdown alert
        await self._alerter.send_shutdown("Manual stop (Ctrl+C)")
        await self._alerter.close()

        # Print final status
        analyzer_status = self._analyzer.get_status()
        order_status = self._order_manager.get_status()

        console.print("\n[bold]Final Status:[/bold]")
        console.print(f"  Bankroll: ${self._cache.get_bankroll()}")
        console.print(f"  Positions: {self._cache.get_position_count()}")
        console.print(f"  Trades executed: {order_status['total_executions']}")
        console.print(f"  Trades rejected: {order_status['total_rejections']}")
        console.print(f"  Claude calls: {analyzer_status['total_calls']}")
        console.print(f"  Claude cost: ${analyzer_status['cumulative_cost']:.4f}")
        console.print(f"  Cache hit rate: {analyzer_status['cache_rate_pct']:.0f}%")

        console.print("[green]EdgeRunner: Shutdown complete.[/green]")

    async def run(self) -> None:
        """
        Main entry point. Runs all tasks concurrently until Ctrl+C.

        Task architecture:
        - feed.run(): WebSocket connection to Kalshi (pushes to queue)
        - nba_poller.run(): Polls NBA APIs (pushes to queue)
        - smart_money.run(): Polls Polymarket leaderboard (pushes to queue)
        - _signal_evaluator(): Reads queue, calls Claude, executes trades
        - _watchdog(): Monitors system health
        """
        self._running = True

        await self._startup()

        try:
            await asyncio.gather(
                self._feed.run(),
                self._market_poller.run(),
                self._nba_poller.run(),
                self._smart_money.run(),
                self._signal_evaluator(),
                self._watchdog(),
                return_exceptions=True,
            )
        except asyncio.CancelledError:
            pass
        finally:
            await self._shutdown()


def main() -> None:
    """Entry point with Ctrl+C handling."""
    agent = EdgeRunner()

    # Handle Ctrl+C gracefully
    loop = asyncio.new_event_loop()

    def _handle_signal() -> None:
        console.print("\n[yellow]Ctrl+C received. Stopping...[/yellow]")
        for task in asyncio.all_tasks(loop):
            task.cancel()

    # Register signal handlers
    if sys.platform != "win32":
        loop.add_signal_handler(signal.SIGINT, _handle_signal)
        loop.add_signal_handler(signal.SIGTERM, _handle_signal)

    try:
        loop.run_until_complete(agent.run())
    except KeyboardInterrupt:
        console.print("\n[yellow]KeyboardInterrupt. Cleaning up...[/yellow]")
        loop.run_until_complete(agent._shutdown())
    finally:
        loop.close()


if __name__ == "__main__":
    main()
