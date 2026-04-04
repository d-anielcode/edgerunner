"""
In-memory cache and queue message types for EdgeRunner.

This module is the central state store for the entire agent. It holds:
- Current orderbook state (top bids/asks per market)
- NBA player stats and live game data
- Bankroll and open positions
- Order Flow Imbalance (OFI) calculations

It also defines the typed queue messages that flow between modules.
feeds.py and nba_poller.py WRITE to the cache and push messages to
the asyncio.Queue. The signal evaluator (built later) READS from both.

Why build this first? Both feeds.py and nba_poller.py depend on it.
"""

import asyncio
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict
from rich.console import Console
from rich.table import Table

from config.settings import ORDERBOOK_STALE_THRESHOLD
from storage.models import Position

console = Console()

UTC = timezone.utc


# =============================================================================
# QUEUE MESSAGE TYPES
# =============================================================================
# Every item pushed to the shared asyncio.Queue must be one of these types.
# The signal evaluator dispatches on msg_type to decide what to do.


class QueueMsg(BaseModel):
    """Base class for all queue messages. Discriminated by msg_type."""

    model_config = ConfigDict(frozen=True)

    msg_type: str
    timestamp: datetime


class OrderbookUpdate(QueueMsg):
    """Pushed when the Kalshi orderbook changes for a tracked market."""

    msg_type: Literal["orderbook_update"] = "orderbook_update"
    ticker: str
    best_bid: Decimal | None = None
    best_ask: Decimal | None = None
    bid_volume: Decimal = Decimal("0")
    ask_volume: Decimal = Decimal("0")
    ofi: float = 0.0  # Order Flow Imbalance: (V_bid - V_ask) / (V_bid + V_ask)


class TradeUpdate(QueueMsg):
    """Pushed when a trade executes on the Kalshi exchange."""

    msg_type: Literal["trade_update"] = "trade_update"
    ticker: str
    price: Decimal
    count: int = 0
    taker_side: str = ""  # "yes" or "no"


class NbaStatsUpdate(QueueMsg):
    """Pushed when player stats are refreshed from NBA data APIs."""

    msg_type: Literal["nba_stats"] = "nba_stats"
    player_name: str
    player_id: int
    team: str = ""  # Team abbreviation (e.g., "BOS", "LAL", "PHI")
    season_avg_pts: float = 0.0
    season_avg_reb: float = 0.0
    season_avg_ast: float = 0.0
    recent_game_pts: list[float] = []
    recent_game_reb: list[float] = []
    recent_game_ast: list[float] = []
    status: str = "Active"  # "Active", "Out", "Day-To-Day", "Questionable"


class NbaGameUpdate(QueueMsg):
    """Pushed when live game data is refreshed."""

    msg_type: Literal["nba_game"] = "nba_game"
    game_id: int
    home_team: str
    away_team: str
    home_score: int = 0
    away_score: int = 0
    period: int = 0
    status: str = "Scheduled"  # "Scheduled", "In Progress", "Final"
    game_time: str = ""  # e.g. "8:42 Q3"


class SmartMoneySignal(QueueMsg):
    """
    Pushed when multiple top Polymarket traders converge on the same NBA position.

    This is a "smart money" consensus signal — when 3+ top sports traders
    are all betting the same side on an NBA market, it suggests the true
    probability differs from what the market is pricing.
    """

    msg_type: Literal["smart_money"] = "smart_money"
    market_title: str  # Polymarket market title (e.g., "Lakers vs Celtics")
    consensus_side: str  # "yes" or "no" — which side top traders favor
    trader_count: int  # How many top traders hold this position
    total_size_usd: float  # Combined position size in USD
    avg_entry_price: float  # Average entry price across traders
    top_trader_names: list[str] = []  # Usernames of traders in this position


class StaleDataAlert(QueueMsg):
    """Pushed when a data source hasn't updated within the threshold."""

    msg_type: Literal["stale_data"] = "stale_data"
    source: str  # "orderbook" or "nba"
    last_update: datetime


# =============================================================================
# ORDERBOOK ENTRY — One market's top-of-book snapshot
# =============================================================================


class OrderbookEntry:
    """
    Snapshot of one market's top-of-book state.

    This is NOT a Pydantic model — it's a mutable data class that gets
    updated in-place on every WebSocket tick for performance.
    """

    def __init__(self, ticker: str) -> None:
        self.ticker: str = ticker
        self.best_bid: Decimal | None = None
        self.best_ask: Decimal | None = None
        self.bid_volume: Decimal = Decimal("0")
        self.ask_volume: Decimal = Decimal("0")
        self.ofi: float = 0.0
        self.last_updated: float = time.monotonic()

    @property
    def spread(self) -> Decimal | None:
        """Bid-ask spread in dollars. None if either side is missing."""
        if self.best_bid is not None and self.best_ask is not None:
            return self.best_ask - self.best_bid
        return None


# =============================================================================
# AGENT CACHE — Central in-memory state
# =============================================================================


class AgentCache:
    """
    Central in-memory state store for the entire agent.

    Holds orderbook snapshots, player stats, live games, bankroll,
    and open positions. All accessor methods are synchronous (no I/O)
    since this is pure in-memory data.
    """

    def __init__(self) -> None:
        self._orderbooks: dict[str, OrderbookEntry] = {}
        self._player_stats: dict[int, NbaStatsUpdate] = {}
        self._live_games: dict[int, NbaGameUpdate] = {}
        self._bankroll: Decimal = Decimal("0")
        self._positions: dict[str, Position] = {}
        self._smart_money: dict[str, SmartMoneySignal] = {}

    # --- Orderbook ---

    def update_orderbook(
        self,
        ticker: str,
        best_bid: Decimal | None,
        best_ask: Decimal | None,
        bid_volume: Decimal,
        ask_volume: Decimal,
    ) -> OrderbookUpdate:
        """
        Update a market's orderbook state and compute OFI.

        Returns an OrderbookUpdate queue message ready to be pushed.
        The OFI (Order Flow Imbalance) quantifies buying vs selling pressure:
          OFI = (V_bid - V_ask) / (V_bid + V_ask)
          OFI > 0.65 → strong buying pressure (price likely going up)
          OFI < -0.65 → strong selling pressure (price likely going down)
        """
        total = bid_volume + ask_volume
        ofi = float((bid_volume - ask_volume) / total) if total > 0 else 0.0

        if ticker not in self._orderbooks:
            self._orderbooks[ticker] = OrderbookEntry(ticker)

        entry = self._orderbooks[ticker]
        entry.best_bid = best_bid
        entry.best_ask = best_ask
        entry.bid_volume = bid_volume
        entry.ask_volume = ask_volume
        entry.ofi = ofi
        entry.last_updated = time.monotonic()

        return OrderbookUpdate(
            timestamp=datetime.now(UTC),
            ticker=ticker,
            best_bid=best_bid,
            best_ask=best_ask,
            bid_volume=bid_volume,
            ask_volume=ask_volume,
            ofi=ofi,
        )

    def get_orderbook(self, ticker: str) -> OrderbookEntry | None:
        """Get the current orderbook state for a market. None if not tracked."""
        return self._orderbooks.get(ticker)

    def get_all_orderbooks(self) -> dict[str, OrderbookEntry]:
        """Get all tracked orderbook states."""
        return dict(self._orderbooks)

    def is_orderbook_stale(self, ticker: str) -> bool:
        """
        Check if a market's orderbook data is stale.

        Stale = no update received within ORDERBOOK_STALE_THRESHOLD seconds.
        The watchdog uses this to detect silent WebSocket disconnects.
        """
        entry = self._orderbooks.get(ticker)
        if entry is None:
            return True
        return (time.monotonic() - entry.last_updated) > ORDERBOOK_STALE_THRESHOLD

    def get_stale_tickers(self) -> list[str]:
        """Get all tickers with stale orderbook data."""
        return [t for t in self._orderbooks if self.is_orderbook_stale(t)]

    # --- NBA Player Stats ---

    def update_player_stats(self, update: NbaStatsUpdate) -> None:
        """Store or update player stats in the cache."""
        self._player_stats[update.player_id] = update

    def get_player_stats(self, player_id: int) -> NbaStatsUpdate | None:
        """Get cached stats for a player. None if not tracked."""
        return self._player_stats.get(player_id)

    def get_all_player_stats(self) -> dict[int, NbaStatsUpdate]:
        """Get all cached player stats."""
        return dict(self._player_stats)

    # --- NBA Live Games ---

    def update_live_game(self, update: NbaGameUpdate) -> None:
        """Store or update a live game in the cache."""
        self._live_games[update.game_id] = update

    def get_live_games(self) -> dict[int, NbaGameUpdate]:
        """Get all cached live games."""
        return dict(self._live_games)

    # --- Bankroll ---

    def set_bankroll(self, amount: Decimal) -> None:
        """Set the current bankroll amount."""
        self._bankroll = amount

    def get_bankroll(self) -> Decimal:
        """Get the current bankroll amount."""
        return self._bankroll

    # --- Positions ---

    def update_position(self, position: Position) -> None:
        """Add or update an open position."""
        self._positions[position.kalshi_ticker] = position

    def remove_position(self, ticker: str) -> None:
        """Remove a closed position."""
        self._positions.pop(ticker, None)

    def get_positions(self) -> dict[str, Position]:
        """Get all open positions."""
        return dict(self._positions)

    # --- Smart Money ---

    def update_smart_money(self, signal: SmartMoneySignal) -> None:
        """Store or update a smart money signal, keyed by market title."""
        self._smart_money[signal.market_title] = signal

    def get_smart_money_signals(self) -> dict[str, SmartMoneySignal]:
        """Get all current smart money signals."""
        return dict(self._smart_money)

    def clear_smart_money(self) -> None:
        """Clear all smart money signals (called before each refresh cycle)."""
        self._smart_money.clear()

    def get_position_count(self) -> int:
        """Get the number of open positions."""
        return len(self._positions)


# =============================================================================
# MODULE SINGLETON
# =============================================================================

_cache: AgentCache | None = None


def get_cache() -> AgentCache:
    """
    Get the global AgentCache singleton.

    Same lazy-init pattern as storage/supabase_client.py.
    """
    global _cache
    if _cache is None:
        _cache = AgentCache()
    return _cache


# =============================================================================
# Standalone test
# =============================================================================

if __name__ == "__main__":
    console.print("[bold]Testing data/cache.py...[/bold]\n")

    cache = get_cache()

    # Test OFI calculation
    console.print("[cyan]1. Orderbook update + OFI calculation:[/cyan]")
    update = cache.update_orderbook(
        ticker="KXNBA-LEBRON-PTS-O25",
        best_bid=Decimal("0.42"),
        best_ask=Decimal("0.45"),
        bid_volume=Decimal("150"),
        ask_volume=Decimal("50"),
    )
    console.print(f"   Ticker: {update.ticker}")
    console.print(f"   Bid: ${update.best_bid} | Ask: ${update.best_ask}")
    console.print(f"   OFI: {update.ofi:.3f} (positive = buying pressure)")
    assert update.ofi == 0.5, f"Expected OFI=0.5, got {update.ofi}"
    console.print("   [green]OFI calculation correct (0.5)[/green]")

    # Test spread calculation
    entry = cache.get_orderbook("KXNBA-LEBRON-PTS-O25")
    assert entry is not None
    assert entry.spread == Decimal("0.03")
    console.print(f"   Spread: ${entry.spread}")
    console.print("   [green]Spread calculation correct ($0.03)[/green]")

    # Test staleness detection
    console.print("\n[cyan]2. Staleness detection:[/cyan]")
    assert not cache.is_orderbook_stale("KXNBA-LEBRON-PTS-O25")
    console.print("   [green]Fresh data correctly detected as non-stale[/green]")
    assert cache.is_orderbook_stale("NONEXISTENT-TICKER")
    console.print("   [green]Unknown ticker correctly detected as stale[/green]")

    # Test player stats
    console.print("\n[cyan]3. Player stats:[/cyan]")
    stats = NbaStatsUpdate(
        timestamp=datetime.now(UTC),
        player_name="LeBron James",
        player_id=2544,
        season_avg_pts=27.1,
        season_avg_reb=7.3,
        season_avg_ast=8.0,
        recent_game_pts=[30.0, 25.0, 32.0, 28.0, 22.0],
        status="Active",
    )
    cache.update_player_stats(stats)
    retrieved = cache.get_player_stats(2544)
    assert retrieved is not None
    assert retrieved.player_name == "LeBron James"
    console.print(f"   {retrieved.player_name}: {retrieved.season_avg_pts} PPG")
    console.print("   [green]Player stats stored and retrieved correctly[/green]")

    # Test live games
    console.print("\n[cyan]4. Live games:[/cyan]")
    game = NbaGameUpdate(
        timestamp=datetime.now(UTC),
        game_id=12345,
        home_team="LAL",
        away_team="BOS",
        home_score=95,
        away_score=102,
        period=3,
        status="In Progress",
        game_time="8:42 Q3",
    )
    cache.update_live_game(game)
    games = cache.get_live_games()
    assert 12345 in games
    console.print(f"   {game.away_team} {game.away_score} @ {game.home_team} {game.home_score} — {game.game_time}")
    console.print("   [green]Live game stored and retrieved correctly[/green]")

    # Test bankroll
    console.print("\n[cyan]5. Bankroll:[/cyan]")
    cache.set_bankroll(Decimal("100.00"))
    assert cache.get_bankroll() == Decimal("100.00")
    console.print(f"   Bankroll: ${cache.get_bankroll()}")
    console.print("   [green]Bankroll set and retrieved correctly[/green]")

    # Test positions
    console.print("\n[cyan]6. Positions:[/cyan]")
    pos = Position(
        kalshi_ticker="KXNBA-LEBRON-PTS-O25",
        side="yes",
        avg_price=Decimal("0.42"),
        quantity=Decimal("5"),
    )
    cache.update_position(pos)
    assert cache.get_position_count() == 1
    cache.remove_position("KXNBA-LEBRON-PTS-O25")
    assert cache.get_position_count() == 0
    console.print("   [green]Position add/remove working correctly[/green]")

    # Test queue message types
    console.print("\n[cyan]7. Queue message types:[/cyan]")
    msgs = [
        update,
        TradeUpdate(timestamp=datetime.now(UTC), ticker="KXNBA-TEST", price=Decimal("0.55"), count=10, taker_side="yes"),
        stats,
        game,
        StaleDataAlert(timestamp=datetime.now(UTC), source="orderbook", last_update=datetime.now(UTC)),
    ]
    table = Table(title="Queue Message Types")
    table.add_column("Type", style="cyan")
    table.add_column("Class", style="green")
    for msg in msgs:
        table.add_row(msg.msg_type, type(msg).__name__)
    console.print(table)

    console.print("\n[green]data/cache.py: All tests passed.[/green]")
