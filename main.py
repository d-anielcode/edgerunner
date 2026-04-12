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
import os
import signal
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal

from rich.console import Console

from alerts.discord import DiscordAlerter
from config.settings import (
    DEBUG_MODE,
    ENABLE_NBA_POLLER,
    MAX_POSITION_PCT,
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
from execution.arbitrage import ArbitrageScanner
from execution.brier_tracker import BrierTracker
from execution.decision_log import log_decision, classify_market_type
from execution.position_monitor import PositionMonitor
from execution.risk_gates import RiskGates
from signals.rules import RulesEvaluator

console = Console()
UTC = timezone.utc

# How often the signal evaluator checks the queue (seconds)
EVAL_INTERVAL: float = 5.0

# How often the watchdog runs health checks (seconds)
WATCHDOG_INTERVAL: float = 10.0

# Tracked NBA market tickers — will be populated from Kalshi API
# For MVP, these are manually configured. In V2, auto-discovered.
DEFAULT_TRACKED_TICKERS: list[str] = []
"""
Populated at startup by auto-discovery.
The agent scans Kalshi for all active NBA game and spread markets.
No more manual ticker configuration needed.
"""

# Tracked NBA players for stats polling
DEFAULT_TRACKED_PLAYERS: list[dict] = []
"""
Populated at startup from discovered market tickers.
Player names are extracted from player prop market titles.
"""


def _extract_game_id(ticker: str) -> str | None:
    """
    Extract the game identifier from a Kalshi sports ticker.

    Supports all sports:
      KXNBAGAME-26APR03NOPSAC-NOP → NOPSAC
      KXNHLGAME-26APR06TORSEA-TOR → TORSEA
      KXEPLGAME-26JAN25ARSMUN-ARS → ARSMUN
      KXUFCFIGHT-25NOV15PANTOP-PAN → PANTOP

    The game ID is the 3-6 letter combo that appears after the date segment.
    """
    import re
    # Match: KX<type>-<2-digit year><3-letter month><2-digit day><GAMEID>-<rest>
    match = re.search(r"KX\w+-\d{2}[A-Z]{3}\d{2}([A-Z]{3,8})-", ticker.upper())
    if match:
        return match.group(1)
    return None


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
        # Track starting bankroll for the session — used to cap max bet size
        # so that profits from earlier trades don't inflate position sizes
        self._starting_bankroll: Decimal = Decimal("0")
        # Track when the agent started (for midnight cutoff logic)
        self._start_time: float = time.monotonic()
        # Shutdown reason (set by auto-shutdown or Ctrl+C)
        self._shutdown_reason: str = "Unknown"
        self._running: bool = False
        # Daily recap tracking
        self._daily_trades: int = 0
        self._daily_wins: int = 0
        self._daily_start_bankroll: Decimal = Decimal("0")
        # Live game states from ESPN (updated every 30s by watchdog)
        self._live_game_states: dict = {}
        # Track last analyzed price and time per ticker to avoid redundant Claude calls
        self._last_analyzed_price: dict[str, Decimal] = {}
        self._last_analyzed_time: dict[str, float] = {}
        # Cache market titles and volumes (fetched at startup)
        self._market_titles: dict[str, str] = {}
        self._market_volumes: dict[str, float] = {}
        # Track tickers with resting (unfilled) maker orders to prevent duplicates
        # Maps ticker -> timestamp when order was placed. Expires after RESTING_ORDER_TIMEOUT_SECONDS.
        self._resting_order_times: dict[str, float] = {}
        # Batch race prevention: track how many orders were placed per game THIS cycle.
        # Cleared at the start of each evaluation batch to prevent same-cycle bypass of MAX_PER_GAME.
        self._pending_game_ids: dict[str, int] = {}  # game_id -> count of orders this cycle
        # Track initial discovery price — if price moves >20% from discovery,
        # the event is likely in progress (mid-game price spike)
        # Persisted to disk so restarts don't lose the baseline
        from data.discovery_cache import load_discovery_prices
        self._discovery_prices: dict[str, Decimal] = load_discovery_prices()
        if self._discovery_prices:
            console.print(f"[blue]Loaded {len(self._discovery_prices)} discovery prices from cache.[/blue]")
        # Minimum price change (in dollars) before re-analyzing a market
        self._min_price_change: Decimal = Decimal("0.01")
        # Re-analyze even without price change after this many seconds
        self._max_stale_analysis: float = 300.0  # 5 minutes
        self._risk_mgr = None  # Initialized in _start() after portfolio sync

        # Modules
        self._feed: KalshiFeed = KalshiFeed(
            queue=self._queue,
            cache=self._cache,
            tracked_tickers=DEFAULT_TRACKED_TICKERS,
        )
        self._nba_poller: NbaPoller | None = None
        if ENABLE_NBA_POLLER:
            self._nba_poller = NbaPoller(
                queue=self._queue,
                cache=self._cache,
                tracked_players=DEFAULT_TRACKED_PLAYERS,
            )
        self._smart_money: SmartMoneyTracker = SmartMoneyTracker(
            queue=self._queue,
            cache=self._cache,
        )
        self._rules: RulesEvaluator = RulesEvaluator()
        self._kalshi_client: KalshiClient = KalshiClient()
        self._market_poller: MarketPoller = MarketPoller(
            queue=self._queue,
            cache=self._cache,
            kalshi_client=self._kalshi_client,
            tracked_tickers=DEFAULT_TRACKED_TICKERS,
        )
        self._order_manager: OrderManager = OrderManager(kalshi_client=self._kalshi_client)
        self._alerter: DiscordAlerter = DiscordAlerter()
        self._position_monitor: PositionMonitor = PositionMonitor(
            kalshi_client=self._kalshi_client,
            cache=self._cache,
            analyzer=None,  # No longer using LLM for position management
            alerter=self._alerter,
        )
        self._arb_scanner: ArbitrageScanner = ArbitrageScanner(
            kalshi_client=self._kalshi_client,
            alerter=self._alerter,
            cache=self._cache,
        )
        # 5-gate risk system (initialized in _startup after bankroll sync)
        self._risk_gates: RiskGates | None = None
        self._brier_tracker: BrierTracker = BrierTracker(
            kalshi_client=self._kalshi_client,
            risk_gates=None,  # Set in _startup after risk_gates is initialized
            cache=self._cache,
            alerter=self._alerter,
        )

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

        # Short warmup: wait 30 seconds for market data to load
        # No Claude startup needed — rules engine is instant
        console.print("[blue]Signal evaluator: Loading market data (30s)...[/blue]")
        await asyncio.sleep(30)
        console.print("[green]Signal evaluator: Ready. Rules engine active.[/green]")

        while self._running:
            try:
                # Wait for messages to accumulate, then drain the queue
                # This ensures ALL market poller updates are in cache before evaluating
                try:
                    msg = await asyncio.wait_for(self._queue.get(), timeout=EVAL_INTERVAL)
                except asyncio.TimeoutError:
                    continue

                # Drain all pending messages into a batch (keeps latest per ticker)
                latest_orderbooks: dict[str, OrderbookUpdate] = {}
                if isinstance(msg, OrderbookUpdate) and msg.best_bid is not None:
                    latest_orderbooks[msg.ticker] = msg
                elif isinstance(msg, SmartMoneySignal):
                    console.print(
                        f"[yellow]Smart money signal: {msg.market_title} — "
                        f"{msg.consensus_side.upper()} ({msg.trader_count} traders)[/yellow]"
                    )

                # Drain remaining queue items
                while not self._queue.empty():
                    try:
                        batch_msg = self._queue.get_nowait()
                        if isinstance(batch_msg, OrderbookUpdate) and batch_msg.best_bid is not None:
                            latest_orderbooks[batch_msg.ticker] = batch_msg
                        elif isinstance(batch_msg, SmartMoneySignal):
                            console.print(
                                f"[yellow]Smart money signal: {batch_msg.market_title} — "
                                f"{batch_msg.consensus_side.upper()} ({batch_msg.trader_count} traders)[/yellow]"
                            )
                    except asyncio.QueueEmpty:
                        break

                # Evaluate markets in BATCHES (3-8 per Claude call) to save API cost
                # Filter out markets that don't need analysis first
                markets_to_analyze: list[dict] = []
                for ticker, update in latest_orderbooks.items():
                    market_data = self._prepare_market_for_analysis(update)
                    if market_data is not None:
                        markets_to_analyze.append(market_data)

                # Reset per-cycle game tracking before processing this batch
                self._pending_game_ids.clear()

                # Evaluate each market with rules engine (instant, no API cost)
                for market_data in markets_to_analyze:
                    await self._evaluate_with_rules(market_data)

            except Exception as e:
                console.print(
                    f"[red]Signal evaluator error: {type(e).__name__}: {e}[/red]"
                )
                await asyncio.sleep(1)

    def _prepare_market_for_analysis(self, update: OrderbookUpdate) -> dict | None:
        """
        Pre-filter a market update. Returns a dict ready for Claude if it passes
        all checks, or None if it should be skipped.

        This is the gate that prevents wasteful API calls.
        """
        orderbook = self._cache.get_orderbook(update.ticker)
        from config.markets import is_game_winner as _is_gw2
        _is_gw_ticker = _is_gw2(update.ticker)

        if orderbook is None or orderbook.best_bid is None:
            if _is_gw_ticker:
                console.print(f"[dim]PREP SKIP {update.ticker[:40]}: no cache/bid (ob={orderbook is not None}, bid={orderbook.best_bid if orderbook else 'N/A'})[/dim]")
            return None

        # Skip markets where the game is almost over (< 2 min left in Q4)
        # No point placing new trades on a game that's about to resolve
        game_states = self._live_game_states
        if game_states:
            from data.espn_scores import get_quarter_from_game, parse_clock_minutes
            import re
            match = re.search(r"KXNBA\w*-\d{2}[A-Z]{3}\d{2}([A-Z]{6})", update.ticker.upper())
            if match:
                game_id = match.group(1)
                game = game_states.get(game_id)
                if game:
                    if game.status == "Final":
                        return None  # Game already ended
                    # Block trades when game is almost over:
                    # - Q4 with < 2 min AND score difference > 5 (no OT likely)
                    # - Any OT period (Q5+) with < 1 min left
                    # - Game status indicates it's wrapping up
                    mins_left = parse_clock_minutes(game.clock)
                    score_diff = abs(game.home_score - game.away_score)

                    should_skip = False
                    if game.quarter == 4 and mins_left < 2.0 and score_diff > 5:
                        should_skip = True  # Q4, < 2 min, not close — game is decided
                    elif game.quarter >= 5 and mins_left < 1.0:
                        should_skip = True  # OT with < 1 min — truly ending

                    if should_skip:
                        if DEBUG_MODE:
                            console.print(
                                f"[dim]Skipped {update.ticker[:30]}: Q{game.quarter} {game.clock} "
                                f"diff={score_diff} (game ending)[/dim]"
                            )
                        return None

        # Skip if spread is too wide
        if orderbook.spread is not None and orderbook.spread > Decimal("0.05"):
            if _is_gw_ticker:
                console.print(f"[dim]PREP SKIP {update.ticker[:40]}: spread {orderbook.spread} > 0.05[/dim]")
            return None

        # Skip if price hasn't changed AND analysis is recent
        # Use cache price (always up-to-date) not update price (may be None from WS)
        current_price = orderbook.best_bid or Decimal("0")

        # Track initial discovery price for mid-game detection (persists to disk)
        if update.ticker not in self._discovery_prices and current_price > 0:
            self._discovery_prices[update.ticker] = current_price
            from data.discovery_cache import save_discovery_prices
            save_discovery_prices(self._discovery_prices)

        # Mid-game detection: if price moved >20% from discovery, event is likely live
        # This catches sports without ESPN feeds (UCL, EPL, WTA, UFC, etc.)
        disc_price = self._discovery_prices.get(update.ticker)
        if disc_price and disc_price > 0 and current_price > 0 and _is_gw_ticker:
            price_drift = abs(current_price - disc_price) / disc_price
            if price_drift > Decimal("0.20"):
                console.print(
                    f"[dim]PREP SKIP {update.ticker[:40]}: price drifted {price_drift:.0%} "
                    f"from discovery ${disc_price} -> ${current_price} (likely mid-game)[/dim]"
                )
                return None

        last_price = self._last_analyzed_price.get(update.ticker, Decimal("-1"))
        last_time = self._last_analyzed_time.get(update.ticker, 0.0)
        time_since = time.monotonic() - last_time

        if last_price > 0 and current_price > 0:
            pct_change = abs(current_price - last_price) / last_price
            price_changed = pct_change >= Decimal("0.02")
        else:
            price_changed = abs(current_price - last_price) >= self._min_price_change

        analysis_stale = time_since >= self._max_stale_analysis

        if not price_changed and not analysis_stale:
            if _is_gw_ticker:
                console.print(f"[dim]PREP SKIP {update.ticker[:40]}: no price change (cur={current_price} last={last_price} stale={time_since:.0f}s)[/dim]")
            return None

        self._last_analyzed_price[update.ticker] = current_price
        self._last_analyzed_time[update.ticker] = time.monotonic()

        # Build game context
        live_games = self._cache.get_live_games()
        game_data = None
        if live_games:
            game_info = {}
            for gid, game in live_games.items():
                game_info[f"{game.away_team} @ {game.home_team}"] = (
                    f"{game.status} {game.game_time} | "
                    f"{game.away_team} {game.away_score} - {game.home_team} {game.home_score}"
                )
            if game_info:
                game_data = game_info

        # Get smart money
        smart_money = None
        signals = self._cache.get_smart_money_signals()
        for title, sig in signals.items():
            ticker_lower = update.ticker.lower()
            title_lower = title.lower()
            if any(word in ticker_lower for word in title_lower.split() if len(word) > 2):
                smart_money = sig
                break

        return {
            "ticker": update.ticker,
            "title": self._market_titles.get(update.ticker, update.ticker),
            "orderbook": orderbook,
            "smart_money": smart_money,
            "game_data": game_data,
            "update": update,
        }

    async def _execute_decision(self, decision, update_ticker: str) -> None:
        """
        Execute a trade decision through the 5-gate risk system.

        ALL trades must pass:
        1. Pre-checks (confidence, data quality, opposite-side, BUY_NO logic)
        2. 5-gate risk system (drawdown, edge, liquidity, concentration, position limit)
        3. Order execution
        4. Decision logging (accepted AND rejected)
        """
        import re

        ticker = decision.target_market_id
        market_type = classify_market_type(ticker)

        if not decision.is_actionable:
            return

        # --- PRE-CHECKS (domain-specific, before risk gates) ---

        # NBA volume filter: skip medium-volume games (sharp trader zone, -37% ROI)
        from config.markets import get_sport as _get_sport_exec
        _exec_sport = _get_sport_exec(ticker)
        if _exec_sport == "NBA":
            _vol = self._market_volumes.get(ticker, 0)
            if 500_000 <= _vol <= 2_000_000:
                await log_decision(
                    ticker=ticker, title=ticker, action=decision.action,
                    edge=decision.edge, kelly_fraction=decision.kelly_fraction,
                    confidence=decision.confidence_score, rationale=decision.rationale,
                    market_prob=decision.implied_market_probability,
                    agent_prob=decision.agent_calculated_probability,
                    gate_results="PRE-CHECK", accepted=False,
                    rejection_reason=f"NBA vol filter: {_vol:,.0f} in sharp zone (500K-2M)",
                    market_type=market_type,
                )
                return

        # WNBA volume filter: skip low-volume markets (<100K) — only +7% ROI vs +94% at 100K-500K
        if _exec_sport == "WNBA":
            _vol = self._market_volumes.get(ticker, 0)
            if _vol < 100_000:
                await log_decision(
                    ticker=ticker, title=ticker, action=decision.action,
                    edge=decision.edge, kelly_fraction=decision.kelly_fraction,
                    confidence=decision.confidence_score, rationale=decision.rationale,
                    market_prob=decision.implied_market_probability,
                    agent_prob=decision.agent_calculated_probability,
                    gate_results="PRE-CHECK", accepted=False,
                    rejection_reason=f"WNBA vol filter: {_vol:,.0f} < 100K (low vol = no edge)",
                    market_type=market_type,
                )
                return

        # Confidence floor
        if decision.confidence_score < 0.55:
            await log_decision(
                ticker=ticker, title=ticker, action=decision.action,
                edge=decision.edge, kelly_fraction=decision.kelly_fraction,
                confidence=decision.confidence_score, rationale=decision.rationale,
                market_prob=decision.implied_market_probability,
                agent_prob=decision.agent_calculated_probability,
                gate_results="PRE-CHECK", accepted=False,
                rejection_reason="Confidence < 0.55", market_type=market_type,
            )
            return

        # No-data check
        if decision.rationale:
            no_data_phrases = [
                "not in available", "no player data", "missing player data",
                "without.*stats", "insufficient.*data", "limited player data",
            ]
            rationale_lower = decision.rationale.lower()
            for phrase in no_data_phrases:
                if re.search(phrase, rationale_lower):
                    await log_decision(
                        ticker=ticker, title=ticker, action=decision.action,
                        edge=decision.edge, kelly_fraction=decision.kelly_fraction,
                        confidence=decision.confidence_score, rationale=decision.rationale,
                        market_prob=decision.implied_market_probability,
                        agent_prob=decision.agent_calculated_probability,
                        gate_results="PRE-CHECK", accepted=False,
                        rejection_reason=f"No data: '{phrase}'", market_type=market_type,
                    )
                    return

        # Duplicate and opposite-side block (includes resting orders)
        existing_positions = self._cache.get_positions()
        if ticker in existing_positions:
            existing_side = existing_positions[ticker].side
            new_side = "yes" if decision.action == "BUY_YES" else "no"
            if existing_side == new_side:
                # Already holding this exact position — don't double up
                return
        # Block if we have a resting (unfilled) maker order on this ticker
        import time as _time
        from config.settings import RESTING_ORDER_TIMEOUT_SECONDS
        if ticker in self._resting_order_times:
            elapsed = _time.monotonic() - self._resting_order_times[ticker]
            if elapsed < RESTING_ORDER_TIMEOUT_SECONDS:
                return  # Order still resting, don't duplicate
            else:
                del self._resting_order_times[ticker]  # Expired, allow new order
            if existing_side != new_side:
                await log_decision(
                    ticker=ticker, title=ticker, action=decision.action,
                    edge=decision.edge, kelly_fraction=decision.kelly_fraction,
                    confidence=decision.confidence_score, rationale=decision.rationale,
                    market_prob=decision.implied_market_probability,
                    agent_prob=decision.agent_calculated_probability,
                    gate_results="PRE-CHECK", accepted=False,
                    rejection_reason="Opposite-side auto-net block", market_type=market_type,
                )
                return

        # BUY_NO probability direction
        if decision.action == "BUY_NO" and decision.agent_calculated_probability > decision.implied_market_probability:
            await log_decision(
                ticker=ticker, title=ticker, action=decision.action,
                edge=decision.edge, kelly_fraction=decision.kelly_fraction,
                confidence=decision.confidence_score, rationale=decision.rationale,
                market_prob=decision.implied_market_probability,
                agent_prob=decision.agent_calculated_probability,
                gate_results="PRE-CHECK", accepted=False,
                rejection_reason="BUY_NO with inconsistent probabilities", market_type=market_type,
            )
            return

        # Brier score category check
        if self._brier_tracker.is_category_flagged(market_type):
            await log_decision(
                ticker=ticker, title=ticker, action=decision.action,
                edge=decision.edge, kelly_fraction=decision.kelly_fraction,
                confidence=decision.confidence_score, rationale=decision.rationale,
                market_prob=decision.implied_market_probability,
                agent_prob=decision.agent_calculated_probability,
                gate_results="BRIER_FLAG", accepted=False,
                rejection_reason=f"Category {market_type} flagged for poor Brier score",
                market_type=market_type,
            )
            return

        # --- 5-GATE RISK SYSTEM ---

        if self._risk_gates is None:
            return

        orderbook = self._cache.get_orderbook(ticker)
        exec_price = Decimal(str(decision.implied_market_probability))
        if decision.action == "BUY_NO":
            exec_price = Decimal("1") - exec_price

        # Estimate bet amount for concentration check
        from execution.risk import calculate_kelly_bet
        kelly = calculate_kelly_bet(
            decision, self._cache.get_bankroll(),
            self._cache.get_position_count(),
            orderbook.spread if orderbook else None,
        )

        game_id = _extract_game_id(ticker)

        # Batch race prevention: block if we already placed an order on this game THIS cycle
        if game_id:
            pending_count = self._pending_game_ids.get(game_id, 0)
            if pending_count >= 2:  # Match MAX_PER_GAME
                console.print(f"[yellow]BATCH SKIP {ticker}: already placed {pending_count} orders on game {game_id} this cycle.[/yellow]")
                return

        # Correlation-aware sizing: if we already hold a position on the same game
        # (e.g., NBA game winner + NBA spread), reduce Kelly by 50% to avoid
        # doubling exposure on a single event outcome.
        if game_id and existing_positions:
            correlated = sum(
                1 for t in existing_positions
                if _extract_game_id(t) == game_id and t != ticker
            )
            if correlated > 0:
                original_kelly = decision.kelly_fraction
                decision.kelly_fraction *= 0.5
                console.print(
                    f"[yellow]Correlation: {correlated} existing position(s) on game {game_id} "
                    f"— Kelly {original_kelly:.4f} -> {decision.kelly_fraction:.4f}[/yellow]"
                )

        gate_result = self._risk_gates.check_all(
            edge=decision.edge,
            exec_price=exec_price,
            spread=orderbook.spread if orderbook else None,
            volume_24h=int(self._market_volumes.get(ticker, 0)),
            depth=int(orderbook.bid_volume + orderbook.ask_volume) if orderbook else 0,
            game_id=game_id,
            positions=existing_positions,
            # Use portfolio value (cash + positions) for drawdown, cash for concentration
            current_bankroll=self._cache.get_portfolio_value(),
            new_bet_amount=kelly.bet_amount if not kelly.rejected else Decimal("0"),
            current_positions=self._cache.get_position_count(),
        )

        if not gate_result.passed:
            console.print(
                f"[yellow]GATE BLOCKED: {ticker[:30]} — {gate_result.rejection_reason[:60]}[/yellow]"
            )
            await log_decision(
                ticker=ticker, title=ticker, action=decision.action,
                edge=decision.edge, kelly_fraction=decision.kelly_fraction,
                confidence=decision.confidence_score, rationale=decision.rationale,
                market_prob=decision.implied_market_probability,
                agent_prob=decision.agent_calculated_probability,
                gate_results=gate_result.summary(),
                accepted=False, rejection_reason=gate_result.rejection_reason,
                market_type=market_type,
            )
            return

        # --- APPLY UNITIZED NAV DRAWDOWN KELLY MULTIPLIER ---

        if hasattr(self, '_risk_mgr') and self._risk_mgr:
            nav_kelly = float(self._risk_mgr.get_kelly_multiplier())
            if nav_kelly <= 0.0:
                console.print(f"[red bold]HALTED by NAV drawdown >= 40%[/red bold]")
                return
            if nav_kelly < 1.0:
                original_kelly = decision.kelly_fraction
                decision.kelly_fraction *= nav_kelly
                console.print(
                    f"[yellow]NAV DD Kelly reduction: {original_kelly:.4f} -> "
                    f"{decision.kelly_fraction:.4f} (x{nav_kelly:.2f})[/yellow]"
                )

        # --- DYNAMIC KELLY: scale based on bankroll size ---
        # At low bankroll, individual bets are small enough that higher Kelly is safe.
        # At high bankroll, the $200 max cap limits upside anyway.
        portfolio = float(self._cache.get_portfolio_value())
        if portfolio < 300:
            bankroll_scale = 1.30  # Aggressive at low bankroll
        elif portfolio < 1000:
            bankroll_scale = 1.00  # Normal
        elif portfolio < 5000:
            bankroll_scale = 0.80  # Conservative as we grow
        else:
            bankroll_scale = 0.66  # Protective at scale

        if bankroll_scale != 1.0:
            decision.kelly_fraction *= bankroll_scale

        # --- EXECUTE ---

        trade = await self._order_manager.execute_trade(
            decision=decision,
            cache=self._cache,
            orderbook=orderbook,
            max_bankroll=None,  # MAX_BET_DOLLARS cap handles risk instead
        )

        if trade is not None:
            # Track resting order to prevent duplicate maker orders
            import time as _time
            self._resting_order_times[ticker] = _time.monotonic()

            # Batch race prevention: count this order toward the in-cycle game limit
            if game_id:
                self._pending_game_ids[game_id] = self._pending_game_ids.get(game_id, 0) + 1

            bankroll = self._cache.get_bankroll()
            bankroll_pct = (
                float(trade.price * trade.quantity / bankroll * 100)
                if bankroll > 0
                else 0.0
            )

            # Log accepted decision
            await log_decision(
                ticker=ticker, title=ticker, action=decision.action,
                edge=decision.edge, kelly_fraction=decision.kelly_fraction,
                confidence=decision.confidence_score, rationale=decision.rationale,
                market_prob=decision.implied_market_probability,
                agent_prob=decision.agent_calculated_probability,
                gate_results=gate_result.summary(),
                accepted=True, bet_amount=float(trade.price * trade.quantity),
                market_type=market_type,
            )

            # Track for daily recap
            self._daily_trades += 1

            # Discord alert
            from config.markets import get_sport as _gs
            from execution.position_monitor import SPORT_PROFIT_TAKE, AUTO_PROFIT_TAKE_PCT
            _trade_sport = _gs(trade.kalshi_ticker)
            _trade_pt = SPORT_PROFIT_TAKE.get(_trade_sport, AUTO_PROFIT_TAKE_PCT) * 100

            await self._alerter.send_trade_alert(
                ticker=trade.kalshi_ticker,
                side=trade.side,
                price=trade.price,
                bet_amount=trade.price * trade.quantity,
                bankroll_pct=bankroll_pct,
                rationale=trade.claude_reasoning or "",
                bankroll=bankroll,
                latency_ms=trade.execution_latency_ms or 0,
                portfolio_value=self._cache.get_portfolio_value(),
                sport=_trade_sport,
                profit_take_pct=_trade_pt,
            )

    async def _evaluate_with_rules(self, market_data: dict) -> None:
        """Evaluate a single market using the rules engine. No API cost."""
        # Get ESPN game state for veto logic
        espn_game = None
        game_states = self._live_game_states
        if game_states:
            import re
            match = re.search(r"KX\w+-\d{2}[A-Z]{3}\d{2}([A-Z]{3,8})-", market_data["ticker"].upper())
            if match:
                game_id = match.group(1)
                game = game_states.get(game_id)
                if game:
                    espn_game = {
                        "status": game.status,
                        "quarter": game.quarter,
                        "home_score": game.home_score,
                        "away_score": game.away_score,
                    }

        # Build companion market signal (spread price for NBA/NHL, draw price for EPL/UCL)
        companion_signal = None
        try:
            from config.markets import get_sport as _gs_comp
            comp_sport = _gs_comp(market_data["ticker"])
            if comp_sport in ("NBA", "NHL", "EPL", "UCL"):
                import re
                # Extract game_key (e.g., 26APR04DETPHI) from ticker
                comp_match = re.search(r"\d{2}[A-Z]{3}\d{2}[A-Z]{3,8}", market_data["ticker"].upper())
                if comp_match:
                    game_key = comp_match.group(0)
                    all_obs = self._cache.get_all_orderbooks()

                    if comp_sport in ("NBA", "NHL"):
                        # Find spread market for same game — look for lowest spread number
                        spread_prefix = "KXNBASPREAD" if comp_sport == "NBA" else "KXNHLSPREAD"
                        spread_prices = []
                        for ob_ticker, ob_entry in all_obs.items():
                            if spread_prefix in ob_ticker.upper() and game_key in ob_ticker.upper():
                                if ob_entry.best_bid is not None and ob_entry.best_bid > 0:
                                    spread_prices.append(int(ob_entry.best_bid * 100))
                        if spread_prices:
                            # Use the lowest spread price (closest spread = most relevant)
                            companion_signal = {"spread_price": min(spread_prices), "draw_price": None}

                    elif comp_sport in ("EPL", "UCL"):
                        # Find TIE/draw market for same game
                        draw_prefix = "KXEPLGAME" if comp_sport == "EPL" else "KXUCLGAME"
                        for ob_ticker, ob_entry in all_obs.items():
                            if draw_prefix in ob_ticker.upper() and game_key in ob_ticker.upper() and "TIE" in ob_ticker.upper():
                                if ob_entry.best_bid is not None and ob_entry.best_bid > 0:
                                    companion_signal = {"spread_price": None, "draw_price": int(ob_entry.best_bid * 100)}
                                    break
        except Exception:
            pass  # Never crash for companion lookup

        decision = self._rules.evaluate_market(
            ticker=market_data["ticker"],
            title=market_data.get("title", market_data["ticker"]),
            orderbook=market_data.get("orderbook"),
            espn_game=espn_game,
            companion_signal=companion_signal,
        )

        # Debug: log game winner evaluations
        from config.markets import is_game_winner as _is_gw
        if _is_gw(market_data["ticker"]):
            ob = market_data.get("orderbook")
            bid = ob.best_bid if ob else "None"
            console.print(
                f"[dim]GW EVAL: {market_data['ticker'][:40]} | bid={bid} | "
                f"{decision.action} | {decision.rationale[:60]}[/dim]"
            )

        await self._execute_decision(decision, market_data["ticker"])

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
        import time as _wt
        self._last_bankroll_sync = _wt.monotonic()
        self._last_known_bankroll = self._cache.get_bankroll()

        while self._running:
            await asyncio.sleep(WATCHDOG_INTERVAL)

            # Refresh ESPN game states (free, no auth, powers quarter-aware stops)
            try:
                from data.espn_scores import fetch_live_scores
                self._live_game_states = await fetch_live_scores()
            except Exception:
                pass

            # Periodic bankroll + position sync (every 5 minutes)
            # Detects deposits, settled positions, and bankroll changes
            try:
                if _wt.monotonic() - self._last_bankroll_sync > 300:
                    await self._order_manager.sync_bankroll(self._cache)
                    await self._order_manager.sync_positions(self._cache)
                    # Use portfolio value (cash + positions) for HWM tracking
                    portfolio_val = self._cache.get_portfolio_value()
                    cash_bal = self._cache.get_bankroll()
                    if cash_bal > self._last_known_bankroll:
                        console.print(
                            f"[green]Cash increased: ${self._last_known_bankroll} -> ${cash_bal} "
                            f"(deposit or wins settled) | Portfolio: ${portfolio_val}[/green]"
                        )
                    if hasattr(self, '_risk_mgr') and self._risk_mgr:
                        self._risk_mgr.update_from_trading(portfolio_val)
                    self._last_known_bankroll = cash_bal
                    self._last_bankroll_sync = _wt.monotonic()
            except Exception:
                pass

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

                if DEBUG_MODE:
                    positions = self._cache.get_position_count()
                    bankroll = self._cache.get_bankroll()
                    rules_stats = self._rules.get_stats()
                    console.print(
                        f"[dim]Watchdog: Queue={qsize} | Positions={positions} | "
                        f"Bankroll=${bankroll} | "
                        f"Rules: {rules_stats['total_signals']}/{rules_stats['total_evaluated']} signals "
                        f"({rules_stats['signal_rate']})[/dim]"
                    )

            except Exception as e:
                console.print(
                    f"[red]Watchdog error: {type(e).__name__}: {e}[/red]"
                )

    async def _discover_nba_markets(self) -> None:
        """
        Auto-discover all active NBA and NHL markets on Kalshi.

        Scans for game winners, spreads, and player props. Only tracks
        markets that have real orderbook depth (at least one bid and ask).
        This replaces manual ticker configuration entirely.
        """
        console.print("[blue]Discovering sports markets (NBA/NHL/EPL/UCL/LaLiga/WNBA/UFC)...[/blue]")

        discovered_tickers: list[str] = []
        discovered_titles: dict[str, str] = {}
        latest_game_end: datetime | None = None

        # Search for all supported game winner markets + NBA props/spreads
        for prefix in [
            "KXNBAGAME", "KXNBASPREAD", "KXNBAPTS",  # NBA
            "KXNHLGAME",                               # NHL
            "KXEPLGAME",                               # EPL Soccer
            # "KXUCLGAME",                             # UCL — disabled: -49% ROI after slippage
            "KXLALIGAGAME",                            # La Liga
            "KXWNBAGAME",                              # WNBA
            "KXUFCFIGHT",                              # UFC/MMA
            "KXNCAAMBGAME",                            # NCAA Men's Basketball
            "KXNCAAWBGAME",                            # NCAA Women's Basketball
            "KXWTAMATCH",                              # WTA Tennis — RE-ENABLED: 150% PT at 76-90c rescues it (Sharpe 0.183)
            "KXATPMATCH",                              # ATP Tennis — year-round, strong FLB 71-85c, +2.5% retirement premium
            "KXCFBGAME",                               # College Football — Sep-Jan, strong FLB at 90c+, conservative until validated
            "KXMLBGAME",                               # MLB — 50% PT at 76-84c, very conservative (small sample)
            "KXMLBTOTAL",                              # MLB Totals — BEST new market: 0.815 Sharpe, 82% WR, 100% PT
            "KXNFLGAME",                               # NFL Game Winners — 0.244 Sharpe, 66% WR, 100% PT
            # "KXNFLTEAMTOTAL",                        # NFL Team Totals — disabled: +16% ROI after slippage (below 20% threshold)
            "KXCBAGAME",                               # CBA (Chinese Basketball) — +39% ROI, 57% WR
            # "KXLIGUE",                               # Ligue 1 — disabled: +4% ROI after slippage (below 20% threshold)
            "KXLOLMAP",                                # League of Legends — +69% ROI, 75% WR
            # "KXATPCHALLENGERMATCH",                  # ATP Challenger — disabled: +0.2% ROI after slippage (breakeven)
            # Weather DISABLED: These are categorical range markets (5-50c per bucket),
            # NOT binary favorites. No FLB to exploit — avg YES price is 22c.
            # "KXHIGHNY", "KXHIGHCHI", "KXHIGHMIA",
            # "KXHIGHLA", "KXHIGHSF", "KXHIGHHOU",
            # "KXHIGHDEN", "KXHIGHDC", "KXHIGHDAL",
            # "KXHIGHAUS", "KXHIGHPHIL",
            "CPI", "CPICORE", "CPICOREYOY",            # CPI / Inflation
            "KXNFLANYTD",                              # NFL Anytime Touchdown
            "KXNHLSPREAD",                             # NHL Spreads (OOS confirmed)
            # "KXNHLFIRSTGOAL",                         # NHL First Goal — dropped: OOS decayed (62% YES vs 45% predicted)
            # "KXNBA2D",                               # NBA Double-Double — dropped: -7% ROI after recalibration
            "KXNFLSPREAD",                             # NFL Spreads
        ]:
            cursor = None
            for _ in range(5):
                try:
                    params = f"/markets?series_ticker={prefix}&status=open&limit=100"
                    if cursor:
                        params += f"&cursor={cursor}"
                    result = await self._kalshi_client._request_with_retry("GET", params)
                    markets = result.get("markets", [])

                    for m in markets:
                        ticker = m.get("ticker", "")
                        title = m.get("title", "")
                        volume = float(m.get("volume_fp", "0"))

                        if ticker and title:
                            # Only track markets expiring within 12 hours (today's games)
                            exp_time = m.get("expected_expiration_time", m.get("close_time"))
                            expires_today = False
                            if exp_time:
                                try:
                                    from datetime import timedelta
                                    exp_dt = datetime.fromisoformat(exp_time.replace("Z", "+00:00"))
                                    hours_until = (exp_dt - datetime.now(timezone.utc)).total_seconds() / 3600
                                    expires_today = 0 < hours_until < 12
                                except (ValueError, TypeError):
                                    pass

                            if not expires_today:
                                continue  # Skip markets not happening today

                            # Within today's games, be selective by volume
                            should_track = False
                            from config.markets import GAME_WINNER_PATTERNS
                            game_winner_prefixes = tuple(GAME_WINNER_PATTERNS)
                            if prefix in game_winner_prefixes:
                                should_track = True  # Always track game/fight winners
                            elif prefix == "KXNBASPREAD" and volume > 500:
                                should_track = True
                            elif prefix == "KXNBAPTS" and volume > 100:
                                should_track = True

                            if should_track:
                                discovered_tickers.append(ticker)
                                discovered_titles[ticker] = title
                                self._market_volumes[ticker] = volume

                            # Track the latest game end time for auto-shutdown
                            exp_time = m.get("expected_expiration_time", m.get("close_time"))
                            if exp_time:
                                try:
                                    exp_dt = datetime.fromisoformat(exp_time.replace("Z", "+00:00"))
                                    if latest_game_end is None or exp_dt > latest_game_end:
                                        latest_game_end = exp_dt
                                except (ValueError, TypeError):
                                    pass

                    cursor = result.get("cursor")
                    if not cursor or not markets:
                        break
                except Exception:
                    break
                await asyncio.sleep(0.1)

        # Store latest game end time for auto-shutdown
        self._latest_game_end = latest_game_end

        # Update global lists
        DEFAULT_TRACKED_TICKERS.clear()
        DEFAULT_TRACKED_TICKERS.extend(discovered_tickers)
        self._market_titles = discovered_titles

        # Update the feed, market poller, and arb scanner with discovered tickers
        self._feed._tracked_tickers = discovered_tickers
        self._market_poller._tracked_tickers = discovered_tickers
        self._arb_scanner.update_tickers(discovered_tickers)

        # Extract player names from player prop titles for the NBA poller
        import re
        player_names: set[str] = set()
        for ticker, title in discovered_titles.items():
            if "KXNBAPTS" in ticker:
                # Title format: "Klay Thompson: 25+ points"
                name_match = re.match(r"^([A-Za-z\s\.']+?):\s", title)
                if name_match:
                    player_names.add(name_match.group(1).strip())

        DEFAULT_TRACKED_PLAYERS.clear()
        DEFAULT_TRACKED_PLAYERS.extend({"name": name} for name in player_names)
        if self._nba_poller:
            self._nba_poller._tracked_players = DEFAULT_TRACKED_PLAYERS

        # Count by sport
        from config.markets import get_sport
        sport_counts: dict[str, int] = {}
        for t in discovered_tickers:
            s = get_sport(t) or "Other"
            sport_counts[s] = sport_counts.get(s, 0) + 1
        sport_summary = " + ".join(f"{c} {s}" for s, c in sorted(sport_counts.items()) if c > 0)

        # Display latest game end time
        if latest_game_end:
            local_end = latest_game_end.astimezone()
            console.print(
                f"[green]Discovered: {sport_summary}, "
                f"{len(player_names)} players | "
                f"Last game ends ~{local_end.strftime('%I:%M %p')}[/green]"
            )
        else:
            console.print(
                f"[green]Discovered: {sport_summary}, "
                f"{len(player_names)} players[/green]"
            )

        # Print summary by game
        games: dict[str, int] = {}
        for t in discovered_tickers:
            gid = _extract_game_id(t)
            if gid:
                games[gid] = games.get(gid, 0) + 1
        for gid, count in sorted(games.items()):
            console.print(f"  [blue]{gid}: {count} markets[/blue]")

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

        # Record starting bankroll and portfolio value
        self._starting_bankroll = self._cache.get_bankroll()
        starting_portfolio = self._cache.get_portfolio_value()

        from data.unitized_risk import UnitizedRiskManager
        self._risk_mgr = UnitizedRiskManager(initial_equity=starting_portfolio)

        self._risk_gates = RiskGates(starting_bankroll=self._starting_bankroll)

        nav_status = self._risk_mgr.get_status()
        console.print(
            f"[blue]Starting bankroll: ${self._starting_bankroll} | "
            f"NAV: ${nav_status['nav']:.4f} | HWM NAV: ${nav_status['hwm_nav']:.4f} | "
            f"DD: {nav_status['drawdown_pct']:.1f}% | "
            f"Shares: {nav_status['shares']:.2f} | 6-gate risk system initialized.[/blue]"
        )

        # Connect brier_tracker to risk_gates (now that risk_gates exists)
        self._brier_tracker._risk_gates = self._risk_gates

        # Initialize Bayesian state from priors if first run
        from data.bayesian_cache import get_or_init_state
        get_or_init_state()

        # Auto-discover NBA markets (replaces manual ticker config)
        await self._discover_nba_markets()

        # Send startup alert
        await self._alerter.send_startup(
            TRADING_MODE, self._cache.get_bankroll(),
            portfolio_value=self._cache.get_portfolio_value(),
        )

        console.print(
            f"[green]EdgeRunner: Ready. Mode={TRADING_MODE.upper()} "
            f"Bankroll=${self._cache.get_bankroll()} "
            f"Tickers={len(DEFAULT_TRACKED_TICKERS)} "
            f"Players={len(DEFAULT_TRACKED_PLAYERS)}[/green]"
        )

    async def _shutdown(self, reason: str = "Manual stop") -> None:
        """Gracefully shut down all modules and send session summary."""
        console.print("\n[yellow]EdgeRunner: Shutting down...[/yellow]")
        self._running = False

        # Stop all modules
        await self._feed.stop()
        await self._market_poller.stop()
        if self._nba_poller:
            await self._nba_poller.stop()
        await self._smart_money.stop()
        await self._position_monitor.stop()
        await self._arb_scanner.stop()
        await self._brier_tracker.stop()

        # Sync final bankroll from Kalshi for accurate reporting
        try:
            await self._order_manager.sync_bankroll(self._cache)
        except Exception:
            pass

        # Build session summary
        analyzer_status = self._rules.get_stats()
        order_status = self._order_manager.get_status()
        final_bankroll = self._cache.get_bankroll()
        pnl = final_bankroll - self._starting_bankroll
        pnl_pct = float(pnl / self._starting_bankroll * 100) if self._starting_bankroll > 0 else 0.0
        uptime_min = (time.monotonic() - self._start_time) / 60
        open_positions = self._cache.get_position_count()

        # Print to terminal
        console.print("\n[bold]SESSION SUMMARY[/bold]")
        console.print(f"  Starting Bankroll: ${self._starting_bankroll}")
        console.print(f"  Final Bankroll:    ${final_bankroll}")
        pnl_color = "green" if pnl >= 0 else "red"
        console.print(f"  [{pnl_color}]Session P&L:       ${pnl:+.2f} ({pnl_pct:+.1f}%)[/{pnl_color}]")
        rules_stats = self._rules.get_stats()
        console.print(f"  Trades Executed:   {order_status['total_executions']}")
        console.print(f"  Trades Rejected:   {order_status['total_rejections']}")
        console.print(f"  Open Positions:    {open_positions}")
        console.print(f"  Markets Evaluated: {rules_stats['total_evaluated']}")
        console.print(f"  Signals Generated: {rules_stats['total_signals']} ({rules_stats['signal_rate']})")
        console.print(f"  Claude API Cost:   $0.00 (rules-based, no LLM)")
        console.print(f"  Session Duration:  {uptime_min:.0f} minutes")
        console.print(f"  Shutdown Reason:   {reason}")

        # Send session summary to Discord
        try:
            await self._alerter.send_shutdown(
                reason=reason,
                pnl=pnl,
                bankroll=final_bankroll,
                trades=order_status["total_executions"],
            )
        except Exception:
            pass

        await self._kalshi_client.close()
        await self._alerter.close()

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
            tasks = [
                self._feed.run(),
                self._market_poller.run(),
                self._smart_money.run(),
                self._signal_evaluator(),
                self._position_monitor.run(),
                self._arb_scanner.run(),
                self._brier_tracker.run(),
                self._watchdog(),
                self._auto_shutdown_timer(),
                self._daily_recap_task(),
            ]
            if self._nba_poller:
                tasks.append(self._nba_poller.run())
            task_names = [
                "feed", "market_poller", "smart_money", "signal_evaluator",
                "position_monitor", "arb_scanner", "brier_tracker",
                "watchdog", "auto_shutdown", "daily_recap",
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            # Check for silently crashed tasks
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    name = task_names[i] if i < len(task_names) else f"task_{i}"
                    console.print(f"[red bold]TASK CRASHED: {name} — {type(result).__name__}: {result}[/red bold]")
                    try:
                        await self._alerter._send_embed(
                            title="TASK CRASH",
                            description=f"Task `{name}` crashed: {result}",
                            color=0xFF0000,
                        )
                    except Exception:
                        pass
        except asyncio.CancelledError:
            pass
        finally:
            await self._shutdown(self._shutdown_reason)

    async def _auto_shutdown_timer(self) -> None:
        """
        Disabled — agent runs continuously. runner.py handles scheduling.
        Re-discovers markets every 2 hours to pick up new games.
        """
        console.print("[blue]Auto-shutdown: DISABLED (24/7 mode). Market re-discovery every 2h.[/blue]")

        # Wait for initial startup to complete
        await asyncio.sleep(600)

        while self._running:
            # Re-discover markets every hour to capture opening line value
            # Gemini research: FLB is strongest when markets first open, before sharp money corrects
            await asyncio.sleep(3600)
            try:
                # Track tickers before re-discovery to identify NEW markets
                old_tickers = set(self._cache.get_all_orderbooks().keys())

                console.print("[blue]Re-discovering markets...[/blue]")
                await self._discover_nba_markets()

                # Clean up discovery prices for tickers no longer tracked
                from data.discovery_cache import clear_old_prices, save_discovery_prices
                self._discovery_prices = clear_old_prices(
                    self._discovery_prices, DEFAULT_TRACKED_TICKERS
                )
                save_discovery_prices(self._discovery_prices)

                # Log new markets — market poller will fetch orderbooks on next cycle (~30s)
                new_tickers = set(self._cache.get_all_orderbooks().keys()) - old_tickers
                if new_tickers:
                    console.print(f"[green]Opening Line Capture: {len(new_tickers)} new markets — poller will evaluate on next cycle[/green]")
            except Exception as e:
                console.print(f"[red]Market re-discovery error: {e} (will retry in 2h)[/red]")

            # Check if we've passed the last game's end time + buffer
            if shutdown_target and now >= shutdown_target:
                has_open_positions = self._cache.get_position_count() > 0
                if has_open_positions:
                    console.print(
                        "[yellow]Auto-shutdown: Past game end time but "
                        f"{self._cache.get_position_count()} positions still open. Waiting...[/yellow]"
                    )
                    continue


    async def _daily_recap_task(self) -> None:
        """
        Send a daily recap to Discord at 12 AM PDT (7 AM UTC).
        Summarizes the day's trades, P&L, and bankroll.
        """
        import pytz

        pdt = pytz.timezone("America/Los_Angeles")
        console.print("[blue]Daily recap: Active (sends at 12:00 AM PDT daily).[/blue]")

        # Initialize daily tracking
        self._daily_start_bankroll = self._cache.get_bankroll()

        while self._running:
            # Check every 60 seconds if it's midnight PDT
            await asyncio.sleep(60)
            try:
                now_pdt = datetime.now(timezone.utc).astimezone(pdt)

                if now_pdt.hour == 0 and now_pdt.minute == 0:
                    # It's midnight PDT — send recap
                    current_bankroll = self._cache.get_bankroll()
                    daily_pnl = current_bankroll - self._daily_start_bankroll
                    daily_pnl_pct = (
                        float(daily_pnl / self._daily_start_bankroll * 100)
                        if self._daily_start_bankroll > 0 else 0.0
                    )
                    positions = self._cache.get_position_count()
                    rules_stats = self._rules.get_stats()

                    pnl_emoji = "+" if daily_pnl >= 0 else ""
                    recap = (
                        f"DAILY RECAP ({now_pdt.strftime('%A %B %d, %Y')})\n"
                        f"Bankroll: ${current_bankroll:.2f}\n"
                        f"Daily P&L: {pnl_emoji}${daily_pnl:.2f} ({pnl_emoji}{daily_pnl_pct:.1f}%)\n"
                        f"Trades today: {self._daily_trades}\n"
                        f"Open positions: {positions}\n"
                        f"Signals: {rules_stats['total_signals']}/{rules_stats['total_evaluated']} ({rules_stats['signal_rate']})"
                    )

                    console.print(f"[bold cyan]{recap}[/bold cyan]")

                    # Get Bayesian update count for the day
                    try:
                        from data.bayesian_cache import load_bayesian_state
                        bstate = load_bayesian_state()
                        bayesian_updates = sum(v.get("updates", 0) for v in bstate.values()) if bstate else 0
                    except Exception:
                        bayesian_updates = 0

                    # Send to Discord
                    try:
                        await self._alerter.send_daily_summary(
                            trades_executed=self._daily_trades,
                            wins=self._daily_wins,
                            losses=self._daily_trades - self._daily_wins,
                            day_pnl=daily_pnl,
                            bankroll=current_bankroll,
                            portfolio_value=self._cache.get_portfolio_value(),
                            open_positions=positions,
                            signals_generated=rules_stats["total_signals"],
                            signals_evaluated=rules_stats["total_evaluated"],
                            bayesian_updates=bayesian_updates,
                        )
                    except Exception:
                        pass

                    # Apply daily Bayesian decay (old priors fade, recent data weighs more)
                    try:
                        from data.bayesian_cache import apply_daily_decay, load_bayesian_state, save_bayesian_state
                        bstate = load_bayesian_state()
                        if bstate:
                            bstate = apply_daily_decay(bstate)
                            save_bayesian_state(bstate)
                            console.print(f"[blue]Bayesian: Applied daily decay to {len(bstate)} buckets[/blue]")
                    except Exception:
                        pass

                    # Sync bankroll from Kalshi (catch overnight settlements)
                    try:
                        await self._order_manager.sync_bankroll(self._cache)
                        await self._order_manager.sync_positions(self._cache)
                    except Exception:
                        pass

                    # Reset daily counters
                    self._daily_trades = 0
                    self._daily_wins = 0
                    self._daily_start_bankroll = self._cache.get_bankroll()
                    self._rules._total_evaluated = 0
                    self._rules._total_signals = 0

                    # Sleep 90 seconds to avoid double-firing
                    await asyncio.sleep(90)

            except Exception as e:
                console.print(f"[red]Daily recap error: {e}[/red]")


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
        loop.run_until_complete(agent._shutdown("Manual stop (Ctrl+C)"))
    finally:
        loop.close()


if __name__ == "__main__":
    main()
