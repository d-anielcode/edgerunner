"""
Position monitor for EdgeRunner.

Periodically checks open positions against current market prices and
decides whether to hold or cut losses. This is the risk management
layer that prevents a bad trade from destroying the bankroll.

How it works:
1. Every 60 seconds, fetch current prices for all open positions
2. Calculate unrealized P&L for each position
3. If a position has lost more than the stop-loss threshold, send it
   to Claude asking "should we exit?"
4. If Claude says sell, place a SELL order to close the position
5. Send a Discord alert either way

Stop-loss logic:
- If current price has moved >50% against entry price, trigger review
- If unrealized loss exceeds 2% of bankroll, trigger review
- Claude makes the final call — it may say hold if fundamentals haven't changed
"""

import asyncio
import time
from datetime import datetime, timezone
from decimal import Decimal

from rich.console import Console

from config.settings import DEBUG_MODE, TRADING_MODE
from data.cache import AgentCache
from execution.kalshi_client import KalshiClient
from signals.schemas import TradeDecision
from storage.models import Position

console = Console()
UTC = timezone.utc

# How often to check positions (seconds)
POSITION_CHECK_INTERVAL: float = 60.0

# Initial stop-loss (from entry price)
INITIAL_STOP_LOSS_PCT: float = 0.50  # Sell if position drops 50% from entry

# Trailing stop-loss (from peak price)
# DISABLED: Backtest showed 25% trailing stop destroys 72% of edge on game winners.
# Games are too volatile — price swings through stop before reverting.
TRAILING_STOP_PCT: float = 999.0  # Effectively disabled (was 0.25)

# Breakeven lock: once position is up this much, floor moves to entry price
BREAKEVEN_LOCK_PCT: float = 0.50  # Up 50% → never let it become a loss

# Bankroll-based stop
BANKROLL_LOSS_THRESHOLD: float = 0.02  # 2% of bankroll loss triggers review

# Sport-specific profit-take thresholds (from primary backtest optimization grid)
# Each sport has its own optimal PT% based on trajectory analysis of 6,636 markets
SPORT_PROFIT_TAKE = {
    "EPL": 1.00,       # 100% — triggers 90% of the time, locks in gains early
    "NBA": 1.50,       # 150% — moderate, higher edge on bigger swings
    "NBASPREAD": 1.50, # 150%
    "NCAAMB": 1.00,    # 100% — high variance, take profits fast
    "NFLSPREAD": 2.00, # 200% — keep current
    "NFLTD": 1.00,     # 100%
    "NHL": 1.00,       # 100% — games resolve correctly, take early
    "NHLSPREAD": 3.00, # 300% — rare but massive when it hits
    "UCL": 1.00,       # 100%
    "UFC": 2.00,       # 200% — keep current
    "WNBA": 1.00,      # 100%
    "ATP": 1.00,       # 100% — default for new sports
    "CFB": 2.00,       # 200% — default until validated
    "WTA": 1.50,       # 150% — RE-ENABLED: best Sharpe at 76-90c with 150% PT
    "MLB": 0.50,       # 50% — take profits fast, weak FLB only viable with quick exits
    "LALIGA": 2.00,    # 200% — optimization confirmed current 200% is optimal
    "MLBTOTAL": 1.00,  # 100% — best new market, 82% WR
    "NFLGW": 1.00,     # 100% — NFL game winners
    "NFLTT": 1.50,     # 150% — NFL team totals
    "CBA": 1.00,       # 100% — Chinese basketball
    "LIGUE1": 1.00,    # 100% — French soccer
    "LOL": 1.00,       # 100% — League of Legends esports
    "ATPCH": 0.50,     # 50% — ATP Challenger, take profits fast
}
AUTO_PROFIT_TAKE_PCT: float = 1.50  # Fallback for sports not in SPORT_PROFIT_TAKE

# Tail-risk stop: sell dying positions at $0.05-0.10 to exploit retail FLB on lottery tickets
# Retail overpays for longshots — selling a 2% chance at $0.08 is positive EV
TAIL_RISK_FLOOR: Decimal = Decimal("0.08")

# Quarter-aware trailing stops for player props
# [Q1, Q2, Q3, Q4] — how much decline to allow from peak before selling
PROP_TRAILING_STOPS = {
    "POINTS": [0.40, 0.35, 0.30, 0.20],
    "REBOUNDS": [0.35, 0.30, 0.25, 0.15],
    "ASSISTS": [0.50, 0.40, 0.35, 0.25],
    "THREES": [0.50, 0.40, 0.35, 0.25],
    "DEFAULT": [0.40, 0.35, 0.30, 0.20],
}

# Hard price floor — never hold below this (wealth transfer zone)
PROP_ABSOLUTE_FLOOR: Decimal = Decimal("0.10")


class PositionMonitor:
    """
    Monitors open positions and manages stop-losses.

    Runs as an asyncio task alongside the other agent components.
    Periodically checks if any position should be exited to limit losses.

    Usage:
        monitor = PositionMonitor(kalshi_client=client, cache=cache, analyzer=analyzer, alerter=alerter)
        await monitor.run()
    """

    def __init__(
        self,
        kalshi_client: KalshiClient,
        cache: AgentCache,
        analyzer: object,  # MarketAnalyzer — avoid circular import
        alerter: object,   # DiscordAlerter — avoid circular import
    ) -> None:
        self._kalshi = kalshi_client
        self._cache = cache
        self._analyzer = analyzer
        self._alerter = alerter
        self._running: bool = False
        # Track the highest price each position has reached (for trailing stop)
        # Load from disk so peaks survive agent restarts
        from data.peak_cache import load_peak_prices
        self._peak_prices: dict[str, Decimal] = load_peak_prices()
        self._peak_lock = asyncio.Lock()
        if self._peak_prices:
            console.print(f"[blue]Loaded {len(self._peak_prices)} peak prices from cache.[/blue]")
        # Track positions we've exited (ticker → exit details) for re-entry logic
        self._exited_positions: dict[str, dict] = {}

    async def _get_current_price(self, ticker: str, side: str) -> Decimal | None:
        """
        Fetch the current price for a position from the orderbook.

        For a YES position: current value = best YES bid (what you'd get selling)
        For a NO position: current value = best NO bid = 1 - best YES ask
        """
        try:
            ob_resp = await self._kalshi._request_with_retry(
                "GET", f"/markets/{ticker}/orderbook"
            )
            data = ob_resp.get("orderbook_fp", {})
            yes_levels = data.get("yes_dollars", [])
            no_levels = data.get("no_dollars", [])

            if side == "yes":
                # Value of YES position = highest YES bid
                if yes_levels:
                    return Decimal(yes_levels[-1][0])
            else:
                # Value of NO position = highest NO bid
                # NO bid = 1 - YES ask, but we can read NO side directly
                if no_levels:
                    return Decimal(no_levels[-1][0])

            return None
        except Exception as e:
            if DEBUG_MODE:
                console.print(f"[dim]Position monitor price error: {e}[/dim]")
            return None

    def _is_player_prop(self, ticker: str) -> bool:
        """Check if a ticker is a player prop market."""
        ticker_upper = ticker.upper()
        prop_prefixes = ["KXNBAPTS", "KXNBAREB", "KXNBAAST", "KXNBA3PT", "KXNBABLK", "KXNBASTL", "KXNBA2D"]
        return any(ticker_upper.startswith(prefix) for prefix in prop_prefixes)

    def _get_prop_type(self, ticker: str) -> str:
        """Get the prop type for quarter-aware stop-loss lookup."""
        ticker_upper = ticker.upper()
        if "KXNBAPTS" in ticker_upper:
            return "POINTS"
        elif "KXNBAREB" in ticker_upper:
            return "REBOUNDS"
        elif "KXNBAAST" in ticker_upper:
            return "ASSISTS"
        elif "KXNBA3PT" in ticker_upper:
            return "THREES"
        return "DEFAULT"

    async def _evaluate_player_prop(self, position: Position, current_quarter: int | None) -> dict:
        """
        Evaluate a player prop position using quarter-aware trailing stops.

        Uses ESPN game clock data to determine the current quarter,
        then applies progressively tighter stop-losses as the game progresses.

        Q1: Wide tolerance (40% decline OK — normal variance)
        Q4: Tight tolerance (20% decline = sell — time is running out)

        Also enforces a hard floor at $0.10 — never hold below this.
        """
        current_price = await self._get_current_price(
            position.kalshi_ticker, position.side
        )
        if current_price is None:
            return {
                "action": "hold", "current_price": None,
                "unrealized_pnl": Decimal("0"), "pnl_pct": 0.0,
                "reason": "Player prop: no price data.",
            }

        entry_price = position.avg_price
        pnl_per_contract = current_price - entry_price
        unrealized_pnl = pnl_per_contract * position.quantity
        pnl_pct = float(pnl_per_contract / entry_price) if entry_price > 0 else 0.0

        # Update peak price
        ticker = position.kalshi_ticker
        async with self._peak_lock:
            prev_peak = self._peak_prices.get(ticker, entry_price)
            if current_price > prev_peak:
                self._peak_prices[ticker] = current_price
                # Persist to disk so peaks survive restarts
                from data.peak_cache import save_peak_prices
                save_peak_prices(self._peak_prices)
            peak_price = self._peak_prices.get(ticker, entry_price)

        drop_from_peak = float((peak_price - current_price) / peak_price) if peak_price > 0 else 0.0

        # RULE 1: Auto profit-take at 300%
        if pnl_pct >= 3.00:
            return {
                "action": "sell", "current_price": current_price,
                "unrealized_pnl": unrealized_pnl, "pnl_pct": pnl_pct,
                "reason": f"Prop auto-take: up {pnl_pct:+.0%}.",
            }

        # RULE 2: Hard floor — never hold below $0.10
        if current_price <= PROP_ABSOLUTE_FLOOR:
            return {
                "action": "sell", "current_price": current_price,
                "unrealized_pnl": unrealized_pnl, "pnl_pct": pnl_pct,
                "reason": f"Prop hard floor: price ${current_price} <= ${PROP_ABSOLUTE_FLOOR}. Exiting wealth-transfer zone.",
            }

        # RULE 3: Quarter-aware trailing stop
        if current_quarter is not None and current_quarter >= 1:
            prop_type = self._get_prop_type(ticker)
            stops = PROP_TRAILING_STOPS.get(prop_type, PROP_TRAILING_STOPS["DEFAULT"])
            q_index = min(current_quarter - 1, 3)  # Clamp to Q4 for OT
            max_decline = stops[q_index]

            if drop_from_peak >= max_decline:
                return {
                    "action": "sell", "current_price": current_price,
                    "unrealized_pnl": unrealized_pnl, "pnl_pct": pnl_pct,
                    "reason": (
                        f"Prop Q{current_quarter} stop: {drop_from_peak:.0%} decline from peak "
                        f"${peak_price} (max {max_decline:.0%} in Q{current_quarter}). "
                        f"Type: {prop_type}."
                    ),
                }

        # RULE 4: No quarter data — use fallback (wider stop)
        elif drop_from_peak >= 0.50:
            return {
                "action": "sell", "current_price": current_price,
                "unrealized_pnl": unrealized_pnl, "pnl_pct": pnl_pct,
                "reason": f"Prop fallback stop: {drop_from_peak:.0%} decline (no game clock data).",
            }

        # ALL CLEAR — HOLD
        q_str = f"Q{current_quarter}" if current_quarter else "pre-game"
        peak_info = f" | Peak=${peak_price}" if peak_price > entry_price else ""
        return {
            "action": "hold", "current_price": current_price,
            "unrealized_pnl": unrealized_pnl, "pnl_pct": pnl_pct,
            "reason": f"Prop hold ({q_str}). P&L: {pnl_pct:+.0%} (${unrealized_pnl:+.2f}){peak_info}",
        }

    async def _evaluate_position(self, position: Position) -> dict:
        """
        Evaluate whether a position should be held or exited.

        THREE MODES:
        - Player props: Quarter-aware trailing stops (tight in Q4, wide in Q1)
        - Game winners/spreads: Standard trailing stop (25% from peak)
        - Both: Hard floor at $0.10, auto-take at 200-300%

        Returns a dict with action, current_price, unrealized_pnl, pnl_pct, reason.
        """
        # Player props: use quarter-aware stops with ESPN game clock
        if self._is_player_prop(position.kalshi_ticker):
            # Fetch current quarter from ESPN
            from data.espn_scores import fetch_live_scores, get_quarter_from_game
            game_states = await fetch_live_scores()
            current_quarter = get_quarter_from_game(game_states, position.kalshi_ticker)
            return await self._evaluate_player_prop(position, current_quarter)

        # Game winners: HOLD TO SETTLEMENT — no trailing stops
        # The backtest models hold-to-settlement and that's where the edge is.
        # Selling early at 33c instead of waiting for $1.00 destroys the payout ratio.
        from config.markets import is_game_winner, get_sport
        pos_sport = get_sport(position.kalshi_ticker)
        hold_to_settle = is_game_winner(position.kalshi_ticker) or pos_sport in ("WEATHER", "CPI", "NFLTD")
        if hold_to_settle:
            current_price = await self._get_current_price(
                position.kalshi_ticker, position.side
            )
            pnl_pct = 0.0
            unrealized = Decimal("0")
            if current_price is not None:
                unrealized = (current_price - position.avg_price) * position.quantity
                pnl_pct = float((current_price - position.avg_price) / position.avg_price) if position.avg_price > 0 else 0.0

                # Sport-specific profit-take (from optimization grid backtest)
                pt_threshold = SPORT_PROFIT_TAKE.get(pos_sport, AUTO_PROFIT_TAKE_PCT)
                if pnl_pct >= pt_threshold:
                    console.print(
                        f"[green]GW PROFIT TAKE: {position.kalshi_ticker[:35]} up {pnl_pct:+.0%}! "
                        f"({pos_sport} threshold: {pt_threshold:.0%}) "
                        f"Selling to lock in ${unrealized:+.2f}.[/green]"
                    )
                    return {
                        "action": "sell",
                        "current_price": current_price,
                        "unrealized_pnl": unrealized,
                        "pnl_pct": pnl_pct,
                        "reason": f"GW profit-take: up {pnl_pct:+.0%} (${unrealized:+.2f}).",
                    }

                # Tail-risk stop: DISABLED for game winners.
                # The Gemini research and our data both show that hold-to-settlement
                # beats stop-losses on game winners. The tail-risk exit was selling
                # positions within minutes of entry (before the game could play out).
                # Game winners should either hit the 200% profit-take or settle at $0/$1.

            return {
                "action": "hold",
                "current_price": current_price,
                "unrealized_pnl": unrealized,
                "pnl_pct": pnl_pct,
                "reason": f"Game winner: hold to settlement (P&L: {pnl_pct:+.0%})",
            }

        # Spreads/other: full trailing stop-loss system
        current_price = await self._get_current_price(
            position.kalshi_ticker, position.side
        )

        if current_price is None:
            return {
                "action": "hold",
                "current_price": None,
                "unrealized_pnl": Decimal("0"),
                "pnl_pct": 0.0,
                "reason": "Cannot fetch current price -- holding.",
            }

        # Calculate P&L
        entry_price = position.avg_price
        quantity = position.quantity
        pnl_per_contract = current_price - entry_price
        unrealized_pnl = pnl_per_contract * quantity
        pnl_pct = float(pnl_per_contract / entry_price) if entry_price > 0 else 0.0

        # Update peak price tracking (locked for concurrent safety)
        ticker = position.kalshi_ticker
        async with self._peak_lock:
            prev_peak = self._peak_prices.get(ticker, entry_price)
            if current_price > prev_peak:
                self._peak_prices[ticker] = current_price
                from data.peak_cache import save_peak_prices
                save_peak_prices(self._peak_prices)
            peak_price = self._peak_prices.get(ticker, entry_price)

        # Calculate drop from peak
        drop_from_peak = float((peak_price - current_price) / peak_price) if peak_price > 0 else 0.0

        bankroll = self._cache.get_bankroll()
        bankroll_loss_pct = (
            float(abs(unrealized_pnl) / bankroll) if bankroll > 0 and unrealized_pnl < 0 else 0.0
        )

        # === RULE 1: AUTO PROFIT-TAKE at 200% gain ===
        if pnl_pct >= AUTO_PROFIT_TAKE_PCT:
            console.print(
                f"[green]AUTO PROFIT TAKE: {ticker} up {pnl_pct:+.0%}! "
                f"Selling to lock in ${unrealized_pnl:+.2f}.[/green]"
            )
            return {
                "action": "sell",
                "current_price": current_price,
                "unrealized_pnl": unrealized_pnl,
                "pnl_pct": pnl_pct,
                "reason": f"Auto profit-take: up {pnl_pct:+.0%} (${unrealized_pnl:+.2f}).",
            }

        # === RULE 2: TRAILING STOP from peak ===
        # Only active once position has been profitable
        if peak_price > entry_price and drop_from_peak >= TRAILING_STOP_PCT:
            # Calculate how much profit we're locking in
            locked_pnl = (current_price - entry_price) * quantity
            console.print(
                f"[yellow]TRAILING STOP: {ticker} dropped {drop_from_peak:.0%} from "
                f"peak ${peak_price}. Selling at ${current_price} to lock in "
                f"${locked_pnl:+.2f}.[/yellow]"
            )
            return {
                "action": "sell",
                "current_price": current_price,
                "unrealized_pnl": unrealized_pnl,
                "pnl_pct": pnl_pct,
                "reason": f"Trailing stop: {drop_from_peak:.0%} drop from peak ${peak_price}. Locking in ${locked_pnl:+.2f}.",
            }

        # === RULE 3: BREAKEVEN LOCK ===
        # Once position was up 50%+, never let it become a loss
        peak_pnl_pct = float((peak_price - entry_price) / entry_price) if entry_price > 0 else 0.0
        if peak_pnl_pct >= BREAKEVEN_LOCK_PCT and current_price <= entry_price:
            console.print(
                f"[yellow]BREAKEVEN LOCK: {ticker} was up {peak_pnl_pct:+.0%} "
                f"(peak ${peak_price}), now back to entry. "
                f"Selling to preserve capital.[/yellow]"
            )
            return {
                "action": "sell",
                "current_price": current_price,
                "unrealized_pnl": unrealized_pnl,
                "pnl_pct": pnl_pct,
                "reason": f"Breakeven lock: was up {peak_pnl_pct:+.0%}, now at entry. Preserving capital.",
            }

        # === RULE 4: INITIAL STOP-LOSS (from entry) ===
        if pnl_pct <= -INITIAL_STOP_LOSS_PCT:
            return {
                "action": "sell",
                "current_price": current_price,
                "unrealized_pnl": unrealized_pnl,
                "pnl_pct": pnl_pct,
                "reason": f"Stop-loss: down {pnl_pct:+.0%} from entry (threshold: {-INITIAL_STOP_LOSS_PCT:.0%}).",
            }

        # === RULE 5: BANKROLL PROTECTION ===
        if bankroll_loss_pct > BANKROLL_LOSS_THRESHOLD:
            return {
                "action": "sell",
                "current_price": current_price,
                "unrealized_pnl": unrealized_pnl,
                "pnl_pct": pnl_pct,
                "reason": f"Bankroll protection: loss is {bankroll_loss_pct:.1%} of bankroll.",
            }

        # === ALL CLEAR — HOLD ===
        peak_info = f" | Peak=${peak_price}" if peak_price > entry_price else ""
        return {
            "action": "hold",
            "current_price": current_price,
            "unrealized_pnl": unrealized_pnl,
            "pnl_pct": pnl_pct,
            "reason": f"Holding. P&L: {pnl_pct:+.0%} (${unrealized_pnl:+.2f}){peak_info}",
        }

    async def _exit_position(self, position: Position, current_price: Decimal, reason: str) -> bool:
        """
        Place a SELL order to exit a position.

        Returns True if the order was placed successfully.
        """
        console.print(
            f"[red]EXITING POSITION: {position.side.upper()} x{position.quantity} "
            f"on {position.kalshi_ticker} @ ${current_price} | {reason}[/red]"
        )

        result = await self._kalshi.place_order(
            ticker=position.kalshi_ticker,
            side=position.side,
            action="sell",
            count=int(position.quantity),
            price=current_price,
        )

        if result is not None:
            # Re-entry recording disabled: caused mid-game churn and bypasses
            # pre-game safety checks (see UCL RMA-BMU 2026-04-07).
            # self._exited_positions[position.kalshi_ticker] = {
            #     "side": position.side,
            #     "entry_price": float(position.avg_price),
            #     "exit_price": float(current_price),
            #     "exit_reason": reason,
            #     "exit_time": time.monotonic(),
            # }

            # Remove from cache and peak tracking (persist to disk)
            self._cache.remove_position(position.kalshi_ticker)
            self._peak_prices.pop(position.kalshi_ticker, None)
            from data.peak_cache import save_peak_prices
            save_peak_prices(self._peak_prices)

            # Update bankroll with sale proceeds (prevents desync until next Kalshi sync)
            proceeds = current_price * position.quantity
            new_bankroll = self._cache.get_bankroll() + proceeds
            self._cache.set_bankroll(new_bankroll)
            console.print(f"[blue]Bankroll updated after exit: +${proceeds:.2f} -> ${new_bankroll:.2f}[/blue]")

            # Send Discord exit alert
            try:
                pnl = (current_price - position.avg_price) * position.quantity
                pnl_pct = float((current_price - position.avg_price) / position.avg_price * 100) if position.avg_price > 0 else 0
                await self._alerter.send_exit_alert(
                    ticker=position.kalshi_ticker,
                    side=position.side,
                    entry_price=position.avg_price,
                    exit_price=current_price,
                    pnl=pnl,
                    pnl_pct=pnl_pct,
                    reason=reason,
                    bankroll=new_bankroll,
                )
            except Exception:
                pass

            console.print(f"[green]Position exited successfully.[/green]")
            return True
        else:
            console.print(f"[red]Exit order failed -- position still open.[/red]")
            return False

    async def _check_cycle(self) -> None:
        """
        One monitoring cycle: check positions, resting orders, and re-entry.
        """
        # 1. Check open positions for stop-loss / profit-take
        positions = self._cache.get_positions()
        for ticker, position in positions.items():
            evaluation = await self._evaluate_position(position)

            if DEBUG_MODE:
                console.print(
                    f"[dim]Position {ticker[:30]}: {evaluation['action']} | "
                    f"P&L={evaluation['pnl_pct']:+.0%} (${evaluation.get('unrealized_pnl', 0):+.2f})[/dim]"
                )

            if evaluation["action"] == "sell" and evaluation["current_price"] is not None:
                await self._exit_position(
                    position, evaluation["current_price"], evaluation["reason"]
                )

            await asyncio.sleep(0.5)

        # 2. Cancel stale resting orders (unfilled limit orders older than 10 min)
        await self._cleanup_resting_orders()

        # 3. Check exited positions for re-entry opportunities
        # DISABLED: Re-entry bypasses pre-game safety checks and causes
        # mid-game churn with double fees (see UCL RMA-BMU 2026-04-07).
        # await self._check_reentry_opportunities()

    async def _cleanup_resting_orders(self) -> None:
        """
        Cancel resting limit orders that haven't filled within 10 minutes.

        Unfilled orders tie up capital and clutter the portfolio.
        If the price moved away from our limit, the edge is gone anyway.
        """
        from config.settings import RESTING_ORDER_TIMEOUT_SECONDS
        max_resting_seconds = RESTING_ORDER_TIMEOUT_SECONDS  # 300s default — Maker orders need time to fill

        try:
            orders = await self._kalshi.get_orders(status="resting")
        except Exception:
            return

        if not orders:
            return

        now_ms = int(time.time() * 1000)

        canceled = 0
        for order in orders:
            order_id = order.get("order_id", "")
            created = order.get("created_time", "")
            ticker = order.get("ticker", "?")

            if not order_id or not created:
                continue

            # Parse creation time — Kalshi returns ISO format
            try:
                from datetime import datetime as dt
                created_dt = dt.fromisoformat(created.replace("Z", "+00:00"))
                age_seconds = (dt.now(timezone.utc) - created_dt).total_seconds()
            except (ValueError, TypeError):
                continue

            if age_seconds > max_resting_seconds:
                console.print(
                    f"[yellow]Canceling stale order: {ticker[:30]} "
                    f"(resting {age_seconds/60:.0f} min)[/yellow]"
                )
                await self._kalshi.cancel_order(order_id)
                canceled += 1
                await asyncio.sleep(0.2)

        if canceled > 0:
            console.print(f"[yellow]Cleaned up {canceled} stale resting orders.[/yellow]")

    async def _check_reentry_opportunities(self) -> None:
        """
        Check if any previously exited positions now offer a better entry.

        Re-entry is NOT automatic. We only FLAG the opportunity for Claude
        to re-evaluate through the normal signal pipeline. Claude decides
        whether there's actually still edge based on current game state.

        Criteria to even flag:
        - At least 3 minutes since exit (avoid whipsaw)
        - Current price is at least 30% below our previous ENTRY price
        - We don't already hold this ticker
        - Max 1 re-entry per ticker (don't keep buying the dip forever)
        - Market must still be open (not near close)
        """
        if not self._exited_positions:
            return

        current_positions = self._cache.get_positions()
        min_wait = 180.0  # 3 minutes since exit (avoid whipsaw)
        reentry_discount = 0.10  # Price must be 10% below previous entry (was 30% — too restrictive for PT re-entries)
        max_reentries = 2  # Allow up to 2 re-entries per ticker (capture multiple swings)

        expired = []
        for ticker, exit_info in self._exited_positions.items():
            # Skip if we already have a position on this ticker
            if ticker in current_positions:
                continue

            # Skip if already re-entered this ticker before
            if exit_info.get("reentry_count", 0) >= max_reentries:
                continue

            # Skip if exited too recently
            time_since_exit = time.monotonic() - exit_info["exit_time"]
            if time_since_exit < min_wait:
                continue

            # Expire old exits (>20 min — game state has changed too much)
            if time_since_exit > 1200:
                expired.append(ticker)
                continue

            # Check current price
            current_price = await self._get_current_price(ticker, exit_info["side"])
            if current_price is None:
                continue

            prev_entry = Decimal(str(exit_info["entry_price"]))
            discount = float((prev_entry - current_price) / prev_entry) if prev_entry > 0 else 0

            if discount >= reentry_discount:
                console.print(
                    f"[yellow]RE-ENTRY FLAG: {ticker} | "
                    f"Prev entry=${prev_entry}, now ${current_price} "
                    f"({discount:.0%} cheaper). Re-evaluating via rules engine.[/yellow]"
                )
                # Mark that we've flagged this re-entry
                exit_info["reentry_count"] = exit_info.get("reentry_count", 0) + 1

                # Push an orderbook update so the signal evaluator sends it
                # to Claude. Claude will decide based on CURRENT game state
                # whether there's still edge — not just because it's cheaper.
                self._cache.update_orderbook(
                    ticker=ticker,
                    best_bid=current_price,
                    best_ask=current_price + Decimal("0.01"),
                    bid_volume=Decimal("1000"),
                    ask_volume=Decimal("1000"),
                )

        # Clean up expired exits
        for ticker in expired:
            del self._exited_positions[ticker]

    async def run(self) -> None:
        """
        Main entry point. Checks positions on a fixed interval.

        Designed to be passed to asyncio.gather() in main.py.
        """
        self._running = True
        console.print(
            f"[blue]Position Monitor: Started (interval={POSITION_CHECK_INTERVAL}s, "
            f"initial-stop={INITIAL_STOP_LOSS_PCT:.0%} / trailing={TRAILING_STOP_PCT:.0%} / "
            f"{BANKROLL_LOSS_THRESHOLD:.0%} bankroll).[/blue]"
        )

        while self._running:
            try:
                await self._check_cycle()
            except Exception as e:
                console.print(
                    f"[red]Position Monitor error: {type(e).__name__}: {e}[/red]"
                )

            await asyncio.sleep(POSITION_CHECK_INTERVAL)

    async def stop(self) -> None:
        """Signal the monitor to stop."""
        self._running = False
        console.print("[blue]Position Monitor: Stopped.[/blue]")


# =============================================================================
# Standalone test
# =============================================================================

if __name__ == "__main__":

    async def _test() -> None:
        from data.cache import get_cache

        console.print("[bold]Testing execution/position_monitor.py...[/bold]\n")

        cache = get_cache()
        cache.set_bankroll(Decimal("29.47"))

        kalshi = KalshiClient()

        # Add a mock position
        pos = Position(
            kalshi_ticker="KXNBAGAME-26APR03NOPSAC-NOP",
            side="no",
            avg_price=Decimal("0.12"),
            quantity=Decimal("30"),
        )
        cache.update_position(pos)

        monitor = PositionMonitor(
            kalshi_client=kalshi,
            cache=cache,
            analyzer=None,
            alerter=None,
        )

        # Test 1: Evaluate position
        console.print("[cyan]1. Evaluate NOP-SAC NO position:[/cyan]")
        evaluation = await monitor._evaluate_position(pos)
        console.print(f"   Action: {evaluation['action']}")
        console.print(f"   Current price: {evaluation['current_price']}")
        console.print(f"   P&L: {evaluation['pnl_pct']:+.0%} (${evaluation['unrealized_pnl']:+.2f})")
        console.print(f"   Reason: {evaluation['reason']}")

        await kalshi.close()
        console.print("\n[green]execution/position_monitor.py: Test complete.[/green]")

    asyncio.run(_test())
