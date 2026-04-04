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
TRAILING_STOP_PCT: float = 0.25  # Sell if price drops 25% from its peak

# Breakeven lock: once position is up this much, floor moves to entry price
BREAKEVEN_LOCK_PCT: float = 0.50  # Up 50% → never let it become a loss

# Bankroll-based stop
BANKROLL_LOSS_THRESHOLD: float = 0.02  # 2% of bankroll loss triggers review

# Auto profit-take (no Claude needed)
AUTO_PROFIT_TAKE_PCT: float = 2.00  # 200% gain → auto-sell, lock it in


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
        self._peak_prices: dict[str, Decimal] = {}
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

    async def _evaluate_position(self, position: Position) -> dict:
        """
        Evaluate whether a position should be held or exited.

        Uses a TRAILING STOP-LOSS that ratchets up as the position profits:
        1. Initial stop: sell if down 50% from entry
        2. Once up 50%: floor moves to entry (never let a winner become a loser)
        3. Trailing stop: sell if price drops 25% from its peak
        4. Auto-take: sell if up 200% (lock in the bag)

        Returns a dict with action, current_price, unrealized_pnl, pnl_pct, reason.
        """
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

        # Update peak price tracking
        ticker = position.kalshi_ticker
        prev_peak = self._peak_prices.get(ticker, entry_price)
        if current_price > prev_peak:
            self._peak_prices[ticker] = current_price
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
            console.print(
                f"[red]STOP-LOSS: {ticker} down {pnl_pct:+.0%} from entry. "
                f"Asking Claude: exit or hold?[/red]"
            )
            try:
                decision = await self._ask_claude_about_exit(
                    position, current_price, unrealized_pnl,
                    f"Down {pnl_pct:.0%} from entry (threshold: {-INITIAL_STOP_LOSS_PCT:.0%})",
                )
                return {
                    "action": decision,
                    "current_price": current_price,
                    "unrealized_pnl": unrealized_pnl,
                    "pnl_pct": pnl_pct,
                    "reason": f"Stop-loss review: down {pnl_pct:+.0%}. Claude says {decision}.",
                }
            except Exception:
                return {
                    "action": "sell",
                    "current_price": current_price,
                    "unrealized_pnl": unrealized_pnl,
                    "pnl_pct": pnl_pct,
                    "reason": f"Stop-loss (Claude unavailable): down {pnl_pct:+.0%}.",
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

        # Position triggered a review — ask Claude
        console.print(
            f"[yellow]Position review triggered: {position.kalshi_ticker} | "
            f"Entry=${entry_price} Current=${current_price} | "
            f"P&L: {pnl_pct:+.0%} (${unrealized_pnl:+.2f}) | {trigger_reason}[/yellow]"
        )

        # Use Claude to decide (if analyzer is available)
        try:
            decision = await self._ask_claude_about_exit(position, current_price, unrealized_pnl, trigger_reason)
            return {
                "action": decision,
                "current_price": current_price,
                "unrealized_pnl": unrealized_pnl,
                "pnl_pct": pnl_pct,
                "reason": trigger_reason,
            }
        except Exception as e:
            console.print(f"[red]Claude exit review failed: {e} — defaulting to SELL[/red]")
            return {
                "action": "sell",
                "current_price": current_price,
                "unrealized_pnl": unrealized_pnl,
                "pnl_pct": pnl_pct,
                "reason": f"Claude unavailable, auto-exiting on stop-loss: {trigger_reason}",
            }

    async def _ask_claude_about_exit(
        self,
        position: Position,
        current_price: Decimal,
        unrealized_pnl: Decimal,
        trigger_reason: str,
    ) -> str:
        """
        Ask Claude whether to exit a losing position.

        Returns "sell" or "hold".
        """
        import anthropic
        from config.settings import ANTHROPIC_API_KEY

        client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)

        prompt = f"""You are a risk manager for a prediction market trading bot. A position has triggered a stop-loss review.

POSITION:
- Market: {position.kalshi_ticker}
- Side: {position.side.upper()}
- Entry Price: ${position.avg_price}
- Current Price: ${current_price}
- Quantity: {position.quantity} contracts
- Unrealized P&L: ${unrealized_pnl:+.2f}
- Trigger: {trigger_reason}

Should we EXIT (sell to cut losses) or HOLD (keep the position)?

Rules:
- If the event hasn't happened yet and fundamentals haven't changed, HOLD may be correct
- If the price move suggests the market has new information we don't have, EXIT
- If the loss exceeds 3% of bankroll, strongly consider EXIT
- Cutting small losses early is better than hoping for recovery

Respond with EXACTLY one word: SELL or HOLD"""

        try:
            response = await client.messages.create(
                model="claude-haiku-4-5",
                max_tokens=10,
                messages=[{"role": "user", "content": prompt}],
            )
            answer = response.content[0].text.strip().upper()
            return "sell" if "SELL" in answer else "hold"
        except Exception:
            return "sell"  # Default to cutting losses if Claude is unavailable

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
            # Record exit for potential re-entry
            self._exited_positions[position.kalshi_ticker] = {
                "side": position.side,
                "entry_price": float(position.avg_price),
                "exit_price": float(current_price),
                "exit_reason": reason,
                "exit_time": time.monotonic(),
            }

            # Remove from cache and peak tracking
            self._cache.remove_position(position.kalshi_ticker)
            self._peak_prices.pop(position.kalshi_ticker, None)

            # Send Discord alert
            try:
                pnl = (current_price - position.avg_price) * position.quantity
                color_word = "PROFIT" if pnl >= 0 else "LOSS"
                await self._alerter.send_trade_alert(
                    ticker=position.kalshi_ticker,
                    side=f"SELL {position.side}",
                    price=current_price,
                    bet_amount=abs(pnl),
                    bankroll_pct=0.0,
                    rationale=f"{color_word}: {reason}",
                    bankroll=self._cache.get_bankroll() + (current_price * position.quantity),
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
        One monitoring cycle: check open positions and re-entry opportunities.
        """
        # Check open positions for stop-loss / profit-take
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

        # Check exited positions for re-entry opportunities
        await self._check_reentry_opportunities()

    async def _check_reentry_opportunities(self) -> None:
        """
        Check if any previously exited positions now offer a better entry.

        Re-entry criteria:
        - At least 2 minutes since exit (avoid whipsaw)
        - Current price is at least 30% below our previous entry price
        - We don't already have a position on this game
        """
        if not self._exited_positions:
            return

        # Don't re-enter tickers we currently hold
        current_positions = self._cache.get_positions()
        min_wait = 120.0  # 2 minutes since exit
        reentry_discount = 0.30  # Price must be 30% below previous entry

        expired = []
        for ticker, exit_info in self._exited_positions.items():
            # Skip if we already have a position on this ticker
            if ticker in current_positions:
                continue

            # Skip if exited too recently
            time_since_exit = time.monotonic() - exit_info["exit_time"]
            if time_since_exit < min_wait:
                continue

            # Expire old exits (>30 min — market has likely changed too much)
            if time_since_exit > 1800:
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
                    f"[green]RE-ENTRY OPPORTUNITY: {ticker} | "
                    f"Prev entry=${prev_entry}, now ${current_price} "
                    f"({discount:.0%} cheaper). Flagging for Claude.[/green]"
                )
                # Push an orderbook update to the queue so the signal evaluator
                # picks it up and sends it to Claude for analysis
                from data.cache import OrderbookUpdate
                update = self._cache.update_orderbook(
                    ticker=ticker,
                    best_bid=current_price,
                    best_ask=current_price + Decimal("0.01"),
                    bid_volume=Decimal("1000"),
                    ask_volume=Decimal("1000"),
                )
                # The signal evaluator will process this via the normal flow

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
            f"stop-loss={PRICE_MOVE_THRESHOLD:.0%} price / "
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
