"""
Order lifecycle manager for EdgeRunner.

Orchestrates the full trade flow:
  TradeDecision → Kelly sizing → Kalshi order → Supabase log → cache update

This is the "hands" of the agent — it takes Claude's brain (TradeDecision)
through the risk engine's safety checks, executes on Kalshi, and ensures
every detail is recorded.

The order manager also handles position synchronization — periodically
fetching the real state from Kalshi and reconciling with the local cache.
"""

import time
from datetime import datetime, timezone
from decimal import Decimal

from rich.console import Console

from config.settings import DRY_RUN, MAX_BET_DOLLARS, TRADING_MODE
from data.cache import AgentCache, OrderbookEntry
from execution.kalshi_client import KalshiClient
from execution.risk import KellyResult, calculate_kelly_bet
from signals.schemas import TradeDecision
from storage.models import Market, Position, Trade
from storage.supabase_client import (
    TABLE_MARKETS,
    TABLE_POSITIONS,
    TABLE_TRADES,
    insert_row,
    upsert_row,
)

console = Console()
UTC = timezone.utc


class OrderManager:
    """
    Manages the full lifecycle of trades from decision to execution.

    Usage:
        manager = OrderManager(kalshi_client=client)
        trade = await manager.execute_trade(decision, cache, orderbook)
    """

    def __init__(self, kalshi_client: KalshiClient) -> None:
        self._kalshi = kalshi_client
        self._total_executions: int = 0
        self._total_rejections: int = 0

    async def execute_trade(
        self,
        decision: TradeDecision,
        cache: AgentCache,
        orderbook: OrderbookEntry | None = None,
        max_bankroll: Decimal | None = None,
    ) -> Trade | None:
        """
        Execute a full trade lifecycle.

        Steps:
        1. Run Kelly sizing (final safety gate)
        2. If rejected → log and return None
        3. Place order on Kalshi
        4. Build Trade model from response
        5. Log to Supabase
        6. Update cache (position + bankroll)
        7. Return Trade (used by Telegram alerts)

        Returns the Trade model on success, None if rejected or failed.
        Measures execution_latency_ms from entry to order confirmation.
        """
        start_time = time.monotonic()

        # Step 1: Kelly sizing
        # Use the LOWER of current bankroll and max_bankroll (starting bankroll)
        # This prevents profits from inflating bet sizes within a session
        bankroll = cache.get_bankroll()
        if max_bankroll is not None and max_bankroll < bankroll:
            bankroll = max_bankroll

        spread = orderbook.spread if orderbook else None
        kelly = calculate_kelly_bet(
            decision=decision,
            bankroll=bankroll,
            current_positions=cache.get_position_count(),
            spread=spread,
        )

        # Step 2: Check if rejected
        if kelly.rejected:
            self._total_rejections += 1
            if kelly.reject_reason != "Action is PASS.":
                console.print(
                    f"[yellow]Order rejected: {decision.target_market_id} — "
                    f"{kelly.reject_reason}[/yellow]"
                )
            return None

        # Step 3: Verify liquidity before placing order
        side = "yes" if decision.action == "BUY_YES" else "no"

        # Check that the side we want to buy has a real price
        # For BUY_YES: need best_bid (someone willing to sell YES to us)
        # For BUY_NO: need best_ask < 1.00 (someone willing to sell NO to us)
        if orderbook:
            if side == "yes" and (orderbook.best_bid is None or orderbook.best_bid <= Decimal("0")):
                console.print(
                    f"[yellow]Order skipped: No YES liquidity on "
                    f"{decision.target_market_id}[/yellow]"
                )
                return None
            if side == "no" and (orderbook.best_ask is None or orderbook.best_ask >= Decimal("0.99")):
                console.print(
                    f"[yellow]Order skipped: No NO liquidity on "
                    f"{decision.target_market_id}[/yellow]"
                )
                return None

        # Maker pricing: post limit orders 1c inside the spread instead of crossing it.
        # This makes us a Maker (1.75% fee) instead of Taker (7% fee) — 75% fee reduction.
        # Exception: player props still cross the spread (adverse selection concern).
        from config.settings import MAKER_OFFSET_CENTS
        exec_price = kelly.price
        is_prop = decision.target_market_id.upper().startswith(
            ("KXNBAPTS", "KXNBAREB", "KXNBAAST", "KXNBA3PT", "KXNBABLK", "KXNBASTL")
        )
        if is_prop and orderbook:
            # Props: cross the spread for guaranteed fill (adverse selection)
            if side == "yes" and orderbook.best_ask is not None:
                exec_price = orderbook.best_ask
            elif side == "no" and orderbook.best_bid is not None:
                exec_price = Decimal("1") - orderbook.best_bid
        elif MAKER_OFFSET_CENTS > 0 and not is_prop:
            # Maker pricing: post 1c better than market for Maker fee tier
            offset = Decimal(str(MAKER_OFFSET_CENTS)) / Decimal("100")
            exec_price = max(Decimal("0.01"), kelly.price - offset)
            console.print(
                f"[blue]Maker pricing: {decision.target_market_id[:30]} "
                f"base=${kelly.price} → maker=${exec_price} (offset {MAKER_OFFSET_CENTS}c)[/blue]"
            )

        # Step 4: Place order on Kalshi (or log only in dry-run mode)
        if DRY_RUN:
            latency_ms = int((time.monotonic() - start_time) * 1000)
            console.print(
                f"[cyan]DRY RUN: Would {side.upper()} x{kelly.contracts:.2f} "
                f"@ ${exec_price} on {decision.target_market_id} | "
                f"Edge: {decision.edge:.1%} | Kelly: {kelly.kelly_adjusted:.4f} | "
                f"Cost: ${kelly.bet_amount:.2f}[/cyan]"
            )
            self._total_executions += 1
            order_response = {"order_id": "dry-run"}
        else:
            order_response = await self._kalshi.place_order(
                ticker=decision.target_market_id,
                side=side,
                action="buy",
                count=kelly.contracts,
                price=exec_price,
            )

            latency_ms = int((time.monotonic() - start_time) * 1000)

            if order_response is None:
                console.print(
                    f"[red]Order placement failed for {decision.target_market_id}[/red]"
                )
                return None

            self._total_executions += 1

        # Step 4: Build Trade model
        trade = Trade(
            kalshi_ticker=decision.target_market_id,
            side=side,
            action="buy",
            quantity=Decimal(str(kelly.contracts)),
            price=kelly.price,
            kelly_fraction=Decimal(str(round(kelly.kelly_adjusted, 6))),
            edge=Decimal(str(round(kelly.edge, 6))),
            claude_reasoning=decision.rationale,
            signal_confidence=Decimal(str(round(decision.confidence_score, 4))),
            execution_latency_ms=latency_ms,
            trading_mode=TRADING_MODE,
        )

        console.print(
            f"[green]EXECUTED: {trade.side.upper()} x{trade.quantity} "
            f"@ ${trade.price} on {trade.kalshi_ticker} | "
            f"Kelly: {kelly.kelly_adjusted:.4f} | "
            f"Latency: {latency_ms}ms [{TRADING_MODE.upper()}][/green]"
        )

        # Step 5: Log to Supabase (non-blocking — don't crash if DB fails)
        await self._log_trade(trade)

        # Step 6: Update cache (skip in dry-run — don't alter local state)
        if not DRY_RUN:
            position = Position(
                kalshi_ticker=decision.target_market_id,
                side=side,
                avg_price=kelly.price,
                quantity=Decimal(str(kelly.contracts)),
            )
            cache.update_position(position)

            # Update bankroll (subtract cost of trade)
            trade_cost = kelly.price * Decimal(str(kelly.contracts))
            new_bankroll = cache.get_bankroll() - trade_cost
            cache.set_bankroll(new_bankroll)

        return trade

    async def _log_trade(self, trade: Trade) -> None:
        """Log a trade to Supabase. Failures are logged but never crash."""
        try:
            # Ensure market exists in the markets table
            await upsert_row(TABLE_MARKETS, {
                "kalshi_ticker": trade.kalshi_ticker,
                "title": trade.kalshi_ticker,  # Title will be updated when we have it
            })

            # Insert the trade record
            await insert_row(TABLE_TRADES, trade.to_insert_dict())
        except Exception as e:
            console.print(
                f"[red]Supabase trade log failed: {type(e).__name__}: {e}[/red]"
            )

    async def sync_positions(self, cache: AgentCache) -> None:
        """
        Fetch real positions from Kalshi and reconcile with cache.

        This catches any drift between what the agent thinks it holds
        and what Kalshi actually reports. Run periodically (e.g., every 5 min).
        """
        positions = await self._kalshi.get_positions()

        # Clear cache and rebuild from Kalshi's truth
        current_cached = cache.get_positions()
        synced_tickers: set[str] = set()

        for pos_data in positions:
            ticker = pos_data.get("ticker", pos_data.get("market_ticker", ""))
            if not ticker:
                continue

            # Kalshi returns position_fp: positive = YES, negative = NO
            position_fp = float(pos_data.get("position_fp", pos_data.get("quantity", 0)))
            if position_fp == 0:
                continue  # No active position

            side = "yes" if position_fp > 0 else "no"
            quantity = abs(position_fp)

            # Compute avg price from total cost / quantity
            total_cost = float(pos_data.get("total_traded_dollars", pos_data.get("total_cost_dollars", 0)))
            avg_price = total_cost / quantity if quantity > 0 else 0

            # Fallback: use exposure / quantity
            if avg_price <= 0 or avg_price >= 1:
                exposure = float(pos_data.get("market_exposure_dollars", 0))
                avg_price = exposure / quantity if quantity > 0 else 0

            # Skip if we still can't determine a valid price
            if avg_price <= 0 or avg_price >= 1:
                console.print(f"[yellow]Position sync: skipping {ticker} (can't determine avg price)[/yellow]")
                continue

            position = Position(
                kalshi_ticker=ticker,
                side=side,
                avg_price=Decimal(str(round(avg_price, 4))),
                quantity=Decimal(str(int(quantity))),
            )
            cache.update_position(position)
            synced_tickers.add(ticker)
            console.print(
                f"[green]Position sync: {side.upper()} x{int(quantity)} on {ticker[:40]} "
                f"@ ${avg_price:.4f}[/green]"
            )

        # Remove positions from cache that Kalshi doesn't report
        for ticker in list(current_cached.keys()):
            if ticker not in synced_tickers:
                cache.remove_position(ticker)

        console.print(
            f"[blue]Position sync: {len(synced_tickers)} positions from Kalshi.[/blue]"
        )

    async def sync_bankroll(self, cache: AgentCache) -> None:
        """
        Fetch real balance from Kalshi and update cache.

        Run at startup and periodically to keep bankroll accurate.
        """
        balance = await self._kalshi.get_balance()
        if balance is not None:
            cache.set_bankroll(balance)
            console.print(f"[blue]Bankroll sync: ${balance}[/blue]")
        else:
            console.print("[yellow]Bankroll sync failed — using cached value.[/yellow]")

    def get_status(self) -> dict:
        """Return order manager status for monitoring."""
        return {
            "total_executions": self._total_executions,
            "total_rejections": self._total_rejections,
            "trading_mode": TRADING_MODE,
        }


# =============================================================================
# Standalone test
# =============================================================================

if __name__ == "__main__":
    import asyncio

    async def _test() -> None:
        """
        Test the order manager with mock data.

        Tests the full flow: decision → Kelly → (mock) execution → logging.
        With placeholder credentials, the Kalshi order will fail gracefully.
        """
        from data.cache import get_cache

        console.print("[bold]Testing execution/order_manager.py...[/bold]\n")

        cache = get_cache()
        cache.set_bankroll(Decimal("100.00"))

        kalshi = KalshiClient()
        manager = OrderManager(kalshi_client=kalshi)

        # Test 1: Initialization
        console.print("[cyan]1. Initialization:[/cyan]")
        status = manager.get_status()
        console.print(f"   Mode: {status['trading_mode']}")
        console.print(f"   Executions: {status['total_executions']}")
        console.print(f"   Rejections: {status['total_rejections']}")
        console.print("   [green]Initialization OK.[/green]")

        # Test 2: Execute a strong-edge trade (will fail at Kalshi API)
        console.print("\n[cyan]2. Execute trade (strong edge, will fail at API):[/cyan]")
        from data.cache import OrderbookEntry

        mock_ob = OrderbookEntry("KXNBA-LEBRON-PTS-O25")
        mock_ob.best_bid = Decimal("0.42")
        mock_ob.best_ask = Decimal("0.44")
        mock_ob.ofi = 0.5

        decision = TradeDecision(
            action="BUY_YES",
            target_market_id="KXNBA-LEBRON-PTS-O25",
            implied_market_probability=0.42,
            agent_calculated_probability=0.65,
            kelly_fraction=0.042,
            confidence_score=0.75,
            rationale="Davis OUT, strong edge.",
        )

        trade = await manager.execute_trade(decision, cache, mock_ob)
        if trade:
            console.print(f"   Trade: {trade.side} x{trade.quantity} @ ${trade.price}")
        else:
            console.print("   [yellow]Trade returned None (expected — API creds are placeholders).[/yellow]")

        # Test 3: PASS decision (should be rejected by Kelly)
        console.print("\n[cyan]3. PASS decision (should be rejected):[/cyan]")
        pass_decision = TradeDecision(
            action="PASS",
            target_market_id="KXNBA-TEST",
            implied_market_probability=0.50,
            agent_calculated_probability=0.50,
            kelly_fraction=0.0,
            confidence_score=0.0,
            rationale="No edge.",
        )
        trade = await manager.execute_trade(pass_decision, cache)
        assert trade is None
        console.print("   [green]Correctly rejected PASS decision.[/green]")

        # Test 4: Status after attempts
        console.print("\n[cyan]4. Status:[/cyan]")
        status = manager.get_status()
        for k, v in status.items():
            console.print(f"   {k}: {v}")

        await kalshi.close()
        console.print("\n[green]execution/order_manager.py: Test complete.[/green]")

    asyncio.run(_test())
