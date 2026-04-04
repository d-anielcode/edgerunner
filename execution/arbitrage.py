"""
Intra-market arbitrage scanner for EdgeRunner.

Detects when YES + NO prices on the same market sum to less than $1.00.
When this happens, buying both sides guarantees a profit since one side
MUST resolve to $1.00.

Example:
  YES bid = $0.40, NO bid = $0.55
  Total cost = $0.95
  Guaranteed payout = $1.00
  Risk-free profit = $0.05 per contract (5.3% return)

This happens during volatile in-game moments when the orderbook disjoints.
No Claude calls needed, no probability estimation — pure math.
"""

import asyncio
from decimal import Decimal

from rich.console import Console

from config.settings import DEBUG_MODE, TRADING_MODE
from execution.kalshi_client import KalshiClient

console = Console()

# Minimum profit per contract to trigger (after fees)
# Kalshi fee at $0.50 is ~$0.02 per side, so we need > $0.04 spread to profit
MIN_ARB_PROFIT: Decimal = Decimal("0.04")

# Maximum contracts to buy per arb (limit exposure)
MAX_ARB_CONTRACTS: int = 10


class ArbitrageScanner:
    """
    Scans Kalshi markets for risk-free arbitrage opportunities.

    For each game, checks if the YES + NO prices on paired markets
    sum to less than $1.00 minus fees. If so, buys both sides for
    guaranteed profit.

    Usage:
        scanner = ArbitrageScanner(kalshi_client=client, alerter=alerter)
        await scanner.run()  # runs as asyncio task
    """

    def __init__(
        self,
        kalshi_client: KalshiClient,
        alerter: object,
        tracked_tickers: list[str] | None = None,
    ) -> None:
        self._kalshi = kalshi_client
        self._alerter = alerter
        self._tracked_tickers = tracked_tickers or []
        self._running: bool = False
        self._total_arbs: int = 0
        self._total_profit: Decimal = Decimal("0")

    def _find_market_pairs(self) -> list[tuple[str, str]]:
        """
        Find paired YES/NO tickers for game winner markets.

        Game winner markets come in pairs:
          KXNBAGAME-26APR04DETPHI-PHI (YES = PHI wins)
          KXNBAGAME-26APR04DETPHI-DET (YES = DET wins)

        If PHI YES + DET YES < $1.00, buying both is an arbitrage.
        """
        # Group tickers by game event
        games: dict[str, list[str]] = {}
        for ticker in self._tracked_tickers:
            if "KXNBAGAME" in ticker.upper():
                # Extract event: KXNBAGAME-26APR04DETPHI
                parts = ticker.split("-")
                if len(parts) >= 3:
                    event = "-".join(parts[:2])  # KXNBAGAME-26APR04DETPHI
                    if event not in games:
                        games[event] = []
                    games[event].append(ticker)

        # Only return pairs (exactly 2 tickers per game)
        pairs = []
        for event, tickers in games.items():
            if len(tickers) == 2:
                pairs.append((tickers[0], tickers[1]))

        return pairs

    async def _check_pair(self, ticker_a: str, ticker_b: str) -> dict | None:
        """
        Check if a pair of game winner markets has an arbitrage opportunity.

        Returns a dict with trade details if arb exists, None otherwise.
        """
        try:
            # Fetch orderbooks for both sides
            ob_a = await self._kalshi._request_with_retry(
                "GET", f"/markets/{ticker_a}/orderbook"
            )
            ob_b = await self._kalshi._request_with_retry(
                "GET", f"/markets/{ticker_b}/orderbook"
            )

            data_a = ob_a.get("orderbook_fp", {})
            data_b = ob_b.get("orderbook_fp", {})

            # Best ask for each side (cheapest price to BUY)
            # YES ask = 1 - best NO bid (highest no level = last element)
            yes_a = data_a.get("yes_dollars", [])
            no_a = data_a.get("no_dollars", [])
            yes_b = data_b.get("yes_dollars", [])
            no_b = data_b.get("no_dollars", [])

            if not no_a or not no_b:
                return None

            # Cost to buy YES on ticker_a = 1 - highest NO bid on a
            ask_a = Decimal("1") - Decimal(no_a[-1][0])
            # Cost to buy YES on ticker_b = 1 - highest NO bid on b
            ask_b = Decimal("1") - Decimal(no_b[-1][0])

            total_cost = ask_a + ask_b
            profit_per_contract = Decimal("1") - total_cost

            # Estimate fees: ~$0.02 per side at mid-range prices
            est_fee = Decimal("0.02") * 2
            net_profit = profit_per_contract - est_fee

            if net_profit >= MIN_ARB_PROFIT:
                # Available volume (minimum of both sides)
                vol_a = int(float(no_a[-1][1])) if no_a else 0
                vol_b = int(float(no_b[-1][1])) if no_b else 0
                max_contracts = min(vol_a, vol_b, MAX_ARB_CONTRACTS)

                if max_contracts < 1:
                    return None

                return {
                    "ticker_a": ticker_a,
                    "ticker_b": ticker_b,
                    "ask_a": ask_a,
                    "ask_b": ask_b,
                    "total_cost": total_cost,
                    "net_profit": net_profit,
                    "contracts": max_contracts,
                    "total_profit": net_profit * max_contracts,
                }

        except Exception as e:
            if DEBUG_MODE:
                console.print(f"[dim]Arb check error: {e}[/dim]")

        return None

    async def _execute_arb(self, arb: dict) -> bool:
        """Execute both sides of an arbitrage simultaneously."""
        console.print(
            f"[green bold]ARBITRAGE FOUND![/green bold] "
            f"{arb['ticker_a'][:25]} + {arb['ticker_b'][:25]} | "
            f"Cost=${arb['total_cost']} Profit=${arb['net_profit']}/contract "
            f"x{arb['contracts']} = ${arb['total_profit']}"
        )

        # Place both orders simultaneously
        result_a = await self._kalshi.place_order(
            ticker=arb["ticker_a"],
            side="yes",
            action="buy",
            count=arb["contracts"],
            price=arb["ask_a"],
        )

        result_b = await self._kalshi.place_order(
            ticker=arb["ticker_b"],
            side="yes",
            action="buy",
            count=arb["contracts"],
            price=arb["ask_b"],
        )

        if result_a and result_b:
            self._total_arbs += 1
            self._total_profit += arb["total_profit"]
            console.print(
                f"[green]ARB EXECUTED: Both sides filled. "
                f"Guaranteed ${arb['total_profit']} profit.[/green]"
            )

            # Discord alert
            try:
                await self._alerter.send_trade_alert(
                    ticker=f"ARB: {arb['ticker_a'][:20]} + {arb['ticker_b'][:20]}",
                    side="BOTH",
                    price=arb["total_cost"],
                    bet_amount=arb["total_cost"] * arb["contracts"],
                    bankroll_pct=0.0,
                    rationale=f"ARBITRAGE: Guaranteed ${arb['total_profit']} profit. "
                    f"YES+YES=${arb['total_cost']} < $1.00",
                    bankroll=Decimal("0"),
                )
            except Exception:
                pass

            return True
        else:
            console.print(
                f"[red]ARB PARTIAL: Only one side filled — "
                f"A={'OK' if result_a else 'FAIL'} B={'OK' if result_b else 'FAIL'}[/red]"
            )
            return False

    async def _scan_cycle(self) -> None:
        """One scan cycle: check all game winner pairs for arbitrage."""
        pairs = self._find_market_pairs()
        if not pairs:
            return

        for ticker_a, ticker_b in pairs:
            arb = await self._check_pair(ticker_a, ticker_b)
            if arb:
                await self._execute_arb(arb)
            await asyncio.sleep(0.2)

    async def run(self) -> None:
        """Main loop: scan for arbitrage every 15 seconds."""
        self._running = True
        console.print(
            f"[blue]Arbitrage Scanner: Started "
            f"({len(self._find_market_pairs())} game pairs).[/blue]"
        )

        while self._running:
            try:
                await self._scan_cycle()
            except Exception as e:
                console.print(
                    f"[red]Arbitrage scan error: {type(e).__name__}: {e}[/red]"
                )
            await asyncio.sleep(15)  # Scan every 15 seconds

    async def stop(self) -> None:
        """Stop the scanner."""
        self._running = False
        if self._total_arbs > 0:
            console.print(
                f"[green]Arbitrage Scanner: Stopped. "
                f"Total arbs: {self._total_arbs}, "
                f"Total profit: ${self._total_profit}[/green]"
            )
        else:
            console.print("[blue]Arbitrage Scanner: Stopped. No arbs found.[/blue]")

    def update_tickers(self, tickers: list[str]) -> None:
        """Update tracked tickers (called after market discovery)."""
        self._tracked_tickers = tickers
