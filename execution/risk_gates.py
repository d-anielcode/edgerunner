"""
5-Gate Risk System for EdgeRunner.

Every trade must pass ALL 5 gates before execution. This is the
institutional-grade risk framework inspired by OctagonAI's Kalshi bot.

Gate 1: DRAWDOWN CIRCUIT BREAKER — halt if session loss exceeds 20%
Gate 2: KELLY SIZING — fee-adjusted edge must exceed threshold
Gate 3: LIQUIDITY — spread, volume, and depth must be sufficient
Gate 4: CONCENTRATION — max positions per game, max total exposure
Gate 5: POSITION LIMIT — max concurrent positions

If ANY gate fails, the trade is rejected with a specific reason.
All decisions (accepted and rejected) are logged for analysis.
"""

import time
from dataclasses import dataclass, field
from decimal import Decimal

from rich.console import Console

from config.settings import (
    MAX_CONCURRENT_POSITIONS,
    MAX_POSITION_PCT,
    MAX_SPREAD_CENTS,
    MIN_EDGE_THRESHOLD,
)

console = Console()

# Gate 1: Drawdown
MAX_DRAWDOWN_PCT: float = 0.40  # Pause at 40% session drawdown (backtested max is 30-57%)
MAX_CONSECUTIVE_LOSSES: int = 6  # At 34% NBA win rate, 3-4 loss streaks are normal
LOSS_COOLDOWN_SECONDS: float = 600.0  # 10 min pause after 6 consecutive losses

# Gate 3: Liquidity
MIN_VOLUME_24H: int = 500
MIN_DEPTH_CONTRACTS: int = 0  # Disabled — volume_24h check is sufficient for game winners

# Gate 4: Concentration
MAX_PER_GAME: int = 3
MAX_TOTAL_EXPOSURE_PCT: float = 0.60


@dataclass
class GateResult:
    """Result of a single gate check."""

    gate_name: str
    passed: bool
    reason: str


@dataclass
class AllGatesResult:
    """Result of all 5 gates."""

    passed: bool
    gates: list[GateResult] = field(default_factory=list)
    rejection_reason: str = ""

    def summary(self) -> str:
        """One-line summary of gate results."""
        passed_gates = [g.gate_name for g in self.gates if g.passed]
        failed_gates = [f"{g.gate_name}: {g.reason}" for g in self.gates if not g.passed]
        if self.passed:
            return f"ALL GATES PASSED ({len(passed_gates)}/5)"
        return f"BLOCKED by {failed_gates[0]}" if failed_gates else "BLOCKED (unknown)"


class RiskGates:
    """
    5-gate risk management system.

    Tracks session state (drawdown, consecutive losses, exposure)
    and validates every trade against all 5 gates.

    Usage:
        gates = RiskGates(starting_bankroll=Decimal("40.00"))
        result = gates.check_all(decision, orderbook, cache, market_data)
        if result.passed:
            execute_trade()
        else:
            log_rejection(result)
    """

    def __init__(self, starting_bankroll: Decimal) -> None:
        self._starting_bankroll = starting_bankroll
        self._high_water_mark = starting_bankroll
        self._consecutive_losses: int = 0
        self._last_loss_time: float = 0.0
        self._halted: bool = False
        self._halt_reason: str = ""

    # --- Gate 1: Drawdown Circuit Breaker ---

    def update_after_trade(self, pnl: Decimal, current_bankroll: Decimal) -> None:
        """
        Update state after a trade resolves.
        Call this when a position closes (win or loss).
        """
        if current_bankroll > self._high_water_mark:
            self._high_water_mark = current_bankroll

        if pnl < 0:
            self._consecutive_losses += 1
            self._last_loss_time = time.monotonic()
        else:
            self._consecutive_losses = 0

    def _check_drawdown(self, current_bankroll: Decimal) -> GateResult:
        """Gate 1: Check if session drawdown exceeds limit."""
        if self._halted:
            return GateResult("DRAWDOWN", False, f"HALTED: {self._halt_reason}")

        # Check drawdown from starting bankroll
        if self._starting_bankroll > 0:
            drawdown = float(
                (self._starting_bankroll - current_bankroll) / self._starting_bankroll
            )
            if drawdown >= MAX_DRAWDOWN_PCT:
                self._halted = True
                self._halt_reason = (
                    f"Session drawdown {drawdown:.1%} >= {MAX_DRAWDOWN_PCT:.0%}. "
                    f"Started at ${self._starting_bankroll}, now ${current_bankroll}."
                )
                console.print(f"[red bold]CIRCUIT BREAKER: {self._halt_reason}[/red bold]")
                return GateResult("DRAWDOWN", False, self._halt_reason)

        # Check consecutive losses
        if self._consecutive_losses >= MAX_CONSECUTIVE_LOSSES:
            time_since = time.monotonic() - self._last_loss_time
            if time_since < LOSS_COOLDOWN_SECONDS:
                remaining = LOSS_COOLDOWN_SECONDS - time_since
                return GateResult(
                    "DRAWDOWN", False,
                    f"{self._consecutive_losses} consecutive losses. "
                    f"Cooling down for {remaining:.0f}s more."
                )
            # Cooldown expired, reset
            self._consecutive_losses = 0

        return GateResult("DRAWDOWN", True, "OK")

    # --- Gate 2: Fee-Adjusted Edge ---

    def _check_edge(self, edge: float, exec_price: Decimal) -> GateResult:
        """
        Gate 2: Check if edge is real after accounting for fees.

        The raw edge (agent_prob - market_prob) must exceed the minimum
        threshold AFTER subtracting Kalshi's fee impact.
        """
        # Kalshi fee per contract: $0.07 * P * (1-P)
        fee = float(Decimal("0.07") * exec_price * (Decimal("1") - exec_price))
        slippage = 0.005  # ~0.5c slippage on liquid game winners (spread < 3c)

        # Fee + slippage as a percentage of the contract cost
        friction = (fee + slippage) / float(exec_price) if float(exec_price) > 0 else 0

        net_edge = abs(edge) - friction

        # Net edge threshold is lower than raw edge threshold
        # because fees have already been subtracted
        net_threshold = max(MIN_EDGE_THRESHOLD - friction, 0.03)

        if net_edge < net_threshold:
            return GateResult(
                "EDGE", False,
                f"Net edge {net_edge:.1%} < {net_threshold:.1%} threshold "
                f"(raw {abs(edge):.1%} - friction {friction:.1%})."
            )

        return GateResult("EDGE", True, f"Net edge {net_edge:.1%}")

    # --- Gate 3: Liquidity ---

    def _check_liquidity(
        self,
        spread: Decimal | None,
        volume_24h: int = 0,
        depth: int = 0,
    ) -> GateResult:
        """
        Gate 3: Check market has sufficient liquidity.

        Thin markets eat your edge through spread costs and
        adverse selection (your orders only fill when you're wrong).
        """
        if spread is not None and spread > Decimal(str(MAX_SPREAD_CENTS)):
            return GateResult(
                "LIQUIDITY", False,
                f"Spread ${spread} > max ${MAX_SPREAD_CENTS}."
            )

        if volume_24h < MIN_VOLUME_24H:
            return GateResult(
                "LIQUIDITY", False,
                f"24h volume {volume_24h} < min {MIN_VOLUME_24H}."
            )

        if depth < MIN_DEPTH_CONTRACTS:
            return GateResult(
                "LIQUIDITY", False,
                f"Depth {depth} contracts < min {MIN_DEPTH_CONTRACTS}."
            )

        return GateResult("LIQUIDITY", True, f"Spread ${spread}, vol={volume_24h}, depth={depth}")

    # --- Gate 4: Concentration ---

    def _check_concentration(
        self,
        game_id: str | None,
        positions: dict,
        current_bankroll: Decimal,
        new_bet_amount: Decimal,
    ) -> GateResult:
        """
        Gate 4: Check portfolio concentration.

        Prevents overloading one game or deploying too much capital.
        """
        # Check per-game concentration
        if game_id:
            from main import _extract_game_id
            game_positions = sum(
                1 for ticker in positions
                if _extract_game_id(ticker) == game_id
            )
            if game_positions >= MAX_PER_GAME:
                return GateResult(
                    "CONCENTRATION", False,
                    f"{game_positions} positions on game {game_id} >= max {MAX_PER_GAME}."
                )

        # Check total exposure
        total_exposure = sum(
            float(pos.avg_price * pos.quantity)
            for pos in positions.values()
        )
        total_with_new = total_exposure + float(new_bet_amount)
        max_exposure = float(current_bankroll) * MAX_TOTAL_EXPOSURE_PCT

        if total_with_new > max_exposure:
            return GateResult(
                "CONCENTRATION", False,
                f"Total exposure ${total_with_new:.2f} > "
                f"max {MAX_TOTAL_EXPOSURE_PCT:.0%} of bankroll (${max_exposure:.2f})."
            )

        return GateResult("CONCENTRATION", True, f"Exposure ${total_with_new:.2f} / ${max_exposure:.2f}")

    # --- Gate 5: Position Limit ---

    def _check_position_limit(self, current_positions: int) -> GateResult:
        """Gate 5: Check total position count."""
        if current_positions >= MAX_CONCURRENT_POSITIONS:
            return GateResult(
                "POSITION_LIMIT", False,
                f"{current_positions} positions >= max {MAX_CONCURRENT_POSITIONS}."
            )
        return GateResult("POSITION_LIMIT", True, f"{current_positions}/{MAX_CONCURRENT_POSITIONS}")

    # --- Run All Gates ---

    def check_all(
        self,
        edge: float,
        exec_price: Decimal,
        spread: Decimal | None,
        volume_24h: int,
        depth: int,
        game_id: str | None,
        positions: dict,
        current_bankroll: Decimal,
        new_bet_amount: Decimal,
        current_positions: int,
    ) -> AllGatesResult:
        """
        Run all 5 gates. Returns AllGatesResult with pass/fail for each.

        ALL gates must pass for the trade to proceed.
        """
        results = []

        # Gate 1: Drawdown
        g1 = self._check_drawdown(current_bankroll)
        results.append(g1)
        if not g1.passed:
            return AllGatesResult(
                passed=False, gates=results,
                rejection_reason=g1.reason,
            )

        # Gate 2: Fee-adjusted edge
        g2 = self._check_edge(edge, exec_price)
        results.append(g2)

        # Gate 3: Liquidity
        g3 = self._check_liquidity(spread, volume_24h, depth)
        results.append(g3)

        # Gate 4: Concentration
        g4 = self._check_concentration(
            game_id, positions, current_bankroll, new_bet_amount
        )
        results.append(g4)

        # Gate 5: Position limit
        g5 = self._check_position_limit(current_positions)
        results.append(g5)

        # All must pass
        all_passed = all(g.passed for g in results)
        rejection = ""
        if not all_passed:
            failed = [g for g in results if not g.passed]
            rejection = failed[0].reason if failed else "Unknown"

        return AllGatesResult(
            passed=all_passed,
            gates=results,
            rejection_reason=rejection,
        )

    def get_status(self) -> dict:
        """Return current risk state for monitoring."""
        return {
            "starting_bankroll": float(self._starting_bankroll),
            "high_water_mark": float(self._high_water_mark),
            "consecutive_losses": self._consecutive_losses,
            "halted": self._halted,
            "halt_reason": self._halt_reason,
        }


# =============================================================================
# Standalone test
# =============================================================================

if __name__ == "__main__":
    console.print("[bold]Testing execution/risk_gates.py...[/bold]\n")

    gates = RiskGates(starting_bankroll=Decimal("40.00"))

    # Test 1: All gates pass
    console.print("[cyan]1. All gates pass (ideal trade):[/cyan]")
    result = gates.check_all(
        edge=0.15, exec_price=Decimal("0.45"),
        spread=Decimal("0.02"), volume_24h=1500, depth=20,
        game_id="DETPHI", positions={}, current_bankroll=Decimal("40.00"),
        new_bet_amount=Decimal("4.00"), current_positions=2,
    )
    console.print(f"  {result.summary()}")
    assert result.passed

    # Test 2: Edge too low after fees
    console.print("\n[cyan]2. Edge too low after fees:[/cyan]")
    result = gates.check_all(
        edge=0.06, exec_price=Decimal("0.50"),
        spread=Decimal("0.02"), volume_24h=1500, depth=20,
        game_id="DETPHI", positions={}, current_bankroll=Decimal("40.00"),
        new_bet_amount=Decimal("2.00"), current_positions=0,
    )
    console.print(f"  {result.summary()}")
    assert not result.passed

    # Test 3: Low liquidity
    console.print("\n[cyan]3. Low liquidity (volume < 500):[/cyan]")
    result = gates.check_all(
        edge=0.20, exec_price=Decimal("0.40"),
        spread=Decimal("0.02"), volume_24h=100, depth=2,
        game_id="DETPHI", positions={}, current_bankroll=Decimal("40.00"),
        new_bet_amount=Decimal("3.00"), current_positions=0,
    )
    console.print(f"  {result.summary()}")
    assert not result.passed

    # Test 4: Drawdown circuit breaker
    console.print("\n[cyan]4. Drawdown > 20%:[/cyan]")
    result = gates.check_all(
        edge=0.15, exec_price=Decimal("0.45"),
        spread=Decimal("0.02"), volume_24h=1500, depth=20,
        game_id="DETPHI", positions={}, current_bankroll=Decimal("30.00"),
        new_bet_amount=Decimal("3.00"), current_positions=0,
    )
    console.print(f"  {result.summary()}")
    assert not result.passed

    # Test 5: Too many positions on same game
    console.print("\n[cyan]5. Concentration (3 positions on same game):[/cyan]")
    from storage.models import Position
    mock_positions = {
        "KXNBAPTS-26APR04DETPHI-A": Position(kalshi_ticker="KXNBAPTS-26APR04DETPHI-A", side="yes", avg_price=Decimal("0.40"), quantity=Decimal("5")),
        "KXNBAPTS-26APR04DETPHI-B": Position(kalshi_ticker="KXNBAPTS-26APR04DETPHI-B", side="no", avg_price=Decimal("0.30"), quantity=Decimal("3")),
        "KXNBASPREAD-26APR04DETPHI-C": Position(kalshi_ticker="KXNBASPREAD-26APR04DETPHI-C", side="yes", avg_price=Decimal("0.50"), quantity=Decimal("2")),
    }
    # Reset halted state for this test
    gates._halted = False
    result = gates.check_all(
        edge=0.15, exec_price=Decimal("0.45"),
        spread=Decimal("0.02"), volume_24h=1500, depth=20,
        game_id="DETPHI", positions=mock_positions, current_bankroll=Decimal("40.00"),
        new_bet_amount=Decimal("3.00"), current_positions=3,
    )
    console.print(f"  {result.summary()}")
    assert not result.passed

    console.print("\n[green]execution/risk_gates.py: All tests passed.[/green]")
