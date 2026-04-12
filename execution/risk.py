"""
Kelly Criterion sizing engine for EdgeRunner.

This module is the final safety gate between Claude's recommendation and
a real trade. It takes a TradeDecision and calculates the exact dollar
amount to wager, accounting for fees, slippage, and hard safety limits.

The Kelly Criterion maximizes long-term bankroll growth:
  f* = (bp - q) / b
Where:
  f* = optimal fraction of bankroll to wager
  p  = true probability of winning (Claude's estimate)
  q  = 1 - p
  b  = net odds = (payout - cost) / cost

CRITICAL: Claude RECOMMENDS, this module GATES. Every safety check
from the PRD is enforced here, not in the LLM.
"""

from decimal import Decimal

from pydantic import BaseModel
from rich.console import Console
from rich.table import Table

from config.settings import (
    FRACTIONAL_KELLY,
    MAX_BET_DOLLARS,
    MAX_CONCURRENT_POSITIONS,
    MAX_POSITION_PCT,
    MAX_SPREAD_CENTS,
    MIN_EDGE_THRESHOLD,
)
from signals.schemas import TradeDecision

console = Console()

# Kalshi fee formula constants — Taker rate (crossing the spread)
KALSHI_FEE_PER_CONTRACT: Decimal = Decimal("0.07")

# Assumed adverse slippage on fill price
SLIPPAGE_BUFFER: Decimal = Decimal("0.005")


class KellyResult(BaseModel):
    """
    The output of the Kelly sizing calculation.

    Contains the recommended bet size, the raw and adjusted Kelly fractions,
    and whether the trade was rejected by any safety check.
    """

    bet_amount: Decimal
    """Dollar amount to wager. $0 if rejected."""

    contracts: int
    """Number of Kalshi contracts to buy (bet_amount / price)."""

    kelly_raw: float
    """Full Kelly fraction before adjustments."""

    kelly_adjusted: float
    """Kelly fraction after fractional multiplier and caps."""

    price: Decimal
    """The execution price (YES price for BUY_YES, 1 - YES price for BUY_NO)."""

    rejected: bool
    """Whether the trade was rejected by a safety check."""

    reject_reason: str
    """Why the trade was rejected (empty string if not rejected)."""

    edge: float
    """The calculated edge after fee/slippage adjustments."""

    fee_per_contract: Decimal
    """Estimated Kalshi fee per contract."""


def calculate_kalshi_fee(price: Decimal) -> Decimal:
    """
    Calculate the Kalshi fee per contract with ceil() rounding.

    Kalshi fee formula: fee_rate * P * (1 - P), rounded UP to nearest cent.
    Maker rate: 0.0175, Taker rate: 0.07.
    """
    import math
    raw = float(KALSHI_FEE_PER_CONTRACT * price * (Decimal("1") - price))
    return Decimal(str(math.ceil(raw * 100) / 100))


def calculate_kelly_bet(
    decision: TradeDecision,
    bankroll: Decimal,
    current_positions: int,
    spread: Decimal | None = None,
) -> KellyResult:
    """
    Calculate the optimal bet size for a TradeDecision.

    This is the FINAL SAFETY GATE. Every check from the PRD is enforced:
    1. PASS actions → reject immediately
    2. Edge threshold → reject if edge < MIN_EDGE_THRESHOLD
    3. Spread check → reject if spread > MAX_SPREAD_CENTS
    4. Position count → reject if at MAX_CONCURRENT_POSITIONS
    5. Kelly math → compute raw Kelly, apply fractional multiplier
    6. Position cap → cap at MAX_POSITION_PCT of bankroll
    7. Minimum viable bet → reject if bet < $0.01

    Returns KellyResult with all details for logging.
    """
    # Determine execution price based on action
    if decision.action == "BUY_YES":
        exec_price = Decimal(str(decision.implied_market_probability))
        true_prob = decision.agent_calculated_probability
    elif decision.action == "BUY_NO":
        exec_price = Decimal("1") - Decimal(str(decision.implied_market_probability))
        true_prob = 1.0 - decision.agent_calculated_probability
    else:
        # PASS — no trade
        return KellyResult(
            bet_amount=Decimal("0"),
            contracts=0,
            kelly_raw=0.0,
            kelly_adjusted=0.0,
            price=Decimal("0.50"),
            rejected=True,
            reject_reason="Action is PASS.",
            edge=0.0,
            fee_per_contract=Decimal("0"),
        )

    fee = calculate_kalshi_fee(exec_price)

    # --- Safety Check 1: Edge threshold ---
    edge = abs(decision.agent_calculated_probability - decision.implied_market_probability)
    if edge < MIN_EDGE_THRESHOLD:
        return KellyResult(
            bet_amount=Decimal("0"),
            contracts=0,
            kelly_raw=0.0,
            kelly_adjusted=0.0,
            price=exec_price,
            rejected=True,
            reject_reason=f"Edge {edge:.1%} below threshold {MIN_EDGE_THRESHOLD:.1%}.",
            edge=edge,
            fee_per_contract=fee,
        )

    # --- Safety Check 2: Spread ---
    if spread is not None and spread > Decimal(str(MAX_SPREAD_CENTS)):
        return KellyResult(
            bet_amount=Decimal("0"),
            contracts=0,
            kelly_raw=0.0,
            kelly_adjusted=0.0,
            price=exec_price,
            rejected=True,
            reject_reason=f"Spread ${spread} exceeds max ${MAX_SPREAD_CENTS}.",
            edge=edge,
            fee_per_contract=fee,
        )

    # --- Safety Check 3: Position count ---
    if current_positions >= MAX_CONCURRENT_POSITIONS:
        return KellyResult(
            bet_amount=Decimal("0"),
            contracts=0,
            kelly_raw=0.0,
            kelly_adjusted=0.0,
            price=exec_price,
            rejected=True,
            reject_reason=f"At max positions ({current_positions}/{MAX_CONCURRENT_POSITIONS}).",
            edge=edge,
            fee_per_contract=fee,
        )

    # --- Kelly Calculation ---
    # Adjust payout for fees and slippage
    # If buying YES at $0.42: payout = $1.00, profit = $0.58
    # After fee and slippage: profit = $0.58 - fee - slippage
    payout = Decimal("1.00")
    cost = exec_price
    net_profit = payout - cost - fee - SLIPPAGE_BUFFER

    if net_profit <= 0:
        return KellyResult(
            bet_amount=Decimal("0"),
            contracts=0,
            kelly_raw=0.0,
            kelly_adjusted=0.0,
            price=exec_price,
            rejected=True,
            reject_reason=f"No profit after fees (${fee}) and slippage (${SLIPPAGE_BUFFER}).",
            edge=edge,
            fee_per_contract=fee,
        )

    # b = net odds = net_profit / cost
    b = float(net_profit / cost)
    p = true_prob
    q = 1.0 - p

    # Kelly formula: f* = (bp - q) / b
    kelly_raw = (b * p - q) / b if b > 0 else 0.0

    if kelly_raw <= 0:
        return KellyResult(
            bet_amount=Decimal("0"),
            contracts=0,
            kelly_raw=kelly_raw,
            kelly_adjusted=0.0,
            price=exec_price,
            rejected=True,
            reject_reason=f"Negative Kelly ({kelly_raw:.4f}) — no edge after adjustments.",
            edge=edge,
            fee_per_contract=fee,
        )

    # --- Apply fractional Kelly ---
    # Use the sport-specific Kelly from rules.py (decision.kelly_fraction) if available.
    # This respects SPORT_PARAMS per-sport aggression levels and OOS-validated cuts.
    # Falls back to global FRACTIONAL_KELLY only if decision doesn't carry a kelly_fraction.
    if decision.kelly_fraction > 0:
        kelly_adjusted = decision.kelly_fraction
    else:
        kelly_adjusted = kelly_raw * FRACTIONAL_KELLY

    # --- Cap at max position percentage ---
    kelly_adjusted = min(kelly_adjusted, MAX_POSITION_PCT)

    # --- Calculate dollar amount ---
    bet_amount = bankroll * Decimal(str(kelly_adjusted))

    # Apply hard dollar cap per trade
    max_bet = Decimal(str(MAX_BET_DOLLARS))
    if bet_amount > max_bet:
        bet_amount = max_bet

    # Round down to nearest cent
    bet_amount = bet_amount.quantize(Decimal("0.01"))

    # Calculate contracts (integer, rounded down)
    contracts = int(bet_amount / exec_price) if exec_price > 0 else 0

    # --- Safety Check 4: Minimum viable bet ---
    # Below $1, the bet isn't worth the fee overhead and tracking
    if bet_amount < Decimal("1.00") or contracts < 1:
        return KellyResult(
            bet_amount=Decimal("0"),
            contracts=0,
            kelly_raw=kelly_raw,
            kelly_adjusted=kelly_adjusted,
            price=exec_price,
            rejected=True,
            reject_reason="Bet amount too small for a single contract.",
            edge=edge,
            fee_per_contract=fee,
        )

    return KellyResult(
        bet_amount=bet_amount,
        contracts=contracts,
        kelly_raw=kelly_raw,
        kelly_adjusted=kelly_adjusted,
        price=exec_price,
        rejected=False,
        reject_reason="",
        edge=edge,
        fee_per_contract=fee,
    )


# =============================================================================
# Standalone test
# =============================================================================

if __name__ == "__main__":
    console.print("[bold]Testing execution/risk.py...[/bold]\n")

    scenarios = [
        {
            "name": "Strong edge (BUY_YES, 23% edge)",
            "decision": TradeDecision(
                action="BUY_YES",
                target_market_id="KXNBA-LEBRON-PTS-O25",
                implied_market_probability=0.42,
                agent_calculated_probability=0.65,
                kelly_fraction=0.042,
                confidence_score=0.75,
                rationale="Davis OUT, strong edge.",
            ),
            "bankroll": Decimal("100.00"),
            "positions": 3,
            "spread": Decimal("0.02"),
            "expect_rejected": False,
        },
        {
            "name": "Weak edge (below 5% threshold)",
            "decision": TradeDecision(
                action="BUY_YES",
                target_market_id="KXNBA-CURRY-PTS-O28",
                implied_market_probability=0.38,
                agent_calculated_probability=0.41,
                kelly_fraction=0.01,
                confidence_score=0.45,
                rationale="Marginal edge.",
            ),
            "bankroll": Decimal("100.00"),
            "positions": 0,
            "spread": Decimal("0.02"),
            "expect_rejected": True,
        },
        {
            "name": "Wide spread (over $0.03)",
            "decision": TradeDecision(
                action="BUY_YES",
                target_market_id="KXNBA-JOKIC-REB-O11",
                implied_market_probability=0.55,
                agent_calculated_probability=0.72,
                kelly_fraction=0.03,
                confidence_score=0.70,
                rationale="Good edge but wide spread.",
            ),
            "bankroll": Decimal("100.00"),
            "positions": 2,
            "spread": Decimal("0.08"),
            "expect_rejected": True,
        },
        {
            "name": "Max positions reached (10/10)",
            "decision": TradeDecision(
                action="BUY_NO",
                target_market_id="KXNBA-LUKA-PTS-O30",
                implied_market_probability=0.65,
                agent_calculated_probability=0.42,
                kelly_fraction=0.04,
                confidence_score=0.70,
                rationale="Good edge, but at max positions.",
            ),
            "bankroll": Decimal("500.00"),
            "positions": 10,
            "spread": Decimal("0.02"),
            "expect_rejected": True,
        },
        {
            "name": "PASS action",
            "decision": TradeDecision(
                action="PASS",
                target_market_id="KXNBA-TEST",
                implied_market_probability=0.50,
                agent_calculated_probability=0.50,
                kelly_fraction=0.0,
                confidence_score=0.0,
                rationale="No edge.",
            ),
            "bankroll": Decimal("100.00"),
            "positions": 0,
            "spread": Decimal("0.02"),
            "expect_rejected": True,
        },
        {
            "name": "BUY_NO with strong edge",
            "decision": TradeDecision(
                action="BUY_NO",
                target_market_id="KXNBA-EMBIID-PTS-O28",
                implied_market_probability=0.70,
                agent_calculated_probability=0.45,
                kelly_fraction=0.04,
                confidence_score=0.72,
                rationale="Embiid on minutes restriction.",
            ),
            "bankroll": Decimal("200.00"),
            "positions": 1,
            "spread": Decimal("0.02"),
            "expect_rejected": False,
        },
    ]

    table = Table(title="Kelly Criterion Test Scenarios")
    table.add_column("Scenario", style="cyan", width=35)
    table.add_column("Result", style="bold")
    table.add_column("Bet", style="green")
    table.add_column("Kelly Raw", style="yellow")
    table.add_column("Kelly Adj", style="yellow")
    table.add_column("Edge", style="blue")
    table.add_column("Reason", style="dim", width=30)

    all_passed = True
    for s in scenarios:
        result = calculate_kelly_bet(
            decision=s["decision"],
            bankroll=s["bankroll"],
            current_positions=s["positions"],
            spread=s["spread"],
        )

        status = "REJECTED" if result.rejected else "TRADE"
        color = "red" if result.rejected else "green"
        correct = result.rejected == s["expect_rejected"]
        if not correct:
            all_passed = False
            status = f"WRONG ({status})"
            color = "red bold"

        table.add_row(
            s["name"],
            f"[{color}]{status}[/{color}]",
            f"${result.bet_amount}" if not result.rejected else "-",
            f"{result.kelly_raw:.4f}",
            f"{result.kelly_adjusted:.4f}",
            f"{result.edge:.1%}",
            result.reject_reason[:30] if result.reject_reason else "OK",
        )

    console.print(table)

    # Detail on the strong edge trade
    console.print("\n[cyan]Detail — Strong edge scenario:[/cyan]")
    s = scenarios[0]
    result = calculate_kelly_bet(s["decision"], s["bankroll"], s["positions"], s["spread"])
    console.print(f"  Price: ${result.price}")
    console.print(f"  Fee/contract: ${result.fee_per_contract:.4f}")
    console.print(f"  Kelly raw: {result.kelly_raw:.4f} ({result.kelly_raw * 100:.1f}% of bankroll)")
    console.print(f"  Kelly adjusted (x{FRACTIONAL_KELLY}): {result.kelly_adjusted:.4f} ({result.kelly_adjusted * 100:.1f}%)")
    console.print(f"  Bet amount: ${result.bet_amount}")
    console.print(f"  Contracts: {result.contracts}")
    console.print(f"  Max position cap: {MAX_POSITION_PCT * 100}%")

    if all_passed:
        console.print("\n[green]execution/risk.py: All tests passed.[/green]")
    else:
        console.print("\n[red]execution/risk.py: SOME TESTS FAILED.[/red]")
