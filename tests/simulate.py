"""
EdgeRunner Simulation Suite — test all trading logic without real APIs.

Runs 5 realistic scenarios through the Kelly engine and trailing stop
to verify the agent makes correct decisions and generates profit.
"""

from decimal import Decimal

from execution.position_monitor import (
    AUTO_PROFIT_TAKE_PCT,
    BREAKEVEN_LOCK_PCT,
    INITIAL_STOP_LOSS_PCT,
    TRAILING_STOP_PCT,
)
from execution.risk import calculate_kelly_bet
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from signals.schemas import TradeDecision

console = Console()


def sim_trailing_stop(
    entry: Decimal, prices: list[Decimal]
) -> tuple[Decimal, str]:
    """Simulate trailing stop on a price sequence."""
    peak = entry
    for price in prices:
        if price > peak:
            peak = price
        pnl_pct = float((price - entry) / entry)
        drop_from_peak = float((peak - price) / peak) if peak > 0 else 0
        peak_gain = float((peak - entry) / entry)

        if pnl_pct >= AUTO_PROFIT_TAKE_PCT:
            return price, f"AUTO-TAKE at {pnl_pct:+.0%}"
        if peak > entry and drop_from_peak >= TRAILING_STOP_PCT:
            locked = float((price - entry) / entry)
            return price, f"TRAIL STOP (peak ${peak}, kept {locked:+.0%})"
        if peak_gain >= BREAKEVEN_LOCK_PCT and price <= entry:
            return price, f"BREAKEVEN LOCK (was up {peak_gain:+.0%})"
        if pnl_pct <= -INITIAL_STOP_LOSS_PCT:
            return price, f"STOP-LOSS at {pnl_pct:+.0%}"
    return prices[-1], "HELD TO END (market resolved)"


def sim_kelly(
    action: str, market_prob: float, agent_prob: float, bankroll: Decimal
) -> object:
    """Run Kelly sizing on a simulated decision."""
    decision = TradeDecision(
        action=action,
        target_market_id="SIM-TEST",
        implied_market_probability=market_prob,
        agent_calculated_probability=agent_prob,
        kelly_fraction=0.08,
        confidence_score=0.6,
        rationale="simulation test",
    )
    return calculate_kelly_bet(decision, bankroll, 0, Decimal("0.01"))


def run_scenario(
    name: str,
    description: str,
    action: str,
    market_prob: float,
    agent_prob: float,
    bankroll: Decimal,
    price_sequence: list[str],
) -> dict:
    """Run a single scenario end-to-end."""
    console.print(f"\n[bold cyan]{name}[/bold cyan]")
    console.print(f"{description}")

    kelly = sim_kelly(action, market_prob, agent_prob, bankroll)
    if kelly.rejected:
        console.print(f"[yellow]Kelly REJECTED: {kelly.reject_reason}[/yellow]")
        return {"pnl": Decimal("0"), "bankroll": bankroll, "rejected": True}

    entry = kelly.price
    contracts = kelly.contracts
    cost = entry * contracts
    console.print(
        f"Entry: ${entry} x{contracts} contracts = ${cost:.2f} cost | "
        f"Max payout: ${contracts}"
    )

    prices = [Decimal(p) for p in price_sequence]
    exit_price, reason = sim_trailing_stop(entry, prices)
    pnl = (exit_price - entry) * contracts
    new_bankroll = bankroll - cost + exit_price * contracts

    color = "green" if pnl >= 0 else "red"
    console.print(f"Exit: ${exit_price} | Reason: {reason}")
    console.print(
        f"[{color}]P&L: ${pnl:+.2f} | Bankroll: ${bankroll} -> ${new_bankroll:.2f}[/{color}]"
    )
    return {"pnl": pnl, "bankroll": new_bankroll, "rejected": False}


if __name__ == "__main__":
    console.print(Panel("[bold]EdgeRunner Simulation Suite[/bold]", border_style="blue"))
    console.print(
        f"Rules: Initial stop={INITIAL_STOP_LOSS_PCT:.0%} | "
        f"Trailing={TRAILING_STOP_PCT:.0%} from peak | "
        f"Breakeven lock at {BREAKEVEN_LOCK_PCT:.0%} gain | "
        f"Auto-take at {AUTO_PROFIT_TAKE_PCT:.0%} gain"
    )

    # SCENARIO 1: Tonight's replay
    run_scenario(
        "SCENARIO 1: Tonight's Replay (NOP vs SAC)",
        "BUY_NO on NOP at 75% -> betting SAC wins. SAC comes back and wins.",
        "BUY_NO", 0.75, 0.55, Decimal("40.00"),
        ["0.25","0.28","0.32","0.35","0.30","0.38","0.42","0.48","0.44","0.40",
         "0.52","0.60","0.55","0.48","0.65","0.72","0.80","0.88","0.95"],
    )

    # SCENARIO 2: Bad bet — blown out
    run_scenario(
        "SCENARIO 2: Bad Bet (Team Gets Blown Out)",
        "BUY_YES on underdog at 30%. They get crushed immediately.",
        "BUY_YES", 0.30, 0.45, Decimal("40.00"),
        ["0.30","0.28","0.25","0.22","0.18","0.15","0.12","0.10","0.08","0.06","0.04"],
    )

    # SCENARIO 3: Volatile game
    run_scenario(
        "SCENARIO 3: Volatile Game (Up, Down, Up, Down)",
        "BUY_YES at 50% on close game. Wild swings, trailing stop locks profit.",
        "BUY_YES", 0.50, 0.62, Decimal("40.00"),
        ["0.50","0.55","0.62","0.58","0.65","0.72","0.68","0.60","0.55","0.58",
         "0.63","0.70","0.75","0.80","0.72","0.65","0.60"],
    )

    # SCENARIO 4: Moonshot longshot
    run_scenario(
        "SCENARIO 4: Moonshot (Cheap Longshot Hits)",
        "BUY_YES at 8% on heavy underdog. They shock everyone.",
        "BUY_YES", 0.08, 0.15, Decimal("40.00"),
        ["0.08","0.07","0.06","0.08","0.10","0.15","0.20","0.25","0.30","0.35",
         "0.28","0.35","0.45","0.55","0.65","0.75","0.85","0.92"],
    )

    # SCENARIO 5: Full session — 5 trades
    console.print("\n" + "=" * 60)
    console.print(Panel("[bold]SCENARIO 5: Full Night Session (5 Trades)[/bold]", border_style="green"))

    session_bankroll = Decimal("40.00")
    starting = session_bankroll

    trades = [
        ("Game 1: BUY_NO", "BUY_NO", 0.80, 0.60,
         ["0.20","0.22","0.25","0.30","0.35","0.28","0.32","0.40","0.45","0.50","0.42"]),
        ("Game 2: BUY_YES", "BUY_YES", 0.45, 0.58,
         ["0.45","0.48","0.52","0.55","0.50","0.47","0.42","0.38","0.35"]),
        ("Game 3: BUY_YES", "BUY_YES", 0.35, 0.48,
         ["0.35","0.38","0.42","0.48","0.55","0.60","0.52","0.58","0.65","0.70","0.62"]),
        ("Game 4: BUY_NO", "BUY_NO", 0.65, 0.50,
         ["0.35","0.38","0.42","0.45","0.40","0.38","0.35","0.30","0.25","0.20","0.15"]),
        ("Game 5: BUY_YES", "BUY_YES", 0.55, 0.68,
         ["0.55","0.58","0.62","0.65","0.70","0.75","0.80","0.78","0.72","0.68"]),
    ]

    table = Table(title="Session Results")
    table.add_column("#", style="dim")
    table.add_column("Trade", style="cyan")
    table.add_column("Entry")
    table.add_column("Exit")
    table.add_column("Reason", width=28)
    table.add_column("P&L", style="bold")
    table.add_column("Bankroll")

    for i, (name, action, mkt_prob, agent_prob, price_list) in enumerate(trades, 1):
        kelly = sim_kelly(action, mkt_prob, agent_prob, min(session_bankroll, starting))
        if kelly.rejected:
            table.add_row(
                str(i), name, "-", "-", kelly.reject_reason[:28],
                "$0", f"${session_bankroll:.2f}",
            )
            continue

        entry = kelly.price
        cost = entry * kelly.contracts
        prices = [Decimal(p) for p in price_list]
        exit_price, reason = sim_trailing_stop(entry, prices)
        pnl = (exit_price - entry) * kelly.contracts
        session_bankroll = session_bankroll - cost + exit_price * kelly.contracts

        color = "green" if pnl >= 0 else "red"
        table.add_row(
            str(i), name, f"${entry}", f"${exit_price}",
            reason[:28], f"[{color}]${pnl:+.2f}[/{color}]",
            f"${session_bankroll:.2f}",
        )

    console.print(table)

    total_pnl = session_bankroll - starting
    pct = float(total_pnl / starting) * 100
    color = "green" if total_pnl >= 0 else "red"
    console.print(
        f"\n[{color} bold]SESSION RESULT: ${starting} -> ${session_bankroll:.2f} "
        f"({pct:+.1f}%)[/{color} bold]"
    )
