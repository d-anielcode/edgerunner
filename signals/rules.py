"""
Rules-based market evaluator for EdgeRunner v2.

Replaces the Claude LLM as the trading decision engine.
Based on analysis of 6.5M real Kalshi NBA trades:

Core strategy: Buy NO on NBA game winners where YES > 60c.
- Favorites are systematically overpriced by retail traders
- 73% of takers buy YES but win only 31.5%
- Buying NO on favorites yields +22-34% ROI after fees
- 33% win rate with 3:1 payout ratio = profitable

No LLM needed. Pure math.
"""

from decimal import Decimal

from rich.console import Console

from config.markets import is_nba_market
from data.cache import OrderbookEntry
from signals.schemas import TradeDecision, pass_decision

console = Console()

# Empirical edge by YES price bucket (from 738 settled game winner markets)
# Maps YES price range to (actual YES hit rate, documented NO ROI)
EDGE_TABLE = {
    # (min_yes, max_yes): (yes_hit_rate, no_roi_after_fees)
    (61, 75): (0.593, 0.220),  # YES hits 59.3%, NO ROI +22.0% after fees
    (76, 99): (0.758, 0.344),  # YES hits 75.8%, NO ROI +34.4% after fees
}

# Below 60c YES, NO is not profitable — skip
MIN_YES_PRICE: Decimal = Decimal("0.60")

# Maximum YES price — extremely heavy favorites (99c) have tiny NO payouts
MAX_YES_PRICE: Decimal = Decimal("0.95")

# Minimum spread to ensure we can actually execute
MAX_SPREAD: Decimal = Decimal("0.05")


class RulesEvaluator:
    """
    Rules-based trading evaluator.

    Evaluates NBA game winner markets using empirically-proven rules
    from 7,365 markets and 6.5M trades of Kalshi data.

    No LLM calls. No API costs. Instant evaluation.

    Usage:
        evaluator = RulesEvaluator()
        decision = evaluator.evaluate_market(ticker, orderbook, espn_game)
    """

    def __init__(self) -> None:
        self._total_evaluated: int = 0
        self._total_signals: int = 0

    def evaluate_market(
        self,
        ticker: str,
        title: str,
        orderbook: OrderbookEntry | None,
        espn_game: dict | None = None,
    ) -> TradeDecision:
        """
        Evaluate a single market using rules.

        Returns BUY_NO if the market meets all criteria, PASS otherwise.
        """
        self._total_evaluated += 1

        # Rule 1: Must be a game winner market
        if "KXNBAGAME" not in ticker.upper():
            return pass_decision(ticker, "Not a game winner market.")

        # Rule 2: Must have orderbook data
        if orderbook is None or orderbook.best_bid is None:
            return pass_decision(ticker, "No orderbook data.")

        yes_price = orderbook.best_bid
        spread = orderbook.spread

        # Rule 3: YES must be above threshold (favorite is overpriced)
        if yes_price < MIN_YES_PRICE:
            return pass_decision(
                ticker,
                f"YES ${yes_price} < ${MIN_YES_PRICE} — not a strong enough favorite to fade.",
            )

        # Rule 4: YES must not be too extreme (tiny payout on NO)
        if yes_price > MAX_YES_PRICE:
            return pass_decision(
                ticker,
                f"YES ${yes_price} > ${MAX_YES_PRICE} — NO payout too small.",
            )

        # Rule 5: Spread must be tradeable
        if spread is not None and spread > MAX_SPREAD:
            return pass_decision(
                ticker,
                f"Spread ${spread} > ${MAX_SPREAD} — too expensive to cross.",
            )

        # Rule 6: ESPN veto — skip if game is in progress and favorite is winning big
        if espn_game:
            status = espn_game.get("status", "")
            if status == "Final":
                return pass_decision(ticker, "Game already finished.")

            if status == "In Progress":
                home_score = espn_game.get("home_score", 0)
                away_score = espn_game.get("away_score", 0)
                quarter = espn_game.get("quarter", 0)
                score_diff = abs(home_score - away_score)

                # If favorite is winning by 15+ in Q3/Q4, don't fade
                if quarter >= 3 and score_diff > 15:
                    return pass_decision(
                        ticker,
                        f"Favorite winning by {score_diff} in Q{quarter} — don't fade a blowout.",
                    )

        # All rules pass — compute edge and generate BUY_NO signal
        no_price = Decimal("1") - yes_price
        yes_price_cents = int(yes_price * 100)

        # Look up empirical edge
        actual_yes_rate = 0.65  # default conservative estimate
        for (min_c, max_c), (hit_rate, _) in EDGE_TABLE.items():
            if min_c <= yes_price_cents <= max_c:
                actual_yes_rate = hit_rate
                break

        # Edge = market implied YES probability - actual YES probability
        # Market says YES is yes_price (e.g., 70%), actual is ~59% → 11% edge on NO
        market_prob = float(yes_price)
        agent_prob = actual_yes_rate
        edge = market_prob - agent_prob  # Positive = YES is overpriced = NO has edge

        if edge < 0.05:
            return pass_decision(
                ticker,
                f"Edge {edge:.1%} too small (market {market_prob:.0%} vs actual {agent_prob:.0%}).",
            )

        # Kelly fraction based on edge and payout
        # b = (1 - no_price) / no_price = yes_price / no_price
        b = float(yes_price / no_price) if no_price > 0 else 0
        p = 1 - actual_yes_rate  # Probability NO wins
        q = actual_yes_rate  # Probability NO loses

        kelly_raw = (b * p - q) / b if b > 0 else 0
        kelly_fraction = max(0.0, min(kelly_raw * 0.35, 0.15))  # 0.35x Kelly, cap 15%

        self._total_signals += 1

        rationale = (
            f"Fade favorite: YES priced at ${yes_price} ({market_prob:.0%}) "
            f"but historically hits only {actual_yes_rate:.0%}. "
            f"Edge: {edge:.0%}. Buy NO at ${no_price}."
        )

        console.print(
            f"[green]RULE SIGNAL: BUY_NO on {ticker[:30]} | "
            f"YES=${yes_price} | Edge={edge:.1%} | Kelly={kelly_fraction:.3f}[/green]"
        )

        return TradeDecision(
            action="BUY_NO",
            target_market_id=ticker,
            implied_market_probability=round(market_prob, 4),
            agent_calculated_probability=round(agent_prob, 4),
            kelly_fraction=round(kelly_fraction, 4),
            confidence_score=0.70,  # Fixed confidence — data-backed, not a guess
            rationale=rationale,
        )

    def get_stats(self) -> dict:
        """Return evaluator stats."""
        return {
            "total_evaluated": self._total_evaluated,
            "total_signals": self._total_signals,
            "signal_rate": (
                f"{self._total_signals / max(self._total_evaluated, 1) * 100:.1f}%"
            ),
        }


# =============================================================================
# Standalone test
# =============================================================================

if __name__ == "__main__":
    console.print("[bold]Testing signals/rules.py...[/bold]\n")

    evaluator = RulesEvaluator()

    # Test 1: YES at 70c — should BUY_NO
    console.print("[cyan]1. Game winner, YES at $0.70 (should BUY_NO):[/cyan]")
    ob = OrderbookEntry("KXNBAGAME-26APR04DETPHI-PHI")
    ob.best_bid = Decimal("0.70")
    ob.best_ask = Decimal("0.72")
    d = evaluator.evaluate_market("KXNBAGAME-26APR04DETPHI-PHI", "PHI wins", ob)
    console.print(f"  Action: {d.action} | Edge: {d.edge:.1%} | Kelly: {d.kelly_fraction}")
    assert d.action == "BUY_NO"
    console.print("  [green]PASS[/green]")

    # Test 2: YES at 45c — should PASS
    console.print("\n[cyan]2. Game winner, YES at $0.45 (should PASS):[/cyan]")
    ob2 = OrderbookEntry("KXNBAGAME-26APR04DETPHI-DET")
    ob2.best_bid = Decimal("0.45")
    ob2.best_ask = Decimal("0.47")
    d2 = evaluator.evaluate_market("KXNBAGAME-26APR04DETPHI-DET", "DET wins", ob2)
    console.print(f"  Action: {d2.action} | Reason: {d2.rationale[:60]}")
    assert d2.action == "PASS"
    console.print("  [green]PASS[/green]")

    # Test 3: Non-game-winner — should PASS
    console.print("\n[cyan]3. Player prop (should PASS):[/cyan]")
    ob3 = OrderbookEntry("KXNBAPTS-26APR04DETPHI-PHIMAXEY-25")
    ob3.best_bid = Decimal("0.70")
    ob3.best_ask = Decimal("0.72")
    d3 = evaluator.evaluate_market("KXNBAPTS-26APR04DETPHI-PHIMAXEY-25", "Maxey 25+", ob3)
    console.print(f"  Action: {d3.action} | Reason: {d3.rationale[:60]}")
    assert d3.action == "PASS"
    console.print("  [green]PASS[/green]")

    # Test 4: YES at 96c — too extreme, should PASS
    console.print("\n[cyan]4. Heavy favorite YES $0.96 (should PASS — payout too small):[/cyan]")
    ob4 = OrderbookEntry("KXNBAGAME-26APR04DETPHI-PHI")
    ob4.best_bid = Decimal("0.96")
    ob4.best_ask = Decimal("0.97")
    d4 = evaluator.evaluate_market("KXNBAGAME-26APR04DETPHI-PHI", "PHI wins", ob4)
    console.print(f"  Action: {d4.action} | Reason: {d4.rationale[:60]}")
    assert d4.action == "PASS"
    console.print("  [green]PASS[/green]")

    # Test 5: Game in blowout Q3 — should PASS
    console.print("\n[cyan]5. Blowout in Q3 (should PASS — favorite winning big):[/cyan]")
    ob5 = OrderbookEntry("KXNBAGAME-26APR04DETPHI-PHI")
    ob5.best_bid = Decimal("0.70")
    ob5.best_ask = Decimal("0.72")
    espn = {"status": "In Progress", "quarter": 3, "home_score": 85, "away_score": 60}
    d5 = evaluator.evaluate_market("KXNBAGAME-26APR04DETPHI-PHI", "PHI wins", ob5, espn)
    console.print(f"  Action: {d5.action} | Reason: {d5.rationale[:60]}")
    assert d5.action == "PASS"
    console.print("  [green]PASS[/green]")

    # Stats
    console.print(f"\n{evaluator.get_stats()}")
    console.print("\n[green]signals/rules.py: All tests passed.[/green]")
