"""
Brier score tracker for EdgeRunner.

Tracks prediction accuracy per market category by comparing
the agent's probability estimates against actual outcomes.

Brier score = (predicted_probability - actual_outcome)^2
- Perfect predictor = 0.0
- Random guessing = 0.25
- Always wrong = 1.0

If a category's average Brier > 0.30, the agent is WORSE than
random guessing on that category and should stop trading it.

Inspired by OctagonAI's per-category Brier tracking.
"""

import asyncio
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal

from rich.console import Console

from execution.kalshi_client import KalshiClient
from storage.supabase_client import insert_row, fetch_rows, TABLE_BRIER_SCORES

console = Console()
UTC = timezone.utc

# Flag categories with Brier > this threshold
UNDERPERFORMANCE_THRESHOLD: float = 0.30

# How often to check for resolved markets (seconds)
BRIER_CHECK_INTERVAL: float = 600.0  # 10 minutes


class BrierTracker:
    """
    Tracks prediction accuracy and flags underperforming categories.

    Periodically checks for resolved markets, computes Brier scores
    from logged decisions, and maintains running averages per category.
    """

    def __init__(self, kalshi_client: KalshiClient) -> None:
        self._kalshi = kalshi_client
        self._scores: dict[str, list[float]] = defaultdict(list)
        self._running: bool = False
        self._flagged_categories: set[str] = set()

    def record_score(self, category: str, predicted_prob: float, actual_outcome: int) -> float:
        """
        Record a Brier score for a resolved prediction.

        Args:
            category: Market type (e.g., "PLAYER_PTS", "GAME_WINNER")
            predicted_prob: Agent's probability estimate (0-1)
            actual_outcome: 1 if event happened, 0 if not

        Returns:
            The Brier score for this prediction.
        """
        brier = (predicted_prob - actual_outcome) ** 2
        self._scores[category].append(brier)

        # Check if category is now underperforming
        avg = sum(self._scores[category]) / len(self._scores[category])
        if avg > UNDERPERFORMANCE_THRESHOLD and len(self._scores[category]) >= 5:
            if category not in self._flagged_categories:
                self._flagged_categories.add(category)
                console.print(
                    f"[red]BRIER WARNING: {category} avg score {avg:.3f} > "
                    f"{UNDERPERFORMANCE_THRESHOLD} (worse than random). "
                    f"Consider disabling this category.[/red]"
                )

        return brier

    def is_category_flagged(self, category: str) -> bool:
        """Check if a category has been flagged for underperformance."""
        return category in self._flagged_categories

    def get_stats(self) -> dict[str, dict]:
        """Get Brier score statistics per category."""
        stats = {}
        for category, scores in self._scores.items():
            if scores:
                avg = sum(scores) / len(scores)
                stats[category] = {
                    "count": len(scores),
                    "avg_brier": round(avg, 4),
                    "flagged": category in self._flagged_categories,
                }
        return stats

    async def run(self) -> None:
        """
        Background task: periodically check for resolved markets
        and compute Brier scores from decision log.

        Runs as an asyncio task alongside other agent components.
        """
        self._running = True
        console.print("[blue]Brier Tracker: Started.[/blue]")

        while self._running:
            await asyncio.sleep(BRIER_CHECK_INTERVAL)

            try:
                # Fetch decisions that haven't been scored yet
                decisions = await fetch_rows(
                    "decisions",
                    filters={"accepted": True},
                    limit=50,
                )

                for decision in decisions:
                    ticker = decision.get("ticker", "")
                    if not ticker:
                        continue

                    # Check if market has resolved
                    try:
                        market = await self._kalshi.get_market(ticker)
                        if not market:
                            continue

                        result = market.get("result")
                        if result not in ("yes", "no"):
                            continue  # Not yet resolved

                        # Compute Brier score
                        agent_prob = float(decision.get("agent_prob", 0.5))
                        actual = 1 if result == "yes" else 0
                        category = decision.get("market_type", "OTHER")

                        brier = self.record_score(category, agent_prob, actual)

                        # Log to Supabase
                        await insert_row(TABLE_BRIER_SCORES, {
                            "predicted_probability": agent_prob,
                            "actual_outcome": actual,
                        })

                    except Exception:
                        continue

                    await asyncio.sleep(0.1)

                # Print summary
                stats = self.get_stats()
                if stats:
                    for cat, s in stats.items():
                        flag = " [FLAGGED]" if s["flagged"] else ""
                        console.print(
                            f"[blue]Brier: {cat} = {s['avg_brier']:.3f} "
                            f"({s['count']} scores){flag}[/blue]"
                        )

            except Exception as e:
                console.print(f"[yellow]Brier check error: {e}[/yellow]")

    async def stop(self) -> None:
        """Stop the tracker."""
        self._running = False
        stats = self.get_stats()
        if stats:
            console.print("[bold]Final Brier Scores:[/bold]")
            for cat, s in stats.items():
                console.print(f"  {cat}: {s['avg_brier']:.3f} ({s['count']} scores)")
        console.print("[blue]Brier Tracker: Stopped.[/blue]")
