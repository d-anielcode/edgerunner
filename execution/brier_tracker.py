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

    def __init__(
        self,
        kalshi_client: KalshiClient,
        risk_gates=None,
        cache=None,
        alerter=None,
    ) -> None:
        self._kalshi = kalshi_client
        self._risk_gates = risk_gates  # For update_after_trade on settlement
        self._cache = cache  # For bankroll sync and position lookup
        self._alerter = alerter  # For edge decay Discord alerts
        self._scores: dict[str, list[float]] = defaultdict(list)
        self._running: bool = False
        self._flagged_categories: set[str] = set()
        self._scored_tickers: set[str] = set()  # Prevent double-scoring (capped at 500)

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
                    if not ticker or ticker in self._scored_tickers:
                        continue

                    # Check if market has resolved
                    try:
                        market = await self._kalshi.get_market(ticker)
                        if not market:
                            continue

                        result = market.get("result")
                        if result not in ("yes", "no"):
                            continue  # Not yet resolved

                        self._scored_tickers.add(ticker)

                        # Compute Brier score
                        agent_prob = float(decision.get("agent_prob", 0.5))
                        market_prob = float(decision.get("market_prob", 0.5))
                        actual = 1 if result == "yes" else 0
                        category = decision.get("market_type", "OTHER")

                        brier = self.record_score(category, agent_prob, actual)

                        # --- Bayesian edge update ---
                        try:
                            from config.markets import get_sport
                            from data.bayesian_cache import update_outcome
                            sport = get_sport(ticker)
                            yes_price_cents = int(market_prob * 100)
                            update_outcome(sport, yes_price_cents, result)
                        except Exception as be:
                            console.print(f"[yellow]Bayesian update error: {be}[/yellow]")

                        # --- P&L tracking + risk gates update ---
                        if self._risk_gates and self._cache:
                            try:
                                positions = self._cache.get_positions()
                                if ticker in positions:
                                    pos = positions[ticker]
                                    # Calculate realized P&L
                                    if pos.side == "no":
                                        pnl = (Decimal("1") - pos.avg_price) * pos.quantity if result == "no" else -pos.avg_price * pos.quantity
                                    else:
                                        pnl = (Decimal("1") - pos.avg_price) * pos.quantity if result == "yes" else -pos.avg_price * pos.quantity
                                    # Sync bankroll from Kalshi (catches deposits too)
                                    balance = await self._kalshi.get_balance()
                                    if balance is not None:
                                        self._cache.set_bankroll(balance)
                                    new_bankroll = self._cache.get_bankroll()
                                    self._risk_gates.update_after_trade(pnl, new_bankroll)
                                    # Remove settled position from cache
                                    self._cache.remove_position(ticker)
                                    win = "WIN" if pnl > 0 else "LOSS"
                                    console.print(
                                        f"[{'green' if pnl > 0 else 'red'}]SETTLED: {ticker[:35]} "
                                        f"{win} ${pnl:+.2f} | Bankroll: ${new_bankroll}[/{'green' if pnl > 0 else 'red'}]"
                                    )
                            except Exception as pe:
                                console.print(f"[yellow]P&L tracking error: {pe}[/yellow]")

                        # Log to Supabase
                        await insert_row(TABLE_BRIER_SCORES, {
                            "predicted_probability": agent_prob,
                            "actual_outcome": actual,
                        })

                    except Exception:
                        continue

                    await asyncio.sleep(0.1)

                # Cap memory: trim scored_tickers if too large
                if len(self._scored_tickers) > 500:
                    self._scored_tickers = set(list(self._scored_tickers)[-200:])

                # --- Passive Bayesian scanner: learn from ALL settled markets ---
                try:
                    await self._scan_all_settlements()
                except Exception as se:
                    console.print(f"[yellow]Bayesian scan error: {se}[/yellow]")

                # --- Send pending edge decay alerts to Discord ---
                try:
                    from data.bayesian_cache import load_bayesian_state, save_bayesian_state
                    bstate = load_bayesian_state()
                    alerts_sent = 0
                    for key, bucket in bstate.items():
                        pending = bucket.pop("_pending_alert", None)
                        if pending:
                            drift = bucket.get("prior_rate", 0.5)
                            current = bucket["alpha"] / (bucket["alpha"] + bucket["beta"])
                            is_decay = current > drift
                            color = 0xE74C3C if is_decay else 0x2ECC71
                            if self._alerter:
                                await self._alerter._send_embed(
                                    title="EDGE DRIFT ALERT",
                                    description=pending,
                                    color=color,
                                )
                            alerts_sent += 1
                    if alerts_sent:
                        save_bayesian_state(bstate)
                except Exception:
                    pass

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

    # Series tickers to passively scan for Bayesian learning
    SCAN_SERIES = [
        "KXNBAGAME", "KXNHLGAME", "KXEPLGAME", "KXUCLGAME", "KXLALIGAGAME",
        "KXWNBAGAME", "KXUFCFIGHT", "KXNCAAMBGAME", "KXNCAAWBGAME",
        "KXATPMATCH", "KXCFBGAME", "KXWTAMATCH", "KXMLBGAME",
        "KXMLBTOTAL", "KXNFLGAME", "KXNFLTEAMTOTAL", "KXCBAGAME",
        "KXLIGUE", "KXLOLMAP", "KXATPCHALLENGERMATCH",
        "KXNFLANYTD", "KXNHLSPREAD", "KXNBASPREAD", "KXNFLSPREAD",
        "KXHIGHNY", "KXHIGHCHI", "KXHIGHMIA", "KXHIGHLA",
    ]

    async def _scan_all_settlements(self) -> None:
        """
        Passively scan ALL recently settled markets (not just traded ones)
        and feed results into the Bayesian cache. This gives 10-50x more
        data points per day for edge table self-correction.
        """
        from config.markets import get_sport
        from data.bayesian_cache import update_outcome

        total_updates = 0
        for series in self.SCAN_SERIES:
            try:
                data = await self._kalshi.get_markets(
                    status="settled", limit=20, series_ticker=series,
                )
                markets = data.get("markets", [])
                for m in markets:
                    ticker = m.get("ticker", "")
                    result = m.get("result", "")
                    if result not in ("yes", "no"):
                        continue
                    if ticker in self._scored_tickers:
                        continue

                    # Get pre-settlement YES price with quality filter
                    # previous_price_dollars is ~24h old (not ideal) but best available
                    # without candlestick endpoint. Filter out settlement-snapped prices.
                    yes_price = None
                    prev = m.get("previous_price_dollars")
                    if prev is not None:
                        try:
                            yp = float(prev)
                            # Quality gate: skip if price is near settlement extremes
                            # (0.01-0.09 or 0.91-0.99 = likely post-settlement snap)
                            if 0.10 <= yp <= 0.90:
                                yes_price = yp
                        except (TypeError, ValueError):
                            pass

                    if yes_price is None:
                        continue  # No reliable price — skip this market

                    yes_cents = int(yes_price * 100)
                    if yes_cents < 55 or yes_cents > 95:
                        continue

                    sport = get_sport(ticker)
                    if sport:
                        update_outcome(sport, yes_cents, result)
                        self._scored_tickers.add(ticker)
                        total_updates += 1

                await asyncio.sleep(0.2)  # Rate limit between series

            except Exception:
                continue

        if total_updates > 0:
            console.print(
                f"[cyan]Bayesian passive scan: {total_updates} new settlements ingested[/cyan]"
            )

    async def stop(self) -> None:
        """Stop the tracker."""
        self._running = False
        stats = self.get_stats()
        if stats:
            console.print("[bold]Final Brier Scores:[/bold]")
            for cat, s in stats.items():
                console.print(f"  {cat}: {s['avg_brier']:.3f} ({s['count']} scores)")
        console.print("[blue]Brier Tracker: Stopped.[/blue]")
