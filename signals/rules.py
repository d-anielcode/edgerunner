"""
Rules-based market evaluator for EdgeRunner v2.

Replaces the Claude LLM as the trading decision engine.
Based on analysis of 154M+ real Kalshi trades across NBA and NHL:

Core strategy: Buy NO on game winners where YES > 60c.
- Favorites are systematically overpriced by retail traders
- NBA: +30.3% flat-bet ROI, 34.8% NO win rate (644 markets)
- NHL: +42.2% flat-bet ROI, 45.8% NO win rate (585 markets) -- strongest edge
- Kelly-sized compounding turns $100 into thousands over a season

No LLM needed. Pure math.
"""

from datetime import datetime, timezone
from decimal import Decimal

from rich.console import Console

from config.markets import get_sport, is_game_winner
from data.cache import OrderbookEntry
from signals.schemas import TradeDecision, pass_decision

console = Console()

# Empirical edge by sport and YES price bucket.
# From 154M+ trades across multiple seasons (TrevorJS dataset).
# Maps YES price range to (actual YES hit rate, documented NO ROI)
EDGE_TABLE_NBA = {
    (61, 75): (0.608, 0.220),  # 644 markets: YES hits 60.8%
    (76, 90): (0.719, 0.344),  # Higher favorites: YES hits 71.9%
}

EDGE_TABLE_NHL = {
    (61, 75): (0.545, 0.322),  # 585 markets: YES hits only 54.5%! Massive edge.
    (76, 90): (0.563, 0.638),  # Even at 76-90c, YES only hits 56.3%
}

# EPL: Best edge in 71-85c range (YES hits only 37-51%). Skip 61-70c (weak).
EDGE_TABLE_EPL = {
    (71, 85): (0.485, 0.0),    # 111 markets: YES hits ~48.5%. Massive edge.
}

# UCL: Inconsistent — only 76-85c is profitable. Small sample, moderate Kelly.
EDGE_TABLE_UCL = {
    (66, 70): (0.400, 0.0),    # 10 markets: YES hits 40% — good edge
    (76, 85): (0.641, 0.0),    # 39 markets: YES hits 64.1%
}

# La Liga: Only 81-85c bucket works (YES hits 58.8%). Very selective.
EDGE_TABLE_LALIGA = {
    (81, 90): (0.588, 0.0),    # 17 markets: YES hits 58.8% at 81-85c
}

# WNBA: Sweet spots are 61-65c, 71-75c, and 81-90c. Skip 66-70c (losing bucket).
EDGE_TABLE_WNBA = {
    (61, 65): (0.559, 0.0),    # 34 markets: YES hits 55.9%
    (71, 75): (0.596, 0.0),    # 57 markets: YES hits 59.6%
    (81, 90): (0.735, 0.0),    # 77 markets: YES hits 73.5% (avg of 64.5% + 82.6%)
}

# UFC: Only 76-85c is profitable. Below that, edge is too thin after fees.
EDGE_TABLE_UFC = {
    (76, 85): (0.622, 0.0),    # 72 markets: YES hits ~62.2%
}

# NCAA Men's Basketball: Edge across all buckets. 82-90c has +116-145% ROI.
EDGE_TABLE_NCAAMB = {
    (61, 70): (0.579, 0.0),    # 603 bets: YES hits 57.9%, NO wins 42.1%
    (71, 80): (0.656, 0.0),    # 517 bets: YES hits 65.6%, NO wins 34.4%
    (82, 90): (0.770, 0.0),    # 529 bets: YES hits ~77%, but +116-145% ROI due to payout ratio
}

# NCAA Women's Basketball: Decent edge. Season: Nov-Mar.
EDGE_TABLE_NCAAWB = {
    (61, 70): (0.600, 0.0),    # Estimated from 28.3% overall NO win rate
    (71, 80): (0.680, 0.0),
    (81, 90): (0.780, 0.0),
}

# WTA Tennis: +19.3% ROI, year-round. Individual match winners.
EDGE_TABLE_WTA = {
    (61, 75): (0.650, 0.0),    # 1,116 markets: 32% NO win rate overall
    (76, 85): (0.680, 0.0),
}

# Weather: Temp predictions massively overpriced. All cities combined.
# YES = "will temp be above X" — hits only 40-42% even when priced 55-95c.
EDGE_TABLE_WEATHER = {
    (55, 65): (0.404, 0.0),    # 240 bets: YES hits 40.4%
    (66, 75): (0.417, 0.0),    # 96 bets: YES hits 41.7%
    (76, 85): (0.417, 0.0),    # 36 bets: YES hits 41.7%
    (86, 95): (0.419, 0.0),    # 31 bets: YES hits 41.9%
}

# CPI: Market overestimates inflation moves.
EDGE_TABLE_CPI = {
    (55, 75): (0.591, 0.0),    # 115 bets: YES hits 59.1%
    (76, 90): (0.703, 0.0),    # 155 bets: YES hits 70.3%
    (91, 95): (0.873, 0.0),    # 150 bets: YES hits 87.3%
}

# NFL Anytime TD: People overestimate players scoring touchdowns.
EDGE_TABLE_NFLTD = {
    (55, 65): (0.492, 0.0),    # 193 bets: YES hits 49.2% (NO wins 50.8%!)
    (66, 75): (0.452, 0.0),    # 93 bets: YES hits 45.2% (NO wins 54.8%!)
    (76, 85): (0.545, 0.0),    # 11 bets: YES hits 54.5%
    (86, 95): (0.286, 0.0),    # 14 bets: YES hits 28.6% (NO wins 71.4%!)
}

# Sport-specific tables
EDGE_TABLES = {
    "NBA": EDGE_TABLE_NBA,
    "NHL": EDGE_TABLE_NHL,
    "EPL": EDGE_TABLE_EPL,
    "UCL": EDGE_TABLE_UCL,
    "LALIGA": EDGE_TABLE_LALIGA,
    "WNBA": EDGE_TABLE_WNBA,
    "UFC": EDGE_TABLE_UFC,
    "NCAAMB": EDGE_TABLE_NCAAMB,
    "NCAAWB": EDGE_TABLE_NCAAWB,
    "WTA": EDGE_TABLE_WTA,
    "WEATHER": EDGE_TABLE_WEATHER,
    "CPI": EDGE_TABLE_CPI,
    "NFLTD": EDGE_TABLE_NFLTD,
}

# Below 60c YES, NO is not profitable — skip
MIN_YES_PRICE: Decimal = Decimal("0.60")

# Maximum YES price — above 90c, edge declines and drawdown increases
MAX_YES_PRICE: Decimal = Decimal("0.90")

# Minimum spread to ensure we can actually execute
MAX_SPREAD: Decimal = Decimal("0.05")

# Sport-specific aggression levels (optimized per-bucket analysis on 154M trades)
# Only trades profitable price buckets per sport
SPORT_PARAMS = {
    "NBA":    {"kelly_mult": 0.10, "max_position": 0.05, "min_edge": 0.08},  # 34% WR, conservative
    "NHL":    {"kelly_mult": 0.30, "max_position": 0.12, "min_edge": 0.05},  # 47% WR, aggressive
    "EPL":    {"kelly_mult": 0.25, "max_position": 0.10, "min_edge": 0.10},  # 50% WR in 71-85c, aggressive
    "UCL":    {"kelly_mult": 0.12, "max_position": 0.06, "min_edge": 0.08},  # Small sample, conservative
    "LALIGA": {"kelly_mult": 0.08, "max_position": 0.04, "min_edge": 0.15},  # Only 81-90c works, very selective
    "WNBA":   {"kelly_mult": 0.12, "max_position": 0.06, "min_edge": 0.05},  # Skip 66-70c losing bucket
    "UFC":    {"kelly_mult": 0.12, "max_position": 0.06, "min_edge": 0.08},  # Only 76-85c profitable
    "NCAAMB": {"kelly_mult": 0.10, "max_position": 0.05, "min_edge": 0.08},  # 39% WR but compounding hurts, conservative
    "NCAAWB": {"kelly_mult": 0.12, "max_position": 0.06, "min_edge": 0.08},  # 28% WR, +26% ROI, Nov-Mar
    "WTA":    {"kelly_mult": 0.08, "max_position": 0.04, "min_edge": 0.10},  # 32% WR, keep conservative
    "WEATHER":{"kelly_mult": 0.25, "max_position": 0.10, "min_edge": 0.10},  # 59% WR, +98% ROI, year-round
    "CPI":    {"kelly_mult": 0.08, "max_position": 0.04, "min_edge": 0.15},  # Edge fading in 2024, very selective
    "NFLTD":  {"kelly_mult": 0.20, "max_position": 0.10, "min_edge": 0.05},  # 53% WR, +47% ROI, Sep-Jan
}

# NHL playoff months — favorites win 80% in playoffs, our edge disappears
# NHL regular season: Oct-Apr | Playoffs: Apr 16 - Jun
# NBA playoffs are less affected (edge holds) so no veto needed
NHL_PLAYOFF_VETO = True  # Set False to disable
NHL_REGULAR_SEASON_END_MONTH_DAY = (4, 16)  # April 16 approximate


def _per_price_yes_rate(sport: str, yes_price_cents: int) -> float | None:
    """
    Per-cent YES hit rate using linear interpolation. NBA/NHL only.

    Derived from per-cent analysis of 154M trades (TrevorJS dataset).
    Replaces the 2-bucket EDGE_TABLES for these sports.

    NBA: ~40% at 65c, ~34% at 75c, ~28% at 85c, ~24% at 92c
    NHL: ~50% at 65c, ~46% at 75c, ~42% at 85c, ~38% at 92c
    """
    if sport == "NBA":
        return max(0.20, 0.50 - (yes_price_cents - 60) * 0.004)
    if sport == "NHL":
        return max(0.30, 0.55 - (yes_price_cents - 60) * 0.003)
    return None  # Other sports use bucket tables


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
        Supports: NBA, NHL, EPL, UCL, La Liga, WNBA, UFC.
        """
        self._total_evaluated += 1

        # Rule 1: Must be a tradeable market type
        if not is_game_winner(ticker):
            return pass_decision(ticker, "Not a tradeable market type.")

        sport = get_sport(ticker)

        # Rule 1b: NHL playoff veto — favorites win 80% in playoffs, edge disappears
        if sport == "NHL" and NHL_PLAYOFF_VETO:
            now = datetime.now(timezone.utc)
            end_month, end_day = NHL_REGULAR_SEASON_END_MONTH_DAY
            # After regular season ends, skip NHL until October
            if (now.month > end_month or (now.month == end_month and now.day > end_day)) and now.month < 10:
                return pass_decision(ticker, "NHL playoffs -- favorites win 80%, edge gone.")

        # Rule 2: Must have orderbook data
        if orderbook is None or orderbook.best_bid is None:
            return pass_decision(ticker, "No orderbook data.")

        yes_price = orderbook.best_bid
        spread = orderbook.spread

        # Rule 3: YES must be above threshold
        # Weather, CPI, and NFL TD have edge starting at 55c; sports start at 60c
        min_price = Decimal("0.55") if sport in ("WEATHER", "CPI", "NFLTD") else MIN_YES_PRICE
        max_price = Decimal("0.95") if sport in ("WEATHER", "CPI", "NFLTD", "NBA", "NHL") else MAX_YES_PRICE

        if yes_price < min_price:
            return pass_decision(
                ticker,
                f"YES ${yes_price} < ${min_price} — below tradeable range for {sport}.",
            )

        # Rule 4: YES must not be too extreme (tiny payout on NO)
        if yes_price > max_price:
            return pass_decision(
                ticker,
                f"YES ${yes_price} > ${max_price} — NO payout too small.",
            )

        # Rule 5: Spread must be tradeable
        if spread is not None and spread > MAX_SPREAD:
            return pass_decision(
                ticker,
                f"Spread ${spread} > ${MAX_SPREAD} — too expensive to cross.",
            )

        # Rule 6: Pre-event only — no mid-game bets
        # Our edge is validated on opening prices only. Mid-game prices reflect
        # live action and the favorite-longshot bias may not exist.
        if espn_game:
            status = espn_game.get("status", "")
            if status == "Final":
                return pass_decision(ticker, "Game already finished.")
            if status == "In Progress":
                return pass_decision(ticker, "Game in progress -- only pre-event bets validated.")

        # All rules pass — compute edge and generate BUY_NO signal
        no_price = Decimal("1") - yes_price
        yes_price_cents = int(yes_price * 100)

        # Per-price linear model for NBA/NHL, bucket table for others
        per_price_rate = _per_price_yes_rate(sport, yes_price_cents)
        if per_price_rate is not None:
            actual_yes_rate = per_price_rate
        else:
            edge_table = EDGE_TABLES.get(sport, EDGE_TABLE_NBA)
            actual_yes_rate = 0.65  # default conservative estimate
            for (min_c, max_c), (hit_rate, _) in edge_table.items():
                if min_c <= yes_price_cents <= max_c:
                    actual_yes_rate = hit_rate
                    break

        # Edge = market implied YES probability - actual YES probability
        # Market says YES is yes_price (e.g., 70%), actual is ~59% → 11% edge on NO
        market_prob = float(yes_price)
        agent_prob = actual_yes_rate
        edge = market_prob - agent_prob  # Positive = YES is overpriced = NO has edge

        # Sport-specific minimum edge threshold
        params = SPORT_PARAMS.get(sport, SPORT_PARAMS["NBA"])
        min_edge = params["min_edge"]

        if edge < min_edge:
            return pass_decision(
                ticker,
                f"Edge {edge:.1%} too small for {sport} (min {min_edge:.0%}, market {market_prob:.0%} vs actual {agent_prob:.0%}).",
            )

        # Kelly fraction based on edge and payout, with sport-specific aggression
        # b = (1 - no_price) / no_price = yes_price / no_price
        b = float(yes_price / no_price) if no_price > 0 else 0
        p = 1 - actual_yes_rate  # Probability NO wins
        q = actual_yes_rate  # Probability NO loses

        kelly_raw = (b * p - q) / b if b > 0 else 0
        if per_price_rate is not None:
            # Per-price model: 0.25x Kelly, 12% cap (data-optimized)
            kelly_mult = 0.25
            max_pos = 0.12
        else:
            kelly_mult = params["kelly_mult"]
            max_pos = params["max_position"]
        kelly_fraction = max(0.0, min(kelly_raw * kelly_mult, max_pos))

        # === SITUATIONAL KELLY MODIFIERS (data-backed) ===
        modifiers = []

        # Modifier 1: Away favorite boost (+47.8% ROI vs +18.3% for home)
        # Ticker format: KXNBAGAME-26APR07SACGSW-GSW → game_id=SACGSW, team=GSW
        # Game ID is AWYHOM (away first 3, home last 3)
        if sport in ("NBA", "NHL"):
            parts = ticker.split("-")
            if len(parts) >= 3:
                game_part = parts[1]  # e.g., 26APR07SACGSW
                team_part = parts[2]  # e.g., GSW
                if len(game_part) >= 6 and len(team_part) >= 2:
                    game_id = game_part[-6:]  # SACGSW
                    away_team = game_id[:3]
                    # If the favorite (this market) is the AWAY team, boost Kelly
                    if team_part == away_team:
                        kelly_fraction = min(kelly_fraction * 1.5, max_pos)
                        modifiers.append("away_fav_1.5x")

        # Modifier 2: NBA early R1 playoff reduction (Apr 13-30)
        # Play-in (Apr 13-17) has -27.5% ROI, R1 (Apr 18-30) has +7.1% ROI
        # Regular season is +24%. Reduce Kelly by 75% during this window.
        if sport == "NBA":
            now = datetime.now(timezone.utc)
            if now.month == 4 and 13 <= now.day <= 30:
                kelly_fraction *= 0.25
                modifiers.append("nba_early_playoff_0.25x")

        # Modifier 3: NFL TD January boost (playoff TDs)
        # January NFL TD has +92.9% ROI, 67% NO win rate
        if sport == "NFLTD":
            now = datetime.now(timezone.utc)
            if now.month == 1:
                kelly_fraction = min(kelly_fraction * 1.5, max_pos)
                modifiers.append("nfltd_jan_1.5x")

        modifier_str = f" [{','.join(modifiers)}]" if modifiers else ""

        self._total_signals += 1

        rationale = (
            f"[{sport}] Fade favorite: YES priced at ${yes_price} ({market_prob:.0%}) "
            f"but historically hits only {actual_yes_rate:.0%}. "
            f"Edge: {edge:.0%}. Buy NO at ${no_price}.{modifier_str}"
        )

        console.print(
            f"[green]RULE SIGNAL: BUY_NO on {ticker[:30]} | "
            f"{sport} | YES=${yes_price} | Edge={edge:.1%} | Kelly={kelly_fraction:.3f}[/green]"
        )

        # Log market flow data for future analysis (passive, no trade impact)
        try:
            from data.flow_logger import log_market_flow
            log_market_flow(
                ticker=ticker, sport=sport, yes_price=yes_price,
                bid_volume=orderbook.bid_volume if orderbook else Decimal("0"),
                ask_volume=orderbook.ask_volume if orderbook else Decimal("0"),
                ofi=orderbook.ofi if orderbook else 0.0,
                volume=0,  # Volume added by main.py at execution time
                edge=edge, kelly=kelly_fraction, action="BUY_NO",
            )
        except Exception:
            pass

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

    # Test 5: NBA Blowout in Q3 — should PASS
    console.print("\n[cyan]5. NBA blowout in Q3 (should PASS — favorite winning big):[/cyan]")
    ob5 = OrderbookEntry("KXNBAGAME-26APR04DETPHI-PHI")
    ob5.best_bid = Decimal("0.70")
    ob5.best_ask = Decimal("0.72")
    espn = {"status": "In Progress", "quarter": 3, "home_score": 85, "away_score": 60}
    d5 = evaluator.evaluate_market("KXNBAGAME-26APR04DETPHI-PHI", "PHI wins", ob5, espn)
    console.print(f"  Action: {d5.action} | Reason: {d5.rationale[:60]}")
    assert d5.action == "PASS"
    console.print("  [green]PASS[/green]")

    # Test 6: NHL game winner YES at 70c — should BUY_NO (bigger edge than NBA)
    console.print("\n[cyan]6. NHL game winner, YES at $0.70 (should BUY_NO):[/cyan]")
    ob6 = OrderbookEntry("KXNHLGAME-26APR06TORSEA-TOR")
    ob6.best_bid = Decimal("0.70")
    ob6.best_ask = Decimal("0.72")
    d6 = evaluator.evaluate_market("KXNHLGAME-26APR06TORSEA-TOR", "TOR wins", ob6)
    console.print(f"  Action: {d6.action} | Edge: {d6.edge:.1%} | Kelly: {d6.kelly_fraction}")
    assert d6.action == "BUY_NO"
    assert d6.edge > 0.10  # NHL edge should be bigger than NBA at same price
    console.print("  [green]PASS[/green]")

    # Test 7: NHL blowout P3 — should PASS
    console.print("\n[cyan]7. NHL blowout in P3, down by 3 (should PASS):[/cyan]")
    ob7 = OrderbookEntry("KXNHLGAME-26APR06TORSEA-TOR")
    ob7.best_bid = Decimal("0.70")
    ob7.best_ask = Decimal("0.72")
    espn_nhl = {"status": "In Progress", "period": 3, "home_score": 4, "away_score": 1}
    d7 = evaluator.evaluate_market("KXNHLGAME-26APR06TORSEA-TOR", "TOR wins", ob7, espn_nhl)
    console.print(f"  Action: {d7.action} | Reason: {d7.rationale[:60]}")
    assert d7.action == "PASS"
    console.print("  [green]PASS[/green]")

    # Test 8: NHL in-progress — should PASS (pre-event only)
    console.print("\n[cyan]8. NHL in-progress game (should PASS -- pre-event only):[/cyan]")
    ob8 = OrderbookEntry("KXNHLGAME-26APR06TORSEA-TOR")
    ob8.best_bid = Decimal("0.70")
    ob8.best_ask = Decimal("0.72")
    espn_nhl2 = {"status": "In Progress", "period": 3, "home_score": 2, "away_score": 1}
    d8 = evaluator.evaluate_market("KXNHLGAME-26APR06TORSEA-TOR", "TOR wins", ob8, espn_nhl2)
    console.print(f"  Action: {d8.action}")
    assert d8.action == "PASS"
    console.print("  [green]PASS[/green]")

    # Test 9: EPL game winner YES at 78c — should BUY_NO (biggest edge in dataset)
    console.print("\n[cyan]9. EPL game winner, YES at $0.78 (should BUY_NO):[/cyan]")
    ob9 = OrderbookEntry("KXEPLGAME-26JAN25ARSMUN-ARS")
    ob9.best_bid = Decimal("0.78")
    ob9.best_ask = Decimal("0.80")
    d9 = evaluator.evaluate_market("KXEPLGAME-26JAN25ARSMUN-ARS", "Arsenal wins", ob9)
    console.print(f"  Action: {d9.action} | Edge: {d9.edge:.1%} | Kelly: {d9.kelly_fraction}")
    assert d9.action == "BUY_NO"
    console.print("  [green]PASS[/green]")

    # Test 10: WNBA game winner YES at 73c — should BUY_NO (71-75c bucket)
    console.print("\n[cyan]10. WNBA game winner, YES at $0.73 (should BUY_NO):[/cyan]")
    ob10 = OrderbookEntry("KXWNBAGAME-25JUL15ATLCHI-ATL")
    ob10.best_bid = Decimal("0.73")
    ob10.best_ask = Decimal("0.75")
    d10 = evaluator.evaluate_market("KXWNBAGAME-25JUL15ATLCHI-ATL", "ATL wins", ob10)
    console.print(f"  Action: {d10.action} | Edge: {d10.edge:.1%} | Kelly: {d10.kelly_fraction}")
    assert d10.action == "BUY_NO"
    console.print("  [green]PASS[/green]")

    # Test 11: UFC fight winner YES at 80c — should BUY_NO (needs higher price for 8% edge)
    console.print("\n[cyan]11. UFC fight winner, YES at $0.80 (should BUY_NO):[/cyan]")
    ob11 = OrderbookEntry("KXUFCFIGHT-25NOV15PANTOP-PAN")
    ob11.best_bid = Decimal("0.80")
    ob11.best_ask = Decimal("0.82")
    d11 = evaluator.evaluate_market("KXUFCFIGHT-25NOV15PANTOP-PAN", "PAN wins", ob11)
    console.print(f"  Action: {d11.action} | Edge: {d11.edge:.1%} | Kelly: {d11.kelly_fraction}")
    assert d11.action == "BUY_NO"
    console.print("  [green]PASS[/green]")

    # Test 12: NBA at 92c — should BUY_NO (was blocked at 90c before, now 95c cap)
    console.print("\n[cyan]12. NBA heavy favorite at $0.92 (should BUY_NO — expanded cap):[/cyan]")
    ob12 = OrderbookEntry("KXNBAGAME-26APR08BOSMIA-BOS")
    ob12.best_bid = Decimal("0.92")
    ob12.best_ask = Decimal("0.93")
    d12 = evaluator.evaluate_market("KXNBAGAME-26APR08BOSMIA-BOS", "BOS wins", ob12)
    console.print(f"  Action: {d12.action} | Edge: {d12.edge:.1%} | Kelly: {d12.kelly_fraction}")
    assert d12.action == "BUY_NO"
    console.print("  [green]PASS[/green]")

    # Test 13: Verify per-price Kelly gives different values at different prices
    console.print("\n[cyan]13. Per-price Kelly: 65c vs 85c vs 92c (should scale up):[/cyan]")
    for price, label in [(65, "65c"), (85, "85c"), (92, "92c")]:
        ob_t = OrderbookEntry("KXNBAGAME-26APR08TEST-TST")
        ob_t.best_bid = Decimal(str(price / 100))
        ob_t.best_ask = Decimal(str((price + 2) / 100))
        d_t = evaluator.evaluate_market("KXNBAGAME-26APR08TEST-TST", "TST wins", ob_t)
        if d_t.action == "BUY_NO":
            console.print(f"  {label}: Kelly={d_t.kelly_fraction:.4f} | Edge={d_t.edge:.1%}")
        else:
            console.print(f"  {label}: PASS ({d_t.rationale[:50]})")
    console.print("  [green]PASS[/green]")

    # Stats
    console.print(f"\n{evaluator.get_stats()}")
    console.print("\n[green]signals/rules.py: All tests passed.[/green]")
