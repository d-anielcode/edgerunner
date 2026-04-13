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

# UCL: Only 66-70c and 76-85c profitable. 55-65c and 71-75c confirmed losing.
EDGE_TABLE_UCL = {
    (66, 70): (0.400, 0.0),    # 10 bets: 60% NO win, +83% ROI, MCL=3
    (76, 85): (0.641, 0.0),    # 37 bets: 38% NO win, +90% ROI, MCL=5-7
}

# La Liga: Only 81-85c bucket works (YES hits 58.8%). Very selective.
EDGE_TABLE_LALIGA = {
    (81, 90): (0.588, 0.0),    # 17 markets: YES hits 58.8% at 81-85c
}

# WNBA: Recalibrated from per-price analysis (319 markets, 2025 season).
# Sweet spots: 55-62c (65% upset), 71-77c (55% upset), 81-87c (45% upset).
# Danger zones removed: 63-70c (losing), 78-82c (0% NO win), 88c+ (losing).
# Volume filter: skip <100K volume markets (+94% ROI at 100K-500K vs +7% at <100K).
EDGE_TABLE_WNBA = {
    (55, 62): (0.380, 0.0),    # 55-62c: ~62% upset rate, massive edge
    (71, 77): (0.550, 0.0),    # 71-77c: ~45% upset rate, strong payout ratio
    (83, 87): (0.540, 0.0),    # 83-87c: ~46% upset rate, huge payout when it hits
}

# UFC: Only 76-85c is profitable. Below that, edge is too thin after fees.
EDGE_TABLE_UFC = {
    (76, 85): (0.622, 0.0),    # 72 markets: YES hits ~62.2%
}

# NCAA Men's Basketball: Recalibrated — dropped 61-65c (losing, -3% ROI, n=260).
# 66-70c is strong (+42% ROI), 71-80c solid (+57% ROI). 82-90c high ROI but MCL=19.
EDGE_TABLE_NCAAMB = {
    (66, 70): (0.536, 0.0),    # 347 bets: YES hits 53.6%, +42% ROI, MCL=12
    (71, 80): (0.656, 0.0),    # 517 bets: YES hits 65.6%, +57% ROI, MCL=6
    (82, 90): (0.770, 0.0),    # 529 bets: YES hits ~77%, +64% ROI, MCL=19 — keep but small size
}

# NCAA Women's Basketball: Recalibrated — dropped 86-90c (-6% ROI, MCL=33, worst in dataset).
# 61-65c is cleanest (MCL=4, +26% ROI). Keep 81-85c but not 86-90c.
EDGE_TABLE_NCAAWB = {
    (61, 70): (0.600, 0.0),    # 235 bets: +22-26% ROI, MCL=4-12
    (71, 80): (0.680, 0.0),    # 305 bets: +17-21% ROI, MCL=12-18
    (81, 85): (0.750, 0.0),    # 154 bets: +44% ROI, MCL=11 — cap at 85c, skip 86-90c
}

# WTA Tennis: RE-ENABLED with PT. Hold-to-settlement was -10% but 150% PT = +$25, Sharpe 0.183.
# Only trade 76-90c range (optimization grid: best Sharpe at this range).
EDGE_TABLE_WTA = {
    (76, 79): (0.695, 0.0),    # 262 markets: YES hits 69.5% at 76-79c
    (80, 84): (0.803, 0.0),    # 188 markets: YES hits 80.3% at 80-84c
    (85, 90): (0.790, 0.0),    # 105+137 markets: YES hits ~79% at 85-94c
}

# MLB Game Winners: Weak FLB on hold, but profitable with 50% PT at 76-90c.
# Only the 80-84c bucket has meaningful edge (YES hits 57.1% vs 82% implied).
# Small sample (40 markets at 76-90c) — very conservative params.
EDGE_TABLE_MLB = {
    (76, 84): (0.640, 0.0),    # 57 markets: YES hits ~64% at 76-84c (blended)
}

# ATP Tennis: Year-round. Strong FLB at 71-85c. Fills off-season gap.
# Retirement premium: ~2.5% of ATP matches end in retirement, settled as NO win.
# OOS validated from TrevorJS data (2,520 markets).
EDGE_TABLE_ATP = {
    (71, 75): (0.650, 0.0),    # 163 markets: YES hits 65.0% at 71-75c. Edge: 8%
    (76, 80): (0.654, 0.0),    # 127 markets: YES hits 65.4% at 76-80c. Edge: 13%
    (81, 85): (0.765, 0.0),    # 98 markets: YES hits 76.5% at 81-85c. Edge: 6.5%
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
    # "EPL": EDGE_TABLE_EPL,  # DISABLED: 0% WR realistic backtest
    "UCL": EDGE_TABLE_UCL,
    # "LALIGA": EDGE_TABLE_LALIGA,  # DISABLED: 0% WR realistic backtest
    "WNBA": EDGE_TABLE_WNBA,
    # "UFC": EDGE_TABLE_UFC,  # DISABLED: 0% WR realistic backtest
    "NCAAMB": EDGE_TABLE_NCAAMB,
    # "NCAAWB": EDGE_TABLE_NCAAWB,  # DISABLED: -19.7% ROI realistic backtest
    # DISABLED: -10% ROI
    # "WTA": EDGE_TABLE_WTA,
    "MLB": EDGE_TABLE_MLB,
    # New markets from risk-adjusted optimization (all STRONG ADD)
    # DISABLED: No backtest validation or OOS data for these markets.
    # "MLBTOTAL": {(55, 65): 0.500, (66, 75): 0.480, (76, 85): 0.450},  # Over/under runs, 82% WR w/ 100% PT
    # "NFLGW":    {(55, 65): 0.520, (66, 75): 0.580, (76, 90): 0.650},  # DISABLED: negative Sharpe (-0.020) in realistic backtest, 192 trades
    "NFLTT":    {(55, 65): 0.500, (66, 75): 0.480, (76, 85): 0.450},  # NFL team totals, 60% WR w/ 150% PT
    # "CBA":      {(55, 65): 0.500, (66, 75): 0.550, (76, 85): 0.620},  # Chinese basketball, 63% WR w/ 100% PT
    # "LIGUE1":   {(55, 65): 0.480, (66, 75): 0.500, (76, 85): 0.550},  # French soccer, 62% WR w/ 100% PT
    # "LOL":      {(55, 65): 0.500, (66, 75): 0.520, (76, 85): 0.550},  # League of Legends, 67% WR w/ 100% PT
    # "ATPCH":    {(55, 65): 0.520, (66, 75): 0.550, (76, 85): 0.620},  # ATP Challenger tennis, 73% WR w/ 50% PT
    "ATP": EDGE_TABLE_ATP,
    # College Football: Not validated yet (season Sep-Jan). Conservative initial params.
    # Gemini research confirms strong FLB especially at 90c+ favorites.
    "CFB": {(71, 80): 0.650, (81, 90): 0.750},
    "WEATHER": EDGE_TABLE_WEATHER,
    "CPI": EDGE_TABLE_CPI,
    "NFLTD": EDGE_TABLE_NFLTD,
    # New spread/prop markets
    "NHLSPREAD": {(55, 65): 0.500, (66, 75): 0.450, (76, 90): 0.400},
    # Player props — "Buy NO on star player Overs" strategy
    # Edge tables from TrevorJS backtest (pre-game pricing, 2024-2025 + Mar 2026 validation)
    # Values = YES hit rate (lower = more edge for NO buyers)
    "NBA_3PT": {(55, 64): 0.497, (65, 74): 0.594, (75, 84): 0.707, (85, 95): 0.771},  # Sharpe 0.239, 644 trades
    "NBA_PTS": {(55, 64): 0.538, (65, 74): 0.657, (75, 84): 0.736, (85, 95): 0.765},  # Sharpe 0.120, 1348 trades
    "NBA_REB": {(55, 64): 0.574, (65, 74): 0.629, (75, 84): 0.701, (85, 95): 0.864},  # Sharpe 0.132, 860 trades
    "NBA_AST": {(55, 64): 0.582, (65, 74): 0.644, (75, 84): 0.747, (85, 95): 0.827},  # Sharpe 0.126, 642 trades
    "NHLFG":     {(55, 70): 0.550, (71, 90): 0.450},
    "NBASPREAD": {(55, 65): 0.480, (66, 75): 0.440, (76, 90): 0.380},
    "NBA2D":     {(55, 65): 0.520, (66, 79): 0.580},  # Cap at 79c — 80-89c has 0% NO win
    "NFLSPREAD": {(55, 65): 0.480, (66, 75): 0.440, (76, 90): 0.380},
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
    # --- OOS Jan 2026 CONFIRMED (reliable data) — keep original params ---
    "NCAAMB": {"kelly_mult": 0.10, "max_position": 0.05, "min_edge": 0.10},  # Confirmed within 1% — Raised: break-even ~10.5%
    "NFLTD":  {"kelly_mult": 0.20, "max_position": 0.10, "min_edge": 0.10},  # Jan better, off-season now — Raised: break-even ~10.5%
    "WEATHER":{"kelly_mult": 0.25, "max_position": 0.10, "min_edge": 0.10},  # Jan better but Feb-Apr data unreliable, keep original
    "NHLSPREAD": {"kelly_mult": 0.15, "max_position": 0.08, "min_edge": 0.10},  # Jan confirmed, Feb-Apr data unreliable — Raised: break-even ~10.5%
    # --- OOS Jan 2026 DECAYED (defensive cuts) ---
    "NBA":    {"kelly_mult": 0.04, "max_position": 0.03, "min_edge": 0.15},  # Severe decay: 65% YES vs 49% predicted. Nearly disabled.
    "NHL":    {"kelly_mult": 0.15, "max_position": 0.08, "min_edge": 0.12},  # Optimization: 76-90c best range. Raised min_edge to filter weak 61-75c.
    "NBASPREAD": {"kelly_mult": 0.06, "max_position": 0.03, "min_edge": 0.12},  # Decayed in Jan
    "NFLSPREAD": {"kelly_mult": 0.06, "max_position": 0.03, "min_edge": 0.12},  # Decayed in Jan
    # --- NOT VALIDATED (keep original conservative params) ---
    # "EPL":    {"kelly_mult": 0.25, "max_position": 0.10, "min_edge": 0.10},  # DISABLED: 0% WR in realistic backtest (14 trades, -69% ROI)
    "UCL":    {"kelly_mult": 0.12, "max_position": 0.06, "min_edge": 0.10},  # Raised: break-even ~10.5%
    # "LALIGA": {"kelly_mult": 0.08, "max_position": 0.04, "min_edge": 0.15},  # DISABLED: 0% WR in realistic backtest (4 trades, -108% ROI)
    "WNBA":   {"kelly_mult": 0.15, "max_position": 0.08, "min_edge": 0.10},  # Raised: break-even ~10.5%
    # "UFC":    {"kelly_mult": 0.12, "max_position": 0.06, "min_edge": 0.10},  # DISABLED: 0% WR in realistic backtest (5 trades, -98% ROI)
    # "NCAAWB": {"kelly_mult": 0.12, "max_position": 0.06, "min_edge": 0.10},  # DISABLED: -19.7% ROI in realistic backtest (18 trades, 16.7% WR)
    # "WTA":    {"kelly_mult": 0.10, "max_position": 0.05, "min_edge": 0.08},  # DISABLED: -10% ROI in research; RE-ENABLED comment was wrong
    "MLB":    {"kelly_mult": 0.06, "max_position": 0.03, "min_edge": 0.12},  # 50% PT at 76-84c. Conservative.
    # --- NEW: Risk-adjusted optimization (all STRONG ADD, Sharpe > 0.2, MaxDD < $4) ---
    # "MLBTOTAL":{"kelly_mult": 0.15, "max_position": 0.08, "min_edge": 0.05},  # DISABLED: No FLB research for over/under totals
    # "NFLGW":   {"kelly_mult": 0.12, "max_position": 0.06, "min_edge": 0.10},  # DISABLED: negative Sharpe (-0.020) in realistic backtest
    "NFLTT":   {"kelly_mult": 0.10, "max_position": 0.05, "min_edge": 0.10},  # NFL team totals, 0.270 Sharpe, $2.9 DD — Raised: break-even ~10.5%
    # "CBA":     {"kelly_mult": 0.12, "max_position": 0.06, "min_edge": 0.08},  # DISABLED: Chinese basketball, no backtest validation
    # "LIGUE1":  {"kelly_mult": 0.10, "max_position": 0.05, "min_edge": 0.08},  # DISABLED: French soccer, no OOS data
    # "LOL":     {"kelly_mult": 0.12, "max_position": 0.06, "min_edge": 0.08},  # DISABLED: LoL esports, no validation
    # "ATPCH":   {"kelly_mult": 0.10, "max_position": 0.05, "min_edge": 0.08},  # DISABLED: ATP Challenger, no validation
    "ATP":    {"kelly_mult": 0.12, "max_position": 0.06, "min_edge": 0.10},  # Year-round, strong FLB + 2.5% retirement premium — Raised: break-even ~10.5%
    "CFB":    {"kelly_mult": 0.08, "max_position": 0.04, "min_edge": 0.12},  # Not OOS validated — very conservative until Sep data
    # --- PLAYER PROPS — Buy NO on star player Overs ---
    # Backtest: all 4 profitable with 0 correlation to game winners (14K+ paired observations)
    # Mar 2026 validation: NO win rates 54-62% (edge stable/strengthening)
    # Using 0.25x fractional Kelly (higher variance than game winners)
    "NBA_3PT": {"kelly_mult": 0.10, "max_position": 0.05, "min_edge": 0.10},  # Best prop: Sharpe 0.239, 26% ROI, 6.3% max DD
    "NBA_PTS": {"kelly_mult": 0.08, "max_position": 0.04, "min_edge": 0.10},  # Highest volume: 1382 trades, 12.6% ROI, 10.3% max DD
    "NBA_REB": {"kelly_mult": 0.08, "max_position": 0.04, "min_edge": 0.10},  # Solid: 922 trades, 11.9% ROI, 12.2% max DD
    "NBA_AST": {"kelly_mult": 0.08, "max_position": 0.04, "min_edge": 0.10},  # Good: 695 trades, 10.9% ROI, 6.5% max DD
    "CPI":    {"kelly_mult": 0.08, "max_position": 0.04, "min_edge": 0.15},
    # --- DROPPED (OOS decayed despite strong backtest) ---
    "NHLFG":     {"kelly_mult": 0.00, "max_position": 0.00, "min_edge": 0.99},  # Disabled
    "NBA2D":     {"kelly_mult": 0.10, "max_position": 0.05, "min_edge": 0.10},
}

# NHL playoff months — favorites win 80% in playoffs, our edge disappears
# NHL regular season: Oct-Apr | Playoffs: Apr 16 - Jun
NHL_PLAYOFF_VETO = True  # Set False to disable
NHL_REGULAR_SEASON_END_MONTH_DAY = (4, 16)  # April 16 approximate

# NBA playoff veto — FLB disappears in playoffs, favorites perform to expectation
# NBA regular season: Oct-Apr | Playoffs: ~Apr 19 - Jun
NBA_PLAYOFF_VETO = True
NBA_PLAYOFF_START_MONTH_DAY = (4, 19)  # April 19 approximate


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
        companion_signal: dict | None = None,
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
            if (now.month > end_month or (now.month == end_month and now.day > end_day)) and now.month < 10:
                return pass_decision(ticker, "NHL playoffs -- favorites win 80%, edge gone.")

        # Rule 1c: NBA playoff veto — FLB disappears in playoffs, edge is zero
        if sport == "NBA" and NBA_PLAYOFF_VETO:
            now = datetime.now(timezone.utc)
            start_month, start_day = NBA_PLAYOFF_START_MONTH_DAY
            if (now.month > start_month or (now.month == start_month and now.day >= start_day)) and now.month < 10:
                return pass_decision(ticker, "NBA playoffs -- FLB disappears, edge gone.")

        # Rule 2: Must have orderbook data
        if orderbook is None or orderbook.best_bid is None:
            return pass_decision(ticker, "No orderbook data.")

        yes_price = orderbook.best_bid
        spread = orderbook.spread

        # Rule 3: YES must be above threshold
        # Many markets have edge starting at 55c (totals, esports, challenger tennis)
        LOW_EDGE_SPORTS = ("WEATHER", "CPI", "NFLTD", "NFLTT", "NBA_3PT", "NBA_PTS", "NBA_REB", "NBA_AST")
        min_price = Decimal("0.55") if sport in LOW_EDGE_SPORTS else MIN_YES_PRICE
        max_price = Decimal("0.90") if sport in LOW_EDGE_SPORTS else MAX_YES_PRICE

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

        # Bayesian posterior first (adaptive), then static fallbacks
        try:
            from data.bayesian_cache import get_yes_rate as bayesian_yes_rate
            bayesian_rate = bayesian_yes_rate(sport, yes_price_cents)
        except Exception:
            bayesian_rate = None

        if bayesian_rate is not None:
            # Bayesian model has enough data (5+ updates) — use adaptive rate
            actual_yes_rate = bayesian_rate
        else:
            # Fall back to static models
            per_price_rate = _per_price_yes_rate(sport, yes_price_cents)
            if per_price_rate is not None:
                actual_yes_rate = per_price_rate
            else:
                edge_table = EDGE_TABLES.get(sport, EDGE_TABLE_NBA)
                actual_yes_rate = 0.65  # default conservative estimate
                for (min_c, max_c), value in edge_table.items():
                    if min_c <= yes_price_cents <= max_c:
                        actual_yes_rate = value[0] if isinstance(value, tuple) else value
                        break

        # === COMPANION SIGNAL: adjust actual_yes_rate, NOT Kelly (Gemini research) ===
        # Modifying probability is mathematically correct; modifying Kelly causes variance drag.
        comp_modifiers = []
        if companion_signal:
            sp = companion_signal.get("spread_price")
            dp = companion_signal.get("draw_price")

            if sport in ("NBA", "NHL") and sp is not None:
                if sp < 40:
                    actual_yes_rate = max(0.30, actual_yes_rate - 0.03)
                    comp_modifiers.append(f"spread_close_{sp}c")
                elif sp > 60:
                    actual_yes_rate = min(0.95, actual_yes_rate + 0.03)
                    comp_modifiers.append(f"spread_blowout_{sp}c")

            if sport in ("EPL", "UCL") and dp is not None:
                if dp >= 25:
                    actual_yes_rate = max(0.30, actual_yes_rate - 0.03)
                    comp_modifiers.append(f"draw_high_{dp}c")
                elif dp < 18:
                    actual_yes_rate = min(0.95, actual_yes_rate + 0.05)
                    comp_modifiers.append(f"draw_low_{dp}c")

        # === VARIANCE-AWARE KELLY: penalize uncertain estimates (DRKP) ===
        # Add 1 sigma to YES rate = more conservative for NO buyers when uncertain
        try:
            from data.bayesian_cache import load_bayesian_state, _bucket_key
            bstate = load_bayesian_state()
            bkey = _bucket_key(sport, yes_price_cents)
            bbucket = bstate.get(bkey)
            if bbucket and bbucket.get("updates", 0) >= 3:
                ba, bb = bbucket["alpha"], bbucket["beta"]
                btotal = ba + bb
                posterior_std = (ba * bb / (btotal ** 2 * (btotal + 1))) ** 0.5
                actual_yes_rate = actual_yes_rate + posterior_std
                comp_modifiers.append(f"var_penalty_{posterior_std:.3f}")
        except Exception:
            pass

        # Edge = market implied YES probability - actual YES probability
        # Market says YES is yes_price (e.g., 70%), actual is ~59% -> 11% edge on NO
        market_prob = float(yes_price)
        agent_prob = actual_yes_rate
        edge = market_prob - agent_prob  # Positive = YES is overpriced = NO has edge

        # Sport-specific minimum edge threshold
        params = SPORT_PARAMS.get(sport, SPORT_PARAMS["NBA"])
        min_edge = params["min_edge"]

        # Fee-aware edge: subtract Maker fee impact before threshold comparison
        # Fee formula: 0.07 * P * (1-P) [Taker rate], expressed as % of contract cost
        no_price_f = float(no_price)
        fee_impact = 0.07 * no_price_f * (1 - no_price_f) / no_price_f if no_price_f > 0 else 0
        edge_net = edge - fee_impact

        if edge_net < min_edge:
            return pass_decision(
                ticker,
                f"Net edge {edge_net:.1%} < min {min_edge:.0%} for {sport} "
                f"(gross {edge:.1%}, fee impact {fee_impact:.1%}).",
            )

        # Kelly fraction based on edge and payout, with sport-specific aggression
        # b = (1 - no_price) / no_price = yes_price / no_price
        b = float(yes_price / no_price) if no_price > 0 else 0
        p = 1 - actual_yes_rate  # Probability NO wins
        q = actual_yes_rate  # Probability NO loses

        kelly_raw = (b * p - q) / b if b > 0 else 0
        # 0.50x scaling — backed by Kelly scaling backtest showing best risk/reward
        # (0.33x was too conservative for high-conviction 76-90c trades)
        kelly_mult = params["kelly_mult"] * 0.50
        max_pos = params["max_position"] * 0.50
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

        # Modifier 2: April reduction for NBA/NHL only (playoff edge decay)
        # The original -71% April loss was caused by the Kelly override bug + NBA/NHL
        # playoff edge decay, not a universal April problem. ATP/EPL/La Liga are fine.
        now = datetime.now(timezone.utc)
        if now.month == 4 and sport in ("NBA", "NHL", "NBASPREAD", "NHLSPREAD", "NHLFG"):
            kelly_fraction *= 0.50
            modifiers.append("april_playoff_0.5x")

        # Modifier 3: NBA early R1 playoff reduction (Apr 13-30)
        if sport == "NBA":
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

        # Companion signals now adjust actual_yes_rate BEFORE edge calc (see above)
        # No longer modify Kelly directly — that causes variance drag (Gemini research)
        modifiers.extend(comp_modifiers)

        # === SIMPLIFIED MAB: sport confidence multiplier ===
        try:
            from data.bayesian_cache import get_sport_confidence
            sport_conf = get_sport_confidence(sport)
            if sport_conf != 1.0:
                kelly_fraction *= sport_conf
                modifiers.append(f"mab_{sport_conf:.2f}x")
        except Exception:
            pass

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

    # Test 12: NBA at 92c — should PASS (90c cap for NBA/NHL)
    console.print("\n[cyan]12. NBA at $0.92 (should PASS — 90c cap):[/cyan]")
    ob12 = OrderbookEntry("KXNBAGAME-26APR08BOSMIA-BOS")
    ob12.best_bid = Decimal("0.92")
    ob12.best_ask = Decimal("0.93")
    d12 = evaluator.evaluate_market("KXNBAGAME-26APR08BOSMIA-BOS", "BOS wins", ob12)
    console.print(f"  Action: {d12.action}")
    assert d12.action == "PASS"
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
