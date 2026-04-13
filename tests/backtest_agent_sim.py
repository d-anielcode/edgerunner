"""
REALISTIC AGENT SIMULATION BACKTEST — EdgeRunner

Runs the current live agent logic against TrevorJS historical data with
two pricing modes side-by-side:
  1. First-trade price  (legacy comparison — overstates edge)
  2. Pre-game price     (last trade 2h+ before close — realistic)

Adds execution realism: spread penalty, fee-aware edge, Kelly sizing with
drawdown circuit breakers, BMA Bayesian updating, CUSUM regime detection,
position limits, profit-take, April debuff, and playoff vetoes.

Usage:  .venv/Scripts/python tests/backtest_agent_sim.py
"""

import math
import re
import time
from collections import defaultdict
from copy import deepcopy


def _isna(val) -> bool:
    """Check if a value is NaN/NAType/None (handles DuckDB NAType)."""
    if val is None:
        return True
    try:
        import pandas as pd
        return pd.isna(val)
    except Exception:
        try:
            return math.isnan(float(val))
        except (TypeError, ValueError):
            return False

import duckdb

t0 = time.time()

# ═══════════════════════════════════════════════════════════════════════
# CONFIG — current live agent values (post-fixes)
# ═══════════════════════════════════════════════════════════════════════

STARTING_BANKROLL = 100.0
MAX_BET = 200.0
SPREAD_THIN = 0.33
MAX_POSITIONS = 25
MAX_PER_GAME = 2
APRIL_DEBUFF = 0.5        # 0.5x Kelly for NBA/NHL in April
SPREAD_PENALTY = 1.5      # cents — half-spread taker cost for pre-game mode

# Drawdown circuit breakers (NAV per-share basis)
DD_REDUCE_15 = 0.50       # 0.5x Kelly at 15% DD
DD_REDUCE_25 = 0.25       # 0.25x Kelly at 25% DD
DD_HALT_40 = True          # halt trading at 40% DD

# BMA Bayesian parameters
BMA_SLOW_DECAY = 0.998
BMA_FAST_DECAY = 0.970
BMA_FORGET = 0.98          # forgetting factor for BMA weight update
BMA_KAPPA_CAP = 20.0       # prior cap
BMA_MIN_UPDATES = 5        # need this many before using BMA rate

# CUSUM parameters
CUSUM_DELTA = 0.05         # shift magnitude p1 = p0 + delta
CUSUM_H = 3.0              # alarm threshold
CUSUM_KELLY_MULT = 0.25    # Kelly multiplier when alarmed

SPORT_PARAMS = {
    # Game winners + spreads (current live agent)
    "NBA":       {"km": 0.04, "mp": 0.03, "me": 0.15},
    "NHL":       {"km": 0.15, "mp": 0.08, "me": 0.12},
    "UCL":       {"km": 0.12, "mp": 0.06, "me": 0.10},
    "WNBA":      {"km": 0.15, "mp": 0.08, "me": 0.10},
    "ATP":       {"km": 0.12, "mp": 0.06, "me": 0.10},
    "NFLTD":     {"km": 0.20, "mp": 0.10, "me": 0.10},
    "NHLSPREAD": {"km": 0.15, "mp": 0.08, "me": 0.10},
    "NBASPREAD": {"km": 0.06, "mp": 0.03, "me": 0.12},
    "NFLSPREAD": {"km": 0.06, "mp": 0.03, "me": 0.12},
    "MLB":       {"km": 0.06, "mp": 0.03, "me": 0.12},
    "NFLTT":     {"km": 0.10, "mp": 0.05, "me": 0.10},
    "CFB":       {"km": 0.08, "mp": 0.04, "me": 0.12},
    # Player props — Buy NO on star player Overs
    "NBA_3PT":   {"km": 0.10, "mp": 0.05, "me": 0.10},
    "NBA_PTS":   {"km": 0.08, "mp": 0.04, "me": 0.10},
    "NBA_REB":   {"km": 0.08, "mp": 0.04, "me": 0.10},
    "NBA_AST":   {"km": 0.08, "mp": 0.04, "me": 0.10},
    # DISABLED: EPL, LALIGA, UFC, NCAAMB, NCAAWB, NFLGW
}

EDGE_TABLES = {
    "UCL":      {(66, 70): 0.400, (76, 85): 0.641},
    "WNBA":     {(55, 62): 0.380, (71, 77): 0.550, (83, 87): 0.540},
    "ATP":      {(71, 75): 0.650, (76, 80): 0.654, (81, 85): 0.765},
    "NFLTD":    {(55, 65): 0.492, (66, 75): 0.452, (76, 85): 0.545, (86, 95): 0.286},
    "NHLSPREAD": {(55, 65): 0.500, (66, 75): 0.450, (76, 90): 0.400},
    "NBASPREAD": {(55, 65): 0.480, (66, 75): 0.440, (76, 90): 0.380},
    "NFLSPREAD": {(55, 65): 0.480, (66, 75): 0.440, (76, 90): 0.380},
    "MLB":      {(76, 84): 0.640},
    "NFLTT":    {(55, 65): 0.500, (66, 75): 0.480, (76, 85): 0.450},
    "CFB":      {(55, 65): 0.520, (66, 75): 0.550, (76, 85): 0.620},
    # Player props
    "NBA_3PT":  {(55, 64): 0.497, (65, 74): 0.594, (75, 84): 0.707, (85, 95): 0.771},
    "NBA_PTS":  {(55, 64): 0.538, (65, 74): 0.657, (75, 84): 0.736, (85, 95): 0.765},
    "NBA_REB":  {(55, 64): 0.574, (65, 74): 0.629, (75, 84): 0.701, (85, 95): 0.864},
    "NBA_AST":  {(55, 64): 0.582, (65, 74): 0.644, (75, 84): 0.747, (85, 95): 0.827},
}

SPORT_PT = {
    "NBA": 150, "NBASPREAD": 150,
    "NFLSPREAD": 200, "NFLTD": 100, "NHL": 100, "NHLSPREAD": 300,
    "UCL": 100, "WNBA": 100, "ATP": 100,
    "CFB": 200, "MLB": 50, "NFLTT": 150,
    "NBA_3PT": 200, "NBA_PTS": 150, "NBA_REB": 150, "NBA_AST": 150,
}

SPORT_FROM_PREFIX = {
    "KXNBAGAME": "NBA", "KXNHLGAME": "NHL",
    "KXUCLGAME": "UCL", "KXWNBAGAME": "WNBA",
    "KXATPMATCH": "ATP", "KXNFLANYTD": "NFLTD",
    "KXNHLSPREAD": "NHLSPREAD", "KXNBASPREAD": "NBASPREAD",
    "KXNFLSPREAD": "NFLSPREAD", "KXMLBGAME": "MLB",
    "KXNFLTEAMTOTAL": "NFLTT", "KXCFBGAME": "CFB",
    # Player props
    "KXNBAPTS": "NBA_PTS", "KXNBA3PT": "NBA_3PT",
    "KXNBAREB": "NBA_REB", "KXNBAAST": "NBA_AST",
}

# Game ID regex — extract the matchup portion from ticker
GAME_ID_RE = re.compile(r"(KX\w+-\d{2}[A-Z]{3}\d{2}[A-Z]{3,8})-")


# ═══════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════

def per_price_yes_rate(sport, yp):
    """Linear model for NBA/NHL — returns expected YES rate at given price."""
    if sport == "NBA":
        return max(0.20, 0.50 - (yp - 60) * 0.004)
    if sport == "NHL":
        return max(0.30, 0.55 - (yp - 60) * 0.003)
    return None


def get_static_yes_rate(sport, yp):
    """Look up expected YES rate from edge tables or per-price model."""
    pp = per_price_yes_rate(sport, yp)
    if pp is not None:
        return pp
    et = EDGE_TABLES.get(sport, {})
    for (lo, hi), value in et.items():
        if lo <= yp <= hi:
            return value
    return None


def kalshi_fee_cents(contracts, price_cents):
    """Fee in dollars: ceil(0.07 * C * P * (1-P)) with cent rounding per contract."""
    p = price_cents / 100.0
    fee_per_contract_cents = math.ceil(0.07 * p * (1 - p) * 100)
    return contracts * fee_per_contract_cents / 100.0


def extract_game_id(ticker):
    """Extract game ID from ticker for position-per-game limits."""
    m = GAME_ID_RE.search(ticker)
    return m.group(1) if m else ticker


def bucket_key(sport, yp):
    """5-cent bucket key for BMA tracking."""
    lo = (yp // 5) * 5
    hi = lo + 4
    return f"{sport}_{lo}_{hi}"


# ═══════════════════════════════════════════════════════════════════════
# BMA BAYESIAN STATE
# ═══════════════════════════════════════════════════════════════════════

class BMAState:
    """Dual Beta filter with BMA weighting per sport+bucket."""

    def __init__(self):
        # Each key maps to: {alpha_s, beta_s, alpha_f, beta_f, w_slow, count}
        self.filters = {}

    def _ensure(self, key, prior_yes_rate):
        """Initialize filter from edge table prior if not present."""
        if key not in self.filters:
            # Initialize with prior — alpha/(alpha+beta) = prior_yes_rate
            # Start with weak prior (alpha+beta ~ 2)
            a = max(0.5, prior_yes_rate * 2)
            b = max(0.5, (1 - prior_yes_rate) * 2)
            self.filters[key] = {
                "alpha_s": a, "beta_s": b,
                "alpha_f": a, "beta_f": b,
                "w_slow": 0.5,
                "count": 0,
            }

    def get_rate(self, key, prior_yes_rate):
        """Get BMA combined YES rate. Falls back to static if < MIN_UPDATES."""
        self._ensure(key, prior_yes_rate)
        f = self.filters[key]
        if f["count"] < BMA_MIN_UPDATES:
            return None  # caller uses static table

        # Slow filter mean
        mean_s = f["alpha_s"] / (f["alpha_s"] + f["beta_s"])
        # Fast filter mean
        mean_f = f["alpha_f"] / (f["alpha_f"] + f["beta_f"])
        # BMA combination
        w = f["w_slow"]
        return w * mean_s + (1 - w) * mean_f

    def update(self, key, prior_yes_rate, yes_outcome):
        """Update both filters after settlement. yes_outcome: True if result='yes'."""
        self._ensure(key, prior_yes_rate)
        f = self.filters[key]
        f["count"] += 1

        y = 1.0 if yes_outcome else 0.0

        # --- Slow filter (decay 0.998) ---
        f["alpha_s"] = BMA_SLOW_DECAY * f["alpha_s"] + y
        f["beta_s"] = BMA_SLOW_DECAY * f["beta_s"] + (1 - y)
        # Cap kappa
        kappa_s = f["alpha_s"] + f["beta_s"]
        if kappa_s > BMA_KAPPA_CAP:
            scale = BMA_KAPPA_CAP / kappa_s
            f["alpha_s"] *= scale
            f["beta_s"] *= scale

        # --- Fast filter (decay 0.970) ---
        f["alpha_f"] = BMA_FAST_DECAY * f["alpha_f"] + y
        f["beta_f"] = BMA_FAST_DECAY * f["beta_f"] + (1 - y)
        kappa_f = f["alpha_f"] + f["beta_f"]
        if kappa_f > BMA_KAPPA_CAP:
            scale = BMA_KAPPA_CAP / kappa_f
            f["alpha_f"] *= scale
            f["beta_f"] *= scale

        # --- BMA weight update ---
        # Likelihood of observation under each model
        mean_s = f["alpha_s"] / (f["alpha_s"] + f["beta_s"])
        mean_f = f["alpha_f"] / (f["alpha_f"] + f["beta_f"])
        lik_s = mean_s if yes_outcome else (1 - mean_s)
        lik_f = mean_f if yes_outcome else (1 - mean_f)
        lik_s = max(lik_s, 1e-6)
        lik_f = max(lik_f, 1e-6)

        # Forgetting factor on prior weights
        w_s_prior = f["w_slow"] ** BMA_FORGET
        w_f_prior = (1 - f["w_slow"]) ** BMA_FORGET
        w_s_post = w_s_prior * lik_s
        w_f_post = w_f_prior * lik_f
        total = w_s_post + w_f_post
        if total > 0:
            f["w_slow"] = w_s_post / total
        else:
            f["w_slow"] = 0.5


# ═══════════════════════════════════════════════════════════════════════
# CUSUM STATE
# ═══════════════════════════════════════════════════════════════════════

class CUSUMState:
    """CUSUM detector per sport — detects negative regime shift."""

    def __init__(self):
        # sport -> {S: cumulative sum, alarmed: bool, alarm_trade: int, alarm_month: str}
        self.state = {}

    def _ensure(self, sport):
        if sport not in self.state:
            self.state[sport] = {
                "S": 0.0, "alarmed": False,
                "alarm_trade": None, "alarm_month": None,
            }

    def update(self, sport, expected_no_rate, actual_no, trade_num, month_str):
        """
        Update CUSUM after a settlement.
        expected_no_rate: our model's P(NO win) = 1 - yes_rate
        actual_no: True if result was 'no' (we won)
        """
        self._ensure(sport)
        s = self.state[sport]
        if s["alarmed"]:
            return  # already alarmed, stays alarmed

        # Under H0: p0 = expected_no_rate
        # Under H1: p1 = p0 - CUSUM_DELTA (worse regime)
        p0 = expected_no_rate
        p1 = p0 - CUSUM_DELTA

        # Log-likelihood ratio for detecting shift toward worse performance
        # We want to detect that actual rate is p1 not p0
        if actual_no:
            # Observation = 1 (win)
            if p1 > 0 and p0 > 0:
                llr = math.log(p1 / p0)
            else:
                llr = 0.0
        else:
            # Observation = 0 (loss)
            if (1 - p1) > 0 and (1 - p0) > 0:
                llr = math.log((1 - p1) / (1 - p0))
            else:
                llr = 0.0

        # CUSUM accumulates evidence for H1 (negative shift)
        # Since p1 < p0, losses give positive LLR, wins give negative
        # We actually want to detect MORE losses than expected
        # Flip sign: positive when things are worse than expected
        s["S"] = max(0.0, s["S"] - llr)

        if s["S"] >= CUSUM_H:
            s["alarmed"] = True
            s["alarm_trade"] = trade_num
            s["alarm_month"] = month_str

    def is_alarmed(self, sport):
        self._ensure(sport)
        return self.state[sport]["alarmed"]


# ═══════════════════════════════════════════════════════════════════════
# DATA LOADING
# ═══════════════════════════════════════════════════════════════════════

print("Loading data from TrevorJS parquet files...")

con = duckdb.connect()
mp = "data/trevorjs/markets-*.parquet"
tp = "data/trevorjs/trades-*.parquet"

# Build SQL CASE statement for sport classification
case_parts = []
for prefix, sport in SPORT_FROM_PREFIX.items():
    case_parts.append(f"WHEN event_ticker LIKE '{prefix}%' THEN '{sport}'")
case_stmt = " ".join(case_parts)
like_clauses = " OR ".join(f"event_ticker LIKE '{p}%'" for p in SPORT_FROM_PREFIX)

print("  Loading markets with first-trade and pre-game prices...")

markets_df = con.sql(f"""
    WITH gm AS (
        SELECT ticker, result, event_ticker, volume, close_time,
               CASE {case_stmt} END as sport
        FROM '{mp}'
        WHERE ({like_clauses}) AND status='finalized' AND result IN ('yes','no')
    ),
    first_trades AS (
        SELECT t.ticker, t.yes_price as first_yes_price,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time ASC) as rn
        FROM '{tp}' t
        WHERE t.ticker IN (SELECT ticker FROM gm)
    ),
    pregame_trades AS (
        SELECT t.ticker, t.yes_price as pregame_yes_price,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time DESC) as rn
        FROM '{tp}' t
        JOIN gm ON t.ticker = gm.ticker
        WHERE t.created_time < gm.close_time - INTERVAL 2 HOURS
    )
    SELECT gm.ticker, gm.result, gm.sport, gm.volume, gm.close_time,
           gm.event_ticker,
           ft.first_yes_price, pg.pregame_yes_price
    FROM gm
    LEFT JOIN first_trades ft ON gm.ticker = ft.ticker AND ft.rn = 1
    LEFT JOIN pregame_trades pg ON gm.ticker = pg.ticker AND pg.rn = 1
    WHERE gm.sport IS NOT NULL
    ORDER BY gm.close_time
""").fetchdf()

print(f"  {len(markets_df)} markets loaded ({time.time()-t0:.1f}s)")

# Load max NO price per market (for profit-take checks)
print("  Loading max NO prices for profit-take...")

traded_tickers = markets_df["ticker"].tolist()

# Insert tickers into temp table for efficient join
con.execute("CREATE TEMP TABLE sim_tickers (ticker VARCHAR)")
batch = [(t,) for t in traded_tickers]
con.executemany("INSERT INTO sim_tickers VALUES (?)", batch)

max_no_df = con.sql(f"""
    SELECT t.ticker, MAX(100 - t.yes_price) as max_no_price
    FROM '{tp}' t
    WHERE t.ticker IN (SELECT ticker FROM sim_tickers)
    GROUP BY t.ticker
""").fetchdf()

max_no_map = {}
for _, row in max_no_df.iterrows():
    max_no_map[row["ticker"]] = int(row["max_no_price"])

print(f"  Max NO prices loaded for {len(max_no_map)} markets ({time.time()-t0:.1f}s)")


# ═══════════════════════════════════════════════════════════════════════
# BUILD MARKET LIST
# ═══════════════════════════════════════════════════════════════════════

print("  Building market list...")

markets = []
for _, row in markets_df.iterrows():
    ticker = row["ticker"]
    sport = row["sport"]
    result = row["result"]
    volume = float(row["volume"]) if not _isna(row["volume"]) else 0
    close_time = row["close_time"]

    try:
        first_yp = int(row["first_yes_price"]) if not _isna(row["first_yes_price"]) else None
    except (TypeError, ValueError):
        first_yp = None
    try:
        pregame_yp = int(row["pregame_yes_price"]) if not _isna(row["pregame_yes_price"]) else None
    except (TypeError, ValueError):
        pregame_yp = None

    if sport not in SPORT_PARAMS:
        continue

    # Extract date info from close_time
    ct_str = str(close_time)
    m_num = int(ct_str[5:7])
    d_num = int(ct_str[8:10])
    date_str = ct_str[:10]
    month_str = ct_str[:7]

    game_id = extract_game_id(ticker)

    markets.append({
        "ticker": ticker,
        "sport": sport,
        "result": result,
        "volume": volume,
        "close_time": close_time,
        "first_yp": first_yp,
        "pregame_yp": pregame_yp,
        "m_num": m_num,
        "d_num": d_num,
        "date": date_str,
        "month": month_str,
        "game_id": game_id,
    })

print(f"  {len(markets)} markets ready for simulation ({time.time()-t0:.1f}s)")


# ═══════════════════════════════════════════════════════════════════════
# SIMULATION ENGINE
# ═══════════════════════════════════════════════════════════════════════

def run_simulation(markets, mode="first_trade"):
    """
    Run full agent simulation.
    mode: "first_trade" or "pregame"
    Returns dict of results.
    """
    bankroll = STARTING_BANKROLL
    nav_peak = STARTING_BANKROLL   # for drawdown circuit breaker
    equity_peak = STARTING_BANKROLL
    max_dd = 0.0
    total_trades = 0
    total_wins = 0
    halted = False

    sport_stats = defaultdict(lambda: {"trades": 0, "wins": 0, "pnl": 0.0, "wagered": 0.0})
    daily_returns = defaultdict(float)  # date -> total pnl
    daily_bankroll = {}                 # date -> ending bankroll

    # Position tracking
    open_positions = 0
    game_positions = defaultdict(int)   # game_id -> count
    last_date = None

    # Bayesian + CUSUM state
    bma = BMAState()
    cusum = CUSUMState()

    for mkt in markets:
        sport = mkt["sport"]
        params = SPORT_PARAMS[sport]

        # Pick yes price based on mode
        if mode == "first_trade":
            yp = mkt["first_yp"]
        else:
            yp = mkt["pregame_yp"]

        if yp is None:
            continue

        # Apply spread penalty for pre-game mode
        # We buy NO, so our cost is higher (NO price goes up = YES price goes down)
        # Spread penalty increases our NO cost by 1.5c
        effective_no_cents = (100 - yp)
        if mode == "pregame":
            effective_no_cents += SPREAD_PENALTY
            # Adjust yp to reflect the worse price we actually get
            yp = int(100 - effective_no_cents)

        if yp < 55 or yp > 95:
            continue

        # Playoff vetoes
        if sport == "NBA" and ((mkt["m_num"] > 4 or (mkt["m_num"] == 4 and mkt["d_num"] > 19)) and mkt["m_num"] < 10):
            continue
        if sport == "NHL" and ((mkt["m_num"] > 4 or (mkt["m_num"] == 4 and mkt["d_num"] > 16)) and mkt["m_num"] < 10):
            continue

        # Reset daily counters on new date
        if mkt["date"] != last_date:
            if last_date is not None:
                daily_bankroll[last_date] = bankroll
            last_date = mkt["date"]
            open_positions = 0
            game_positions.clear()

        # Halted by 40% drawdown
        if halted:
            # Still update BMA/CUSUM from settlements but don't trade
            _update_bayesian_after(bma, cusum, mkt, sport, yp, total_trades)
            continue

        # Position limits
        if open_positions >= MAX_POSITIONS:
            _update_bayesian_after(bma, cusum, mkt, sport, yp, total_trades)
            continue
        if game_positions[mkt["game_id"]] >= MAX_PER_GAME:
            _update_bayesian_after(bma, cusum, mkt, sport, yp, total_trades)
            continue

        # Get YES rate — try BMA first, fall back to static table
        bk = bucket_key(sport, yp)
        static_rate = get_static_yes_rate(sport, yp)
        if static_rate is None:
            _update_bayesian_after(bma, cusum, mkt, sport, yp, total_trades)
            continue

        bma_rate = bma.get_rate(bk, static_rate)
        yes_rate = bma_rate if bma_rate is not None else static_rate

        # Compute edge
        no_cents = 100 - yp
        no_cost = no_cents / 100.0
        edge = (yp / 100.0) - yes_rate

        # Fee-aware edge
        # fee = ceil(0.07 * C * P * (1-P)) — per contract, C=1 for per-unit calc
        fee_per_unit_cents = math.ceil(0.07 * no_cost * (1 - no_cost) * 100)
        fee_drag = fee_per_unit_cents / 100.0 / no_cost  # as fraction of NO cost

        min_edge = params["me"]
        if edge - fee_drag < min_edge:
            _update_bayesian_after(bma, cusum, mkt, sport, yp, total_trades)
            continue

        # Kelly sizing
        b = (yp / 100.0) / no_cost if no_cost > 0 else 0
        kr = (b * (1 - yes_rate) - yes_rate) / b if b > 0 else 0

        km = params["km"]
        mp_ = params["mp"]

        # April debuff for NBA/NHL
        if mkt["m_num"] == 4 and sport in ("NBA", "NHL"):
            km *= APRIL_DEBUFF
            mp_ *= APRIL_DEBUFF

        # Drawdown circuit breaker
        dd_from_peak = (nav_peak - bankroll) / nav_peak if nav_peak > 0 else 0
        if dd_from_peak >= 0.40:
            halted = True
            _update_bayesian_after(bma, cusum, mkt, sport, yp, total_trades)
            continue
        elif dd_from_peak >= 0.25:
            km *= DD_REDUCE_25
            mp_ *= DD_REDUCE_25
        elif dd_from_peak >= 0.15:
            km *= DD_REDUCE_15
            mp_ *= DD_REDUCE_15

        # CUSUM alarm reduction
        if cusum.is_alarmed(sport):
            km *= CUSUM_KELLY_MULT
            mp_ *= CUSUM_KELLY_MULT

        # Final Kelly allocation
        ka = max(0.0, min(kr * km * SPREAD_THIN, mp_ * SPREAD_THIN))
        if ka <= 0:
            _update_bayesian_after(bma, cusum, mkt, sport, yp, total_trades)
            continue

        bet = min(bankroll * ka, MAX_BET)
        contracts = max(1, int(bet / no_cost))
        cost = contracts * no_cost
        if cost > bankroll:
            contracts = max(1, int(bankroll / no_cost))
            cost = contracts * no_cost
            if cost > bankroll:
                _update_bayesian_after(bma, cusum, mkt, sport, yp, total_trades)
                continue

        entry_fee = kalshi_fee_cents(contracts, no_cents)

        # Check profit-take
        pt_pct = SPORT_PT.get(sport)
        pt_triggered = False
        if pt_pct is not None:
            pt_target_no = no_cost * (1 + pt_pct / 100.0)
            pt_target_no_cents = int(pt_target_no * 100)
            max_no = max_no_map.get(mkt["ticker"], 0)
            if max_no >= pt_target_no_cents:
                # Exit at target minus exit fee
                exit_no_cost = pt_target_no
                exit_fee = kalshi_fee_cents(contracts, pt_target_no_cents)
                pnl = contracts * (exit_no_cost - no_cost) - entry_fee - exit_fee
                pt_triggered = True

        if not pt_triggered:
            # Hold to settlement
            if mkt["result"] == "no":
                # NO wins — we get $1 per contract
                pnl = contracts * (1.0 - no_cost) - entry_fee
                total_wins += 1
                sport_stats[sport]["wins"] += 1
            else:
                # YES wins — we lose our cost + entry fee
                pnl = -(cost + entry_fee)

        bankroll += pnl
        total_trades += 1
        open_positions += 1
        game_positions[mkt["game_id"]] += 1

        sport_stats[sport]["trades"] += 1
        sport_stats[sport]["pnl"] += pnl
        sport_stats[sport]["wagered"] += cost

        daily_returns[mkt["date"]] += pnl

        # Update peaks
        if bankroll > nav_peak:
            nav_peak = bankroll
        if bankroll > equity_peak:
            equity_peak = bankroll
        dd = (equity_peak - bankroll) / equity_peak if equity_peak > 0 else 0
        if dd > max_dd:
            max_dd = dd

        # Update BMA and CUSUM
        _update_bayesian_after(bma, cusum, mkt, sport, yp, total_trades)

        if bankroll < 1.0:
            halted = True

    # Record final day
    if last_date is not None:
        daily_bankroll[last_date] = bankroll

    # Compute Sharpe
    daily_pnl_list = [daily_returns[d] for d in sorted(daily_returns.keys())]
    if len(daily_pnl_list) > 1:
        avg_d = sum(daily_pnl_list) / len(daily_pnl_list)
        std_d = (sum((x - avg_d) ** 2 for x in daily_pnl_list) / len(daily_pnl_list)) ** 0.5
        sharpe = (avg_d / std_d * math.sqrt(252)) if std_d > 0 else 0.0
    else:
        sharpe = 0.0

    return {
        "bankroll": bankroll,
        "max_dd": max_dd,
        "sharpe": sharpe,
        "total_trades": total_trades,
        "total_wins": total_wins,
        "sport_stats": dict(sport_stats),
        "bma": bma,
        "cusum": cusum,
        "halted": halted,
    }


def _update_bayesian_after(bma, cusum, mkt, sport, yp, trade_num):
    """Update BMA and CUSUM after observing a market result."""
    static_rate = get_static_yes_rate(sport, yp)
    if static_rate is None:
        return
    bk = bucket_key(sport, yp)
    yes_outcome = (mkt["result"] == "yes")
    bma.update(bk, static_rate, yes_outcome)

    expected_no_rate = 1 - static_rate
    actual_no = (mkt["result"] == "no")
    cusum.update(sport, expected_no_rate, actual_no, trade_num, mkt["month"])


# ═══════════════════════════════════════════════════════════════════════
# RUN BOTH MODES
# ═══════════════════════════════════════════════════════════════════════

print("\n--- Running First-Trade simulation ---")
res_ft = run_simulation(markets, mode="first_trade")
print(f"  Done: {res_ft['total_trades']} trades, final=${res_ft['bankroll']:.2f} ({time.time()-t0:.1f}s)")

print("--- Running Pre-Game (2h) simulation ---")
res_pg = run_simulation(markets, mode="pregame")
print(f"  Done: {res_pg['total_trades']} trades, final=${res_pg['bankroll']:.2f} ({time.time()-t0:.1f}s)")


# ═══════════════════════════════════════════════════════════════════════
# OUTPUT
# ═══════════════════════════════════════════════════════════════════════

def fmt_pct(v):
    return f"{v*100:.1f}%"

def fmt_dollar(v):
    return f"${v:,.2f}"

wr_ft = res_ft["total_wins"] / res_ft["total_trades"] if res_ft["total_trades"] > 0 else 0
wr_pg = res_pg["total_wins"] / res_pg["total_trades"] if res_pg["total_trades"] > 0 else 0
ret_ft = (res_ft["bankroll"] - STARTING_BANKROLL) / STARTING_BANKROLL
ret_pg = (res_pg["bankroll"] - STARTING_BANKROLL) / STARTING_BANKROLL

print()
print("=" * 65)
print("  EDGERUNNER REALISTIC BACKTEST (TrevorJS 2024-2025)")
print("=" * 65)
print()
print(f"  {'':20s} {'FIRST-TRADE':>14s}    {'PRE-GAME (2h)':>14s}")
print(f"  {'Starting:':20s} {fmt_dollar(STARTING_BANKROLL):>14s}    {fmt_dollar(STARTING_BANKROLL):>14s}")
print(f"  {'Final:':20s} {fmt_dollar(res_ft['bankroll']):>14s}    {fmt_dollar(res_pg['bankroll']):>14s}")
print(f"  {'Return:':20s} {'+' if ret_ft >= 0 else ''}{ret_ft*100:.1f}%{'':>8s}    {'+' if ret_pg >= 0 else ''}{ret_pg*100:.1f}%")
print(f"  {'Max Drawdown:':20s} {fmt_pct(res_ft['max_dd']):>14s}    {fmt_pct(res_pg['max_dd']):>14s}")
print(f"  {'Sharpe (daily):':20s} {res_ft['sharpe']:>14.3f}    {res_pg['sharpe']:>14.3f}")
print(f"  {'Win Rate:':20s} {fmt_pct(wr_ft):>14s}    {fmt_pct(wr_pg):>14s}")
print(f"  {'Total Trades:':20s} {res_ft['total_trades']:>14d}    {res_pg['total_trades']:>14d}")
if res_ft["halted"] or res_pg["halted"]:
    ft_halt = "YES" if res_ft["halted"] else "no"
    pg_halt = "YES" if res_pg["halted"] else "no"
    print(f"  {'Halted (40% DD):':20s} {ft_halt:>14s}    {pg_halt:>14s}")

# Per-sport breakdown (pre-game)
print()
print("  PER-SPORT BREAKDOWN (Pre-Game):")
print(f"  {'Sport':<12s} {'Trades':>7s} {'Win%':>7s} {'P&L':>12s} {'ROI':>8s}")
print("  " + "-" * 48)

for sport in sorted(res_pg["sport_stats"].keys()):
    ss = res_pg["sport_stats"][sport]
    if ss["trades"] == 0:
        continue
    wr = ss["wins"] / ss["trades"] if ss["trades"] > 0 else 0
    roi = (ss["pnl"] / ss["wagered"] * 100) if ss["wagered"] > 0 else 0
    sign = "+" if ss["pnl"] >= 0 else ""
    print(f"  {sport:<12s} {ss['trades']:>7d} {wr*100:>6.1f}% ${sign}{ss['pnl']:>9.2f} {'+' if roi >= 0 else ''}{roi:.1f}%")

# BMA final weights
print()
print("  BMA FINAL WEIGHTS (slow vs fast):")
bma_state = res_pg["bma"]
shown = 0
for key in sorted(bma_state.filters.keys()):
    f = bma_state.filters[key]
    if f["count"] >= BMA_MIN_UPDATES:
        mean_s = f["alpha_s"] / (f["alpha_s"] + f["beta_s"])
        mean_f = f["alpha_f"] / (f["alpha_f"] + f["beta_f"])
        w = f["w_slow"]
        print(f"  {key}: w_slow={w:.2f} slow_mean={mean_s:.3f} fast_mean={mean_f:.3f} (n={f['count']})")
        shown += 1
if shown == 0:
    print("  (no buckets with sufficient updates)")

# CUSUM alarms
print()
print("  CUSUM ALARMS:")
cusum_state = res_pg["cusum"]
any_alarm = False
for sport in sorted(cusum_state.state.keys()):
    s = cusum_state.state[sport]
    if s["alarmed"]:
        print(f"  {sport}: alarmed at trade #{s['alarm_trade']} (month {s['alarm_month']})")
        any_alarm = True
    else:
        print(f"  {sport}: S={s['S']:.2f} (no alarm)")
if not any_alarm:
    print("  (no alarms triggered)")

print()
print(f"  Total runtime: {time.time()-t0:.1f}s")
print("=" * 65)
