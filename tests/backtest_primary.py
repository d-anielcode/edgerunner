"""
PRIMARY BACKTEST — EdgeRunner Optimization Framework

Combines trajectory analysis (real intra-game price paths) with parameter
optimization to find the best (profit_take, entry_range) per sport.

Uses DuckDB for all heavy lifting on 154M+ trades. Python only iterates
over the ~2-3K tradeable markets × parameter grid.

This replaces backtest_spread_thin.py and backtest_profit_take.py as the
single source of truth for strategy evaluation.
"""

import math
import time
import duckdb
from collections import defaultdict

t0 = time.time()

# ═══════════════════════════════════════════════════════════════════════
# CONFIG — matches live agent exactly
# ═══════════════════════════════════════════════════════════════════════

STARTING_BANKROLL = 100.0
MAX_BET = 200.0
SPREAD_THIN = 0.50  # Updated from 0.33 to 0.50 (Kelly scaling test result)
APRIL_REDUCTION = 0.5
DAILY_CAP = 25
MIN_TRADES_PER_MARKET = 20

SPORT_PARAMS = {
    # OOS validated / decayed
    "NBA":       {"km": 0.04, "mp": 0.03, "me": 0.15},
    "NHL":       {"km": 0.15, "mp": 0.08, "me": 0.12},
    "EPL":       {"km": 0.25, "mp": 0.10, "me": 0.10},
    "UCL":       {"km": 0.12, "mp": 0.06, "me": 0.08},
    "LALIGA":    {"km": 0.08, "mp": 0.04, "me": 0.15},
    "WNBA":      {"km": 0.15, "mp": 0.08, "me": 0.08},
    "UFC":       {"km": 0.06, "mp": 0.03, "me": 0.12},
    "NCAAMB":    {"km": 0.10, "mp": 0.05, "me": 0.08},
    "NCAAWB":    {"km": 0.12, "mp": 0.06, "me": 0.08},
    "ATP":       {"km": 0.12, "mp": 0.06, "me": 0.08},
    "WTA":       {"km": 0.10, "mp": 0.05, "me": 0.08},
    "NFLTD":     {"km": 0.20, "mp": 0.10, "me": 0.05},
    "NHLSPREAD": {"km": 0.15, "mp": 0.08, "me": 0.05},
    "NBASPREAD": {"km": 0.06, "mp": 0.03, "me": 0.12},
    "NFLSPREAD": {"km": 0.06, "mp": 0.03, "me": 0.12},
    "MLB":       {"km": 0.06, "mp": 0.03, "me": 0.12},
    # New risk-adjusted markets
    "MLBTOTAL":  {"km": 0.15, "mp": 0.08, "me": 0.05},
    "NFLGW":     {"km": 0.12, "mp": 0.06, "me": 0.08},
    "NFLTT":     {"km": 0.10, "mp": 0.05, "me": 0.08},
    "CBA":       {"km": 0.12, "mp": 0.06, "me": 0.08},
    "LIGUE1":    {"km": 0.10, "mp": 0.05, "me": 0.08},
    "LOL":       {"km": 0.12, "mp": 0.06, "me": 0.08},
    "ATPCH":     {"km": 0.10, "mp": 0.05, "me": 0.08},
}

EDGE_TABLES = {
    "EPL":      {(71, 85): 0.485},
    "UCL":      {(66, 70): 0.400, (76, 85): 0.641},
    "LALIGA":   {(81, 90): 0.588},
    "WNBA":     {(55, 62): 0.380, (71, 77): 0.550, (83, 87): 0.540},
    "UFC":      {(76, 85): 0.622},
    "NCAAMB":   {(66, 70): 0.536, (71, 80): 0.656, (82, 90): 0.770},
    "NCAAWB":   {(61, 70): 0.600, (71, 80): 0.680, (81, 85): 0.750},
    "ATP":      {(71, 75): 0.650, (76, 80): 0.654, (81, 85): 0.765},
    "WTA":      {(76, 79): 0.695, (80, 84): 0.803, (85, 90): 0.790},
    "NFLTD":    {(55, 65): 0.492, (66, 75): 0.452, (76, 85): 0.545, (86, 95): 0.286},
    "NHLSPREAD":{(55, 65): 0.500, (66, 75): 0.450, (76, 90): 0.400},
    "NBASPREAD":{(55, 65): 0.480, (66, 75): 0.440, (76, 90): 0.380},
    "NFLSPREAD":{(55, 65): 0.480, (66, 75): 0.440, (76, 90): 0.380},
    "MLB":      {(76, 84): 0.640},
    "MLBTOTAL": {(55, 65): 0.500, (66, 75): 0.480, (76, 85): 0.450},
    "NFLGW":    {(55, 65): 0.520, (66, 75): 0.580, (76, 90): 0.650},
    "NFLTT":    {(55, 65): 0.500, (66, 75): 0.480, (76, 85): 0.450},
    "CBA":      {(55, 65): 0.500, (66, 75): 0.550, (76, 85): 0.620},
    "LIGUE1":   {(55, 65): 0.480, (66, 75): 0.500, (76, 85): 0.550},
    "LOL":      {(55, 65): 0.500, (66, 75): 0.520, (76, 85): 0.550},
    "ATPCH":    {(55, 65): 0.520, (66, 75): 0.550, (76, 85): 0.620},
}

SPORT_FROM_PREFIX = {
    "KXNBAGAME": "NBA", "KXNHLGAME": "NHL", "KXEPLGAME": "EPL",
    "KXUCLGAME": "UCL", "KXLALIGAGAME": "LALIGA", "KXWNBAGAME": "WNBA",
    "KXUFCFIGHT": "UFC", "KXNCAAMBGAME": "NCAAMB", "KXNCAAWBGAME": "NCAAWB",
    "KXATPCHALLENGERMATCH": "ATPCH",  # Must be before KXATPMATCH
    "KXATPMATCH": "ATP", "KXWTAMATCH": "WTA", "KXNFLANYTD": "NFLTD",
    "KXNHLSPREAD": "NHLSPREAD", "KXNBASPREAD": "NBASPREAD",
    "KXNFLSPREAD": "NFLSPREAD", "KXMLBGAME": "MLB", "KXMLBTOTAL": "MLBTOTAL",
    "KXNFLGAME": "NFLGW", "KXNFLTEAMTOTAL": "NFLTT",
    "KXCBAGAME": "CBA", "KXLIGUE": "LIGUE1", "KXLOLMAP": "LOL",
}

# Sport-specific profit-take (from optimization)
SPORT_PT = {
    "EPL": 100, "NBA": 150, "NBASPREAD": 150, "NCAAMB": 100,
    "NFLSPREAD": 200, "NFLTD": 100, "NHL": 100, "NHLSPREAD": 300,
    "UCL": 100, "UFC": 200, "WNBA": 100, "ATP": 100, "WTA": 150,
    "CFB": 200, "MLB": 50, "LALIGA": 200,
    "MLBTOTAL": 100, "NFLGW": 100, "NFLTT": 150, "CBA": 100,
    "LIGUE1": 100, "LOL": 100, "ATPCH": 50,
}

# Low-edge sports that can trade at 55c
LOW_EDGE_SPORTS = ("NFLTD", "MLBTOTAL", "NFLGW", "NFLTT", "CBA", "LIGUE1", "LOL", "ATPCH")

# Optimization grid
PT_LEVELS = [None, 50, 100, 150, 200, 300]
ENTRY_RANGES = [(61, 90), (66, 85), (71, 85), (76, 90)]


def per_price_yes_rate(sport, yp):
    if sport == "NBA":
        return max(0.20, 0.50 - (yp - 60) * 0.004)
    if sport == "NHL":
        return max(0.30, 0.55 - (yp - 60) * 0.003)
    return None


def kalshi_fee(price_cents):
    p = price_cents / 100.0
    return math.ceil(0.07 * p * (1 - p) * 100) / 100


def get_actual_yes(sport, yp):
    pp = per_price_yes_rate(sport, yp)
    if pp is not None:
        return pp
    et = EDGE_TABLES.get(sport, {})
    for (lo, hi), value in et.items():
        if lo <= yp <= hi:
            return value[0] if isinstance(value, tuple) else value
    return None


# ═══════════════════════════════════════════════════════════════════════
# PHASE 1: Load data
# ═══════════════════════════════════════════════════════════════════════

con = duckdb.connect()
mp = "data/trevorjs/markets-*.parquet"
tp = "data/trevorjs/trades-*.parquet"

case_parts = [f"WHEN event_ticker LIKE '{p}%' THEN '{s}'" for p, s in SPORT_FROM_PREFIX.items()]
case_stmt = " ".join(case_parts)
like_clauses = " OR ".join(f"event_ticker LIKE '{p}%'" for p in SPORT_FROM_PREFIX.keys())

print("Phase 1: Loading markets + first trades...")

markets_df = con.sql(f"""
    WITH gm AS (
        SELECT ticker, result, event_ticker, volume,
               CASE {case_stmt} END as sport
        FROM '{mp}'
        WHERE ({like_clauses}) AND status='finalized' AND result IN ('yes','no')
    ),
    trade_stats AS (
        SELECT ticker, MIN(created_time) as first_time, COUNT(*) as trade_count
        FROM '{tp}'
        WHERE ticker IN (SELECT ticker FROM gm) AND created_time >= '2025-01-01'
        GROUP BY ticker HAVING COUNT(*) >= {MIN_TRADES_PER_MARKET}
    ),
    first_trades AS (
        SELECT t.ticker, t.yes_price, t.no_price, t.created_time,
               CAST(t.created_time AS DATE) as trade_date
        FROM '{tp}' t
        JOIN trade_stats ts ON t.ticker = ts.ticker AND t.created_time = ts.first_time
    )
    SELECT ft.ticker, ft.yes_price, ft.trade_date, ft.created_time as entry_time,
           gm.result, gm.sport, gm.volume, ts.trade_count
    FROM first_trades ft
    JOIN gm ON ft.ticker = gm.ticker
    JOIN trade_stats ts ON ft.ticker = ts.ticker
    WHERE gm.sport IS NOT NULL
    ORDER BY ft.created_time
""").fetchdf()

print(f"  {len(markets_df)} qualified markets loaded ({time.time()-t0:.1f}s)")

# ═══════════════════════════════════════════════════════════════════════
# PHASE 2: Build tradeable markets list (widest entry range)
# ═══════════════════════════════════════════════════════════════════════

print("Phase 2: Filtering tradeable markets (widest range)...")

all_markets = []
for _, row in markets_df.iterrows():
    ticker = row["ticker"]
    yp = int(row["yes_price"])
    sport = row["sport"]
    d = str(row["trade_date"])
    volume = float(row["volume"])
    result = row["result"]
    m_num = int(d[5:7])
    d_num = int(d[8:10])

    # Playoff vetoes
    if sport == "NHL" and ((m_num > 4 or (m_num == 4 and d_num > 16)) and m_num < 10):
        continue
    if sport == "NBA" and ((m_num > 4 or (m_num == 4 and d_num > 19)) and m_num < 10):
        continue
    if sport == "NBA" and 500_000 <= volume <= 2_000_000:
        continue

    # Widest possible range — grid search will narrow per-sport
    if yp < 55 or yp > 95:
        continue

    actual_yes = get_actual_yes(sport, yp)
    if actual_yes is None:
        continue

    params = SPORT_PARAMS.get(sport)
    if not params:
        continue

    # Compute edge and Kelly (used at simulation time)
    edge = (yp / 100.0) - actual_yes
    no_cost = (100 - yp) / 100.0
    fee = 0.07 * no_cost * (1 - no_cost)
    friction = (fee + 0.005) / no_cost if no_cost > 0 else 0

    b = (yp / 100.0) / no_cost if no_cost > 0 else 0
    kr = (b * (1 - actual_yes) - actual_yes) / b if b > 0 else 0

    all_markets.append({
        "ticker": ticker, "yp": yp, "sport": sport, "date": d,
        "result": result, "volume": volume, "m_num": m_num, "d_num": d_num,
        "edge": edge, "no_cost_cents": 100 - yp, "kr": kr, "friction": friction,
    })

print(f"  {len(all_markets)} markets pass basic filters")

# ═══════════════════════════════════════════════════════════════════════
# PHASE 3: Load trajectory data from DuckDB
# ═══════════════════════════════════════════════════════════════════════

all_tickers = [m["ticker"] for m in all_markets]
con.execute("CREATE TEMP TABLE all_tickers (ticker VARCHAR)")
for t in all_tickers:
    con.execute("INSERT INTO all_tickers VALUES (?)", [t])

print(f"Phase 3: Loading trajectories for {len(all_tickers)} markets...")

# Get min YES price per market (max swing in our favor)
min_yes_df = con.sql(f"""
    WITH first_times AS (
        SELECT ticker, MIN(created_time) as first_time
        FROM '{tp}' WHERE ticker IN (SELECT ticker FROM all_tickers)
        AND created_time >= '2025-01-01' GROUP BY ticker
    )
    SELECT t.ticker, MIN(t.yes_price) as min_yes
    FROM '{tp}' t
    JOIN first_times ft ON t.ticker = ft.ticker
    WHERE t.created_time > ft.first_time AND t.created_time >= '2025-01-01'
    GROUP BY t.ticker
""").fetchdf()

min_yes_map = {}
for _, row in min_yes_df.iterrows():
    min_yes_map[row["ticker"]] = int(row["min_yes"])

# For each PT level, find crossing points
pt_crossings = {}  # ticker -> {100: exit_yes, 150: exit_yes, 200: exit_yes, 300: exit_yes}

for pt_pct in [50, 100, 150, 200, 300]:
    pt_mult = 1 + pt_pct / 100.0  # 1.5x, 2x, 2.5x, 3x, 4x

    candidates = []
    for m in all_markets:
        no_cents = m["no_cost_cents"]
        yes_max = int(100 - pt_mult * no_cents)
        min_yes = min_yes_map.get(m["ticker"], 999)
        if yes_max > 0 and min_yes <= yes_max:
            candidates.append((m["ticker"], yes_max))

    if not candidates:
        continue

    con.execute(f"DROP TABLE IF EXISTS pt{pt_pct}_cands")
    con.execute(f"CREATE TEMP TABLE pt{pt_pct}_cands (ticker VARCHAR, yes_max INTEGER)")
    for tk, ym in candidates:
        con.execute(f"INSERT INTO pt{pt_pct}_cands VALUES (?, ?)", [tk, ym])

    crossing_df = con.sql(f"""
        WITH first_times AS (
            SELECT ticker, MIN(created_time) as first_time
            FROM '{tp}' WHERE ticker IN (SELECT ticker FROM pt{pt_pct}_cands)
            AND created_time >= '2025-01-01' GROUP BY ticker
        ),
        crosses AS (
            SELECT t.ticker, t.yes_price,
                   ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
            FROM '{tp}' t
            JOIN first_times ft ON t.ticker = ft.ticker
            JOIN pt{pt_pct}_cands c ON t.ticker = c.ticker
            WHERE t.created_time > ft.first_time AND t.created_time >= '2025-01-01'
            AND t.yes_price <= c.yes_max
        )
        SELECT ticker, yes_price FROM crosses WHERE rn = 1
    """).fetchdf()

    for _, row in crossing_df.iterrows():
        tk = row["ticker"]
        if tk not in pt_crossings:
            pt_crossings[tk] = {}
        pt_crossings[tk][pt_pct] = int(row["yes_price"])

print(f"  Trajectories loaded ({time.time()-t0:.1f}s)")
for pt in [100, 150, 200, 300]:
    n = sum(1 for v in pt_crossings.values() if pt in v)
    print(f"  {pt}% PT: {n} markets would trigger")

# ═══════════════════════════════════════════════════════════════════════
# PHASE 4: Optimization Grid
# ═══════════════════════════════════════════════════════════════════════

print(f"\nPhase 4: Running optimization grid...")

sports = sorted(set(m["sport"] for m in all_markets))

# Store results: (sport, pt, range) -> metrics
grid_results = {}


def simulate(sport, pt_pct, entry_lo, entry_hi):
    """Simulate one (sport, profit_take, entry_range) configuration."""
    params = SPORT_PARAMS.get(sport)
    if not params:
        return None

    bankroll = STARTING_BANKROLL
    peak = STARTING_BANKROLL
    max_dd = 0.0
    trades = 0
    wins = 0
    pnls = []
    last_date = None
    daily_count = 0

    for m in all_markets:
        if m["sport"] != sport:
            continue
        if m["yp"] < entry_lo or m["yp"] > entry_hi:
            continue

        # Min edge check
        net_edge = m["edge"] - m["friction"]
        if net_edge < params["me"]:
            continue
        if m["edge"] < 0.03:
            continue

        # Daily cap
        if m["date"] != last_date:
            last_date = m["date"]
            daily_count = 0
        if daily_count >= DAILY_CAP:
            continue

        # Kelly sizing
        km = params["km"] * SPREAD_THIN
        mp_ = params["mp"] * SPREAD_THIN
        if m["m_num"] == 4:
            km *= APRIL_REDUCTION
            mp_ *= APRIL_REDUCTION

        ka = max(0.0, min(m["kr"] * km, mp_))
        if ka <= 0:
            continue

        bet = min(bankroll * ka, MAX_BET)
        no_cents = m["no_cost_cents"]
        no_cost = no_cents / 100.0
        contracts = max(1, int(bet / no_cost))
        cost = contracts * no_cost
        if cost > bankroll:
            continue

        entry_fee = kalshi_fee(no_cents) * contracts
        daily_count += 1
        trades += 1

        # Check if profit-take triggers
        pt_triggered = False
        if pt_pct is not None:
            cross = pt_crossings.get(m["ticker"], {}).get(pt_pct)
            if cross is not None:
                # Sell at the crossing price
                exit_no_cents = 100 - cross
                exit_no_cost = exit_no_cents / 100.0
                exit_fee = kalshi_fee(exit_no_cents) * contracts
                pnl = contracts * (exit_no_cost - no_cost) - entry_fee - exit_fee
                pt_triggered = True

        if not pt_triggered:
            # Hold to settlement
            if m["result"] == "no":
                pnl = contracts * (1.0 - no_cost) - entry_fee
                wins += 1
            else:
                pnl = -(cost + entry_fee)

        bankroll += pnl
        pnls.append(pnl)
        if bankroll > peak:
            peak = bankroll
        dd = (peak - bankroll) / peak if peak > 0 else 0
        if dd > max_dd:
            max_dd = dd
        if bankroll < 2:
            break

    if trades < 5:
        return None

    avg_pnl = sum(pnls) / len(pnls) if pnls else 0
    std_pnl = (sum((p - avg_pnl) ** 2 for p in pnls) / len(pnls)) ** 0.5 if len(pnls) > 1 else 1
    sharpe = avg_pnl / std_pnl if std_pnl > 0 else 0

    pt_trigger_rate = 0
    if pt_pct is not None:
        pt_triggers = sum(
            1 for m2 in all_markets
            if m2["sport"] == sport and entry_lo <= m2["yp"] <= entry_hi
            and m2["ticker"] in pt_crossings and pt_pct in pt_crossings[m2["ticker"]]
        )
        pt_trigger_rate = pt_triggers / trades if trades > 0 else 0

    return {
        "trades": trades, "wins": wins, "wr": wins / trades if trades > 0 else 0,
        "pnl": bankroll - STARTING_BANKROLL, "final": bankroll,
        "max_dd": max_dd, "sharpe": sharpe, "pt_trigger": pt_trigger_rate,
    }


# Run the grid
for sport in sports:
    for pt in PT_LEVELS:
        for (lo, hi) in ENTRY_RANGES:
            result = simulate(sport, pt, lo, hi)
            if result:
                grid_results[(sport, pt, lo, hi)] = result

print(f"  {len(grid_results)} grid points computed ({time.time()-t0:.1f}s)")

# ═══════════════════════════════════════════════════════════════════════
# PHASE 5: Output
# ═══════════════════════════════════════════════════════════════════════

print("\n" + "=" * 90)
print("EDGERUNNER PRIMARY BACKTEST — OPTIMIZATION RESULTS")
print(f"Period: 2025-01 to 2026-01 | Starting: ${STARTING_BANKROLL}")
print("=" * 90)

# 5a: Best config per sport (by Sharpe)
print("\n--- BEST CONFIGURATION PER SPORT (by Sharpe) ---\n")
print(f"{'Sport':<12} {'PT%':>5} {'Range':>8} {'Trades':>7} {'WR':>6} {'P&L':>12} {'MaxDD':>7} {'Sharpe':>7} {'PT Trig':>8}")
print("-" * 80)

best_per_sport = {}
for sport in sports:
    sport_results = {k: v for k, v in grid_results.items() if k[0] == sport}
    if not sport_results:
        continue
    best_key = max(sport_results, key=lambda k: sport_results[k]["sharpe"])
    best = sport_results[best_key]
    _, pt, lo, hi = best_key
    pt_str = f"{pt}%" if pt else "HOLD"
    best_per_sport[sport] = {"pt": pt, "lo": lo, "hi": hi, **best}
    print(f"{sport:<12} {pt_str:>5} {lo}-{hi:>2}c {best['trades']:>7} {best['wr']:>5.0%} "
          f"${best['pnl']:>10,.0f} {best['max_dd']:>6.1%} {best['sharpe']:>7.3f} {best['pt_trigger']:>7.0%}")

# 5b: Current params vs optimal
print("\n--- CURRENT LIVE PARAMS vs OPTIMAL ---\n")
print(f"{'Sport':<12} {'Curr PT':>8} {'Curr Range':>11} {'Opt PT':>7} {'Opt Range':>10} {'P&L Diff':>10} {'DD Diff':>8}")
print("-" * 75)

for sport in sports:
    if sport not in best_per_sport:
        continue
    opt = best_per_sport[sport]

    # Current config: 200% PT globally, sport-specific entry range from edge tables
    curr_pt = 200
    curr_lo, curr_hi = 61, 90
    et = EDGE_TABLES.get(sport, {})
    if et:
        curr_lo = min(lo for lo, _ in et.keys())
        curr_hi = max(hi for _, hi in et.keys())

    # Get current config results
    curr_key = (sport, curr_pt, 61, 90)  # approximate current
    curr = grid_results.get(curr_key, {})
    curr_pnl = curr.get("pnl", 0)
    curr_dd = curr.get("max_dd", 0)

    opt_pt_str = f"{opt['pt']}%" if opt["pt"] else "HOLD"
    diff_pnl = opt["pnl"] - curr_pnl
    diff_dd = opt["max_dd"] - curr_dd

    print(f"{sport:<12} {'200%':>8} {'61-90c':>11} {opt_pt_str:>7} {opt['lo']}-{opt['hi']}c{'':<4} "
          f"{'%+.0f' % diff_pnl:>10} {'%+.1f%%' % (diff_dd * 100):>8}")

# 5c: Per-sport profit-take analysis
print("\n--- PROFIT-TAKE ANALYSIS BY SPORT (entry range 61-90c) ---\n")
print(f"{'Sport':<12} {'HOLD P&L':>10} {'100% P&L':>10} {'150% P&L':>10} {'200% P&L':>10} {'300% P&L':>10} {'Best':>6}")
print("-" * 75)

for sport in sports:
    row = []
    best_pt = None
    best_sharpe = -999
    for pt in PT_LEVELS:
        key = (sport, pt, 61, 90)
        r = grid_results.get(key)
        if r:
            row.append(r["pnl"])
            if r["sharpe"] > best_sharpe:
                best_sharpe = r["sharpe"]
                best_pt = pt
        else:
            row.append(None)

    if not any(v is not None for v in row):
        continue

    vals = [f"${v:>8,.0f}" if v is not None else f"{'N/A':>9}" for v in row]
    best_str = f"{best_pt}%" if best_pt else "HOLD"
    print(f"{sport:<12} {vals[0]:>10} {vals[1]:>10} {vals[2]:>10} {vals[3]:>10} {vals[4]:>10} {best_str:>6}")

# 5d: Per-sport entry range analysis
print("\n--- ENTRY RANGE ANALYSIS BY SPORT (200% PT) ---\n")
print(f"{'Sport':<12} {'61-90c':>10} {'66-85c':>10} {'71-85c':>10} {'76-90c':>10} {'Best':>8}")
print("-" * 60)

for sport in sports:
    row = []
    best_range = None
    best_sharpe = -999
    for (lo, hi) in ENTRY_RANGES:
        key = (sport, 200, lo, hi)
        r = grid_results.get(key)
        if r:
            row.append(r["pnl"])
            if r["sharpe"] > best_sharpe:
                best_sharpe = r["sharpe"]
                best_range = f"{lo}-{hi}"
        else:
            row.append(None)

    if not any(v is not None for v in row):
        continue

    vals = [f"${v:>8,.0f}" if v is not None else f"{'N/A':>9}" for v in row]
    print(f"{sport:<12} {vals[0]:>10} {vals[1]:>10} {vals[2]:>10} {vals[3]:>10} {best_range or 'N/A':>8}")

# 5e: Combined portfolio using LIVE AGENT config (SPORT_PT thresholds)
print("\n--- COMBINED PORTFOLIO (live agent config: SPORT_PT thresholds, 0.50x Kelly) ---\n")

# Override best_per_sport with actual live config
for sport in sports:
    pt = SPORT_PT.get(sport, 150)
    best_per_sport[sport] = {"pt": pt, "lo": 55 if sport in LOW_EDGE_SPORTS else 61, "hi": 95 if sport in LOW_EDGE_SPORTS else 90}

bankroll = STARTING_BANKROLL
peak = STARTING_BANKROLL
max_dd = 0.0
total_trades = 0
total_wins = 0
all_pnls = []
sport_stats = defaultdict(lambda: {"t": 0, "w": 0, "pnl": 0.0})
monthly = {}  # month -> bankroll at end
last_date = None
daily_count = 0
last_month = None

# Sort all markets by date
all_markets_sorted = sorted(all_markets, key=lambda m: m["date"])

for m in all_markets_sorted:
    sport = m["sport"]
    opt = best_per_sport.get(sport)
    if not opt:
        continue

    # Use optimal entry range
    if m["yp"] < opt["lo"] or m["yp"] > opt["hi"]:
        continue

    params = SPORT_PARAMS.get(sport)
    if not params:
        continue

    net_edge = m["edge"] - m["friction"]
    if net_edge < params["me"] or m["edge"] < 0.03:
        continue

    cur_month = m["date"][:7]
    if cur_month != last_month:
        if last_month:
            monthly[last_month] = bankroll
        last_month = cur_month

    if m["date"] != last_date:
        last_date = m["date"]
        daily_count = 0
    if daily_count >= DAILY_CAP:
        continue

    km = params["km"] * SPREAD_THIN
    mp_ = params["mp"] * SPREAD_THIN
    if m["m_num"] == 4:
        km *= APRIL_REDUCTION
        mp_ *= APRIL_REDUCTION

    ka = max(0.0, min(m["kr"] * km, mp_))
    if ka <= 0:
        continue

    bet = min(bankroll * ka, MAX_BET)
    no_cents = m["no_cost_cents"]
    no_cost = no_cents / 100.0
    contracts = max(1, int(bet / no_cost))
    cost = contracts * no_cost
    if cost > bankroll:
        continue

    entry_fee = kalshi_fee(no_cents) * contracts
    daily_count += 1
    total_trades += 1

    # Use optimal PT for this sport
    opt_pt = opt["pt"]
    pt_triggered = False
    if opt_pt is not None:
        cross = pt_crossings.get(m["ticker"], {}).get(opt_pt)
        if cross is not None:
            exit_no_cents = 100 - cross
            exit_no_cost = exit_no_cents / 100.0
            exit_fee = kalshi_fee(exit_no_cents) * contracts
            pnl = contracts * (exit_no_cost - no_cost) - entry_fee - exit_fee
            pt_triggered = True

    if not pt_triggered:
        if m["result"] == "no":
            pnl = contracts * (1.0 - no_cost) - entry_fee
            total_wins += 1
            sport_stats[sport]["w"] += 1
        else:
            pnl = -(cost + entry_fee)

    bankroll += pnl
    all_pnls.append(pnl)
    sport_stats[sport]["t"] += 1
    sport_stats[sport]["pnl"] += pnl
    if bankroll > peak:
        peak = bankroll
    dd = (peak - bankroll) / peak if peak > 0 else 0
    if dd > max_dd:
        max_dd = dd
    if bankroll < 2:
        break

if last_month:
    monthly[last_month] = bankroll

avg_pnl = sum(all_pnls) / len(all_pnls) if all_pnls else 0
std_pnl = (sum((p - avg_pnl) ** 2 for p in all_pnls) / len(all_pnls)) ** 0.5 if len(all_pnls) > 1 else 1
sharpe = avg_pnl / std_pnl if std_pnl > 0 else 0

print(f"  Starting:    ${STARTING_BANKROLL:.2f}")
print(f"  Final:       ${bankroll:>14,.2f}")
print(f"  Trades:      {total_trades:>14,}")
print(f"  Win rate:    {total_wins/total_trades:>13.1%}" if total_trades > 0 else "")
print(f"  Max DD:      {max_dd:>13.1%}")
print(f"  Sharpe:      {sharpe:>13.3f}")

# Monthly breakdown
print(f"\n  MONTHLY:")
prev_br = STARTING_BANKROLL
for mo in sorted(monthly.keys()):
    br = monthly[mo]
    change = br - prev_br
    pct = (change / prev_br * 100) if prev_br > 0 else 0
    print(f"    {mo} | ${br:>12,.2f} | ${change:>+10,.2f} | {pct:>+6.1f}%")
    prev_br = br

print(f"\n  Per-sport:")
for s in sorted(sport_stats.keys()):
    ss = sport_stats[s]
    wr = ss["w"] / ss["t"] if ss["t"] > 0 else 0
    opt = best_per_sport.get(s, {})
    pt_str = f"{opt.get('pt', '?')}%PT" if opt.get("pt") else "HOLD"
    wag = ss["t"] * 0.30  # approximate wagered
    roi = (ss["pnl"] / wag * 100) if wag > 0 else 0
    print(f"    {s:<12} {ss['t']:>5} trades | {wr:>5.0%} WR | ${ss['pnl']:>10,.0f} | {roi:>+6.0f}% ROI | {pt_str}")

# Sorted by P&L
print(f"\n  RANKED BY P&L:")
ranked = sorted(sport_stats.items(), key=lambda x: x[1]["pnl"], reverse=True)
for s, ss in ranked:
    if ss["t"] == 0: continue
    wr = ss["w"] / ss["t"] if ss["t"] > 0 else 0
    print(f"    {s:<12} ${ss['pnl']:>10,.0f}")

print(f"\n  Total time: {time.time()-t0:.1f}s")
