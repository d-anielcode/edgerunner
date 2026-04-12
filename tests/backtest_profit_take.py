"""
Backtest with intra-game price trajectories and profit-take rules.

Loads ALL trades per market to simulate:
  A) Hold to settlement (current strategy)
  B) 200% profit-take (sell at 3x cost)
  C) 100% profit-take (sell at 2x cost)

Uses current live SPORT_PARAMS, 0.33x spread-thin modifier, $200 max bet,
April 0.5x reduction, NHL/NBA playoff vetoes.

Optimized: uses DuckDB for heavy lifting, avoids pandas iterrows on 39M rows.
"""

import math
import time
import duckdb
from collections import defaultdict

t0 = time.time()

# ── Config ─────────────────────────────────────────────────────────────

STARTING_BANKROLL = 100.0
MAX_BET = 200.0
SPREAD_THIN = 0.33
APRIL_REDUCTION = 0.5
DAILY_CAP = 15
MIN_TRADES_PER_MARKET = 20

SPORT_PARAMS = {
    "NBA":    {"kelly_mult": 0.04, "max_position": 0.03, "min_edge": 0.15},
    "NHL":    {"kelly_mult": 0.15, "max_position": 0.08, "min_edge": 0.08},
    "EPL":    {"kelly_mult": 0.25, "max_position": 0.10, "min_edge": 0.10},
    "UCL":    {"kelly_mult": 0.12, "max_position": 0.06, "min_edge": 0.08},
    "LALIGA": {"kelly_mult": 0.08, "max_position": 0.04, "min_edge": 0.15},
    "WNBA":   {"kelly_mult": 0.15, "max_position": 0.08, "min_edge": 0.08},
    "UFC":    {"kelly_mult": 0.12, "max_position": 0.06, "min_edge": 0.08},
    "NCAAMB": {"kelly_mult": 0.10, "max_position": 0.05, "min_edge": 0.08},
    "NCAAWB": {"kelly_mult": 0.12, "max_position": 0.06, "min_edge": 0.08},
    "ATP":    {"kelly_mult": 0.12, "max_position": 0.06, "min_edge": 0.08},
}

EDGE_TABLES = {
    "EPL":    {(71, 85): 0.485},
    "UCL":    {(66, 70): 0.400, (76, 85): 0.641},
    "LALIGA": {(81, 90): 0.588},
    "WNBA":   {(55, 62): 0.380, (71, 77): 0.550, (83, 87): 0.540},
    "UFC":    {(76, 85): 0.622},
    "NCAAMB": {(66, 70): 0.536, (71, 80): 0.656, (82, 90): 0.770},
    "NCAAWB": {(61, 70): 0.600, (71, 80): 0.680, (81, 85): 0.780},
    "ATP":    {(61, 75): 0.550, (76, 85): 0.650},
}

SPORT_FROM_PREFIX = {
    "KXNBAGAME": "NBA", "KXNHLGAME": "NHL", "KXEPLGAME": "EPL",
    "KXUCLGAME": "UCL", "KXLALIGAGAME": "LALIGA", "KXWNBAGAME": "WNBA",
    "KXUFCFIGHT": "UFC", "KXNCAAMBGAME": "NCAAMB", "KXNCAAWBGAME": "NCAAWB",
    "KXATPMATCH": "ATP",
}


def per_price_yes_rate(sport, yes_p):
    if sport == "NBA":
        return max(0.20, 0.50 - (yes_p - 60) * 0.004)
    if sport == "NHL":
        return max(0.30, 0.55 - (yes_p - 60) * 0.003)
    return None


def kalshi_fee(price_cents):
    """Fee in dollars for a price in cents."""
    p = price_cents / 100.0
    return math.ceil(0.07 * p * (1 - p) * 100) / 100


# ── Load data via DuckDB ──────────────────────────────────────────────

con = duckdb.connect()
mp = "data/trevorjs/markets-*.parquet"
tp = "data/trevorjs/trades-*.parquet"

# Build CASE statement for sport mapping
case_parts = []
for prefix, sport in SPORT_FROM_PREFIX.items():
    case_parts.append(f"WHEN event_ticker LIKE '{prefix}%' THEN '{sport}'")
case_stmt = " ".join(case_parts)
like_clauses = " OR ".join(f"event_ticker LIKE '{p}%'" for p in SPORT_FROM_PREFIX.keys())

print("Step 1: Loading markets + first trades + trade counts...")

# Get markets with first trade info AND trade counts in one query
markets_df = con.sql(f"""
    WITH gm AS (
        SELECT ticker, result, event_ticker, volume,
               CASE {case_stmt} END as sport
        FROM '{mp}'
        WHERE ({like_clauses}) AND status='finalized' AND result IN ('yes','no')
    ),
    trade_stats AS (
        SELECT ticker,
               MIN(created_time) as first_time,
               COUNT(*) as trade_count
        FROM '{tp}'
        WHERE ticker IN (SELECT ticker FROM gm)
          AND created_time >= '2025-01-01'
        GROUP BY ticker
        HAVING COUNT(*) >= {MIN_TRADES_PER_MARKET}
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

print(f"  {len(markets_df)} qualified markets (>={MIN_TRADES_PER_MARKET} trades, 2025+)")

# ── Pre-compute which markets pass entry criteria ─────────────────────

print("Step 2: Filtering for tradeable markets...")

tradeable = []
for _, row in markets_df.iterrows():
    ticker = row["ticker"]
    yp = int(row["yes_price"])
    sport = row["sport"]
    d = str(row["trade_date"])
    volume = float(row["volume"])
    result = row["result"]
    entry_time = row["entry_time"]
    m_num = int(d[5:7])
    d_num = int(d[8:10])

    # NHL playoff veto
    if sport == "NHL" and ((m_num > 4 or (m_num == 4 and d_num > 16)) and m_num < 10):
        continue
    # NBA playoff veto
    if sport == "NBA" and ((m_num > 4 or (m_num == 4 and d_num > 19)) and m_num < 10):
        continue
    # NBA volume filter
    if sport == "NBA" and 500_000 <= volume <= 2_000_000:
        continue

    # Price range
    min_price = 61
    max_price = 95 if sport in ("NBA", "NHL") else 90
    if yp < min_price or yp > max_price:
        continue

    # Edge lookup
    pp = per_price_yes_rate(sport, yp)
    if pp is not None:
        actual_yes = pp
    else:
        params = SPORT_PARAMS.get(sport)
        if not params:
            continue
        et = EDGE_TABLES.get(sport, {})
        actual_yes = None
        for (lo, hi), rate in et.items():
            if lo <= yp <= hi:
                actual_yes = rate
                break
        if actual_yes is None:
            continue

    params = SPORT_PARAMS.get(sport)
    if not params:
        continue

    edge = (yp / 100.0) - actual_yes
    if edge < params["min_edge"]:
        continue

    no_cost = (100 - yp) / 100.0
    fee = 0.07 * no_cost * (1 - no_cost)
    friction = (fee + 0.005) / no_cost if no_cost > 0 else 0
    if edge - friction < 0.03:
        continue

    # Kelly
    yes_cost = yp / 100.0
    b = yes_cost / no_cost if no_cost > 0 else 0
    pw = 1 - actual_yes
    kr = (b * pw - actual_yes) / b if b > 0 else 0

    km = params["kelly_mult"] * SPREAD_THIN
    mp_ = params["max_position"] * SPREAD_THIN
    if m_num == 4:
        km *= APRIL_REDUCTION
        mp_ *= APRIL_REDUCTION

    ka = max(0.0, min(kr * km, mp_))
    if ka <= 0:
        continue

    tradeable.append({
        "ticker": ticker,
        "yes_price": yp,
        "sport": sport,
        "date": d,
        "result": result,
        "entry_time": entry_time,
        "kelly_adj": ka,
        "no_cost": no_cost,
    })

print(f"  {len(tradeable)} markets pass entry criteria")

# ── Load price trajectories ONLY for tradeable markets ────────────────

tradeable_tickers = [t["ticker"] for t in tradeable]
if not tradeable_tickers:
    print("No tradeable markets found!")
    exit()

print(f"Step 3: Loading price trajectories for {len(tradeable_tickers)} markets...")

# Use DuckDB to compute min YES price per market (for profit-take check)
# and also get the specific threshold-crossing trade

# Register tradeable tickers as a temp table
con.execute("CREATE TEMP TABLE tradeable_tickers (ticker VARCHAR)")
for t in tradeable_tickers:
    con.execute("INSERT INTO tradeable_tickers VALUES (?)", [t])

# For each tradeable market, get the minimum YES price seen after the first trade
# This tells us the maximum NO value achieved
trajectory_df = con.sql(f"""
    WITH first_times AS (
        SELECT ticker, MIN(created_time) as first_time
        FROM '{tp}'
        WHERE ticker IN (SELECT ticker FROM tradeable_tickers)
          AND created_time >= '2025-01-01'
        GROUP BY ticker
    ),
    subsequent AS (
        SELECT t.ticker, t.yes_price, t.created_time,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
        FROM '{tp}' t
        JOIN first_times ft ON t.ticker = ft.ticker
        WHERE t.created_time > ft.first_time
          AND t.created_time >= '2025-01-01'
    )
    SELECT ticker,
           MIN(yes_price) as min_yes_price
    FROM subsequent
    GROUP BY ticker
""").fetchdf()

# Build lookup: ticker -> min YES price seen after entry
min_yes_by_ticker = {}
for _, row in trajectory_df.iterrows():
    min_yes_by_ticker[row["ticker"]] = int(row["min_yes_price"])

print(f"  Trajectories loaded in {time.time()-t0:.1f}s")

# ── Now we also need the FIRST trade where specific thresholds are crossed ─
# For 200% PT: NO value >= 3 * entry_no_cost => YES <= 100 - 3*entry_no_cents
# For 100% PT: NO value >= 2 * entry_no_cost => YES <= 100 - 2*entry_no_cents
# We need the actual YES price at that crossing point to calculate exit fees

# Pre-compute thresholds per ticker
thresholds = {}
for t in tradeable:
    no_cents = 100 - t["yes_price"]
    thresholds[t["ticker"]] = {
        "no_cents": no_cents,
        "pt200_yes_max": 100 - 3 * no_cents,  # YES must be <= this for 200% PT
        "pt100_yes_max": 100 - 2 * no_cents,  # YES must be <= this for 100% PT
    }

# For markets where min_yes <= threshold, find the exact crossing price
# We need to query for the first trade that crosses each threshold
# Do this in batches to avoid huge queries

print("Step 4: Finding exact profit-take crossing points...")

# Build per-ticker threshold queries
pt_results = {}  # ticker -> {200: yes_price_at_cross, 100: yes_price_at_cross}

# We'll do this in one query per threshold level
for pt_level, pt_mult in [(200, 3), (100, 2)]:
    # Filter to tickers where the threshold could have been crossed
    candidates = []
    for t in tradeable:
        tk = t["ticker"]
        thresh = thresholds[tk]
        yes_max = thresh[f"pt{pt_level}_yes_max"]
        min_yes = min_yes_by_ticker.get(tk, 999)
        if min_yes <= yes_max and yes_max > 0:
            candidates.append((tk, yes_max))

    if not candidates:
        continue

    print(f"  {pt_level}% PT: {len(candidates)} candidates to check")

    # Create temp table with thresholds
    con.execute(f"DROP TABLE IF EXISTS pt{pt_level}_candidates")
    con.execute(f"CREATE TEMP TABLE pt{pt_level}_candidates (ticker VARCHAR, yes_max INTEGER)")
    for tk, ym in candidates:
        con.execute(f"INSERT INTO pt{pt_level}_candidates VALUES (?, ?)", [tk, ym])

    # Find first trade crossing the threshold for each ticker
    crossing_df = con.sql(f"""
        WITH first_times AS (
            SELECT ticker, MIN(created_time) as first_time
            FROM '{tp}'
            WHERE ticker IN (SELECT ticker FROM pt{pt_level}_candidates)
              AND created_time >= '2025-01-01'
            GROUP BY ticker
        ),
        crosses AS (
            SELECT t.ticker, t.yes_price,
                   ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
            FROM '{tp}' t
            JOIN first_times ft ON t.ticker = ft.ticker
            JOIN pt{pt_level}_candidates c ON t.ticker = c.ticker
            WHERE t.created_time > ft.first_time
              AND t.created_time >= '2025-01-01'
              AND t.yes_price <= c.yes_max
        )
        SELECT ticker, yes_price FROM crosses WHERE rn = 1
    """).fetchdf()

    for _, row in crossing_df.iterrows():
        tk = row["ticker"]
        if tk not in pt_results:
            pt_results[tk] = {}
        pt_results[tk][pt_level] = int(row["yes_price"])

print(f"  Found {sum(1 for v in pt_results.values() if 200 in v)} markets hitting 200% PT")
print(f"  Found {sum(1 for v in pt_results.values() if 100 in v)} markets hitting 100% PT")


# ── Simulate strategies ───────────────────────────────────────────────

def simulate_strategy(profit_take_mult, pt_level_key, strategy_name):
    """
    profit_take_mult: None, 3.0, or 2.0
    pt_level_key: None, 200, or 100
    """
    bankroll = STARTING_BANKROLL
    peak = STARTING_BANKROLL
    max_dd = 0.0
    total_trades = 0
    total_wins = 0
    profit_take_hits = 0
    total_pnl = 0.0
    daily_count = {}
    traded = set()

    sport_stats = defaultdict(lambda: {
        "trades": 0, "wins": 0, "pnl": 0.0, "wagered": 0.0,
        "pt_hits": 0,
    })

    for t in tradeable:
        if bankroll < 2:
            break

        ticker = t["ticker"]
        d = t["date"]
        sport = t["sport"]
        result = t["result"]
        no_cost = t["no_cost"]
        ka = t["kelly_adj"]

        if daily_count.get(d, 0) >= DAILY_CAP:
            continue
        if ticker in traded:
            continue

        # Size the bet
        bet = min(bankroll * ka, MAX_BET)
        contracts = max(1, int(bet / no_cost))
        cost = contracts * no_cost
        if cost > bankroll:
            contracts = max(1, int(bankroll / no_cost))
            cost = contracts * no_cost
        if cost > bankroll or contracts < 1:
            continue

        no_cost_cents = round(no_cost * 100)
        entry_fee = kalshi_fee(no_cost_cents) * contracts

        # Check profit-take
        pnl = None
        hit_pt = False

        if profit_take_mult is not None and pt_level_key is not None:
            pt_info = pt_results.get(ticker, {})
            if pt_level_key in pt_info:
                # Profit-take triggered!
                cross_yes = pt_info[pt_level_key]
                sell_no_cents = 100 - cross_yes
                sell_no = sell_no_cents / 100.0
                exit_fee = kalshi_fee(sell_no_cents) * contracts
                revenue = contracts * sell_no
                pnl = revenue - cost - entry_fee - exit_fee
                hit_pt = True

        if pnl is None:
            # Hold to settlement
            if result == "no":
                pnl = contracts * (1.0 - no_cost) - entry_fee
            else:
                pnl = -(cost + entry_fee)

        bankroll += pnl
        total_pnl += pnl
        total_trades += 1
        daily_count[d] = daily_count.get(d, 0) + 1
        traded.add(ticker)

        is_win = pnl > 0
        if is_win:
            total_wins += 1
        if hit_pt:
            profit_take_hits += 1

        if bankroll > peak:
            peak = bankroll
        dd = (peak - bankroll) / peak if peak > 0 else 0
        if dd > max_dd:
            max_dd = dd

        ss = sport_stats[sport]
        ss["trades"] += 1
        ss["wagered"] += cost
        ss["pnl"] += pnl
        if is_win:
            ss["wins"] += 1
        if hit_pt:
            ss["pt_hits"] += 1

    return {
        "name": strategy_name,
        "profit_take_mult": profit_take_mult,
        "total_trades": total_trades,
        "total_wins": total_wins,
        "profit_take_hits": profit_take_hits,
        "total_pnl": total_pnl,
        "win_rate": total_wins / max(total_trades, 1),
        "avg_pnl": total_pnl / max(total_trades, 1),
        "max_drawdown": max_dd,
        "final_bankroll": bankroll,
        "sport_stats": dict(sport_stats),
    }


# ── Run all three strategies ──────────────────────────────────────────

print(f"\n{'='*95}")
print("RUNNING THREE STRATEGIES...")
print("=" * 95)

results = []
for mult, key, name in [
    (None, None, "A: Hold to Settlement"),
    (3.0,  200,  "B: 200% Profit-Take"),
    (2.0,  100,  "C: 100% Profit-Take"),
]:
    r = simulate_strategy(mult, key, name)
    results.append(r)
    print(f"  {name}: {r['total_trades']} trades, P&L ${r['total_pnl']:+,.2f}, final ${r['final_bankroll']:,.2f}")


# ── Print comparison ──────────────────────────────────────────────────

print(f"\n{'='*95}")
print("STRATEGY COMPARISON")
print("=" * 95)

hdr = f"  {'Metric':<25s}"
for r in results:
    hdr += f" | {r['name']:>22s}"
print(hdr)
print("  " + "-" * (25 + 3 * 25))

rows = [
    ("Total Trades",       [f"{r['total_trades']:>20,}" for r in results]),
    ("Profit-Take Hits",   [f"{r['profit_take_hits']:>20,}" if r['profit_take_mult'] else f"{'N/A':>20s}" for r in results]),
    ("PT Hit Rate",        [f"{r['profit_take_hits']/max(r['total_trades'],1)*100:>19.1f}%" if r['profit_take_mult'] else f"{'N/A':>20s}" for r in results]),
    ("Win Rate",           [f"{r['win_rate']*100:>19.1f}%" for r in results]),
    ("Avg P&L / Trade",    [f"${r['avg_pnl']:>+19,.4f}" for r in results]),
    ("Total P&L",          [f"${r['total_pnl']:>+19,.2f}" for r in results]),
    ("Max Drawdown",       [f"{r['max_drawdown']*100:>19.1f}%" for r in results]),
    ("Final Bankroll",     [f"${r['final_bankroll']:>19,.2f}" for r in results]),
]

for label, vals in rows:
    line = f"  {label:<25s}"
    for v in vals:
        line += f" | {v:>22s}"
    print(line)


# ── Per-sport profit-take analysis ─────────────────────────────────────

print(f"\n{'='*95}")
print("PER-SPORT PROFIT-TAKE TRIGGER RATES")
print("=" * 95)

strat_a = results[0]
strat_b = results[1]
strat_c = results[2]

all_sports = sorted(set(
    list(strat_a["sport_stats"].keys()) +
    list(strat_b["sport_stats"].keys()) +
    list(strat_c["sport_stats"].keys())
))

print(f"\n  {'Sport':<10s} | {'Trades':>6s} | {'200% Hits':>9s} | {'200% Rate':>9s} | {'100% Hits':>9s} | {'100% Rate':>9s} | {'Hold P&L':>12s} | {'200% P&L':>12s} | {'100% P&L':>12s}")
print("  " + "-" * 115)

for sp in all_sports:
    sa = strat_a["sport_stats"].get(sp, {"trades": 0, "pnl": 0.0})
    sb = strat_b["sport_stats"].get(sp, {"trades": 0, "pt_hits": 0, "pnl": 0.0})
    sc = strat_c["sport_stats"].get(sp, {"trades": 0, "pt_hits": 0, "pnl": 0.0})
    t = sa.get("trades", 0) or sb.get("trades", 0)
    if t == 0:
        continue
    b_hits = sb.get("pt_hits", 0)
    c_hits = sc.get("pt_hits", 0)
    b_rate = b_hits / t * 100 if t > 0 else 0
    c_rate = c_hits / t * 100 if t > 0 else 0
    print(f"  {sp:<10s} | {t:>6d} | {b_hits:>9d} | {b_rate:>8.1f}% | {c_hits:>9d} | {c_rate:>8.1f}% | ${sa['pnl']:>+10,.2f} | ${sb['pnl']:>+10,.2f} | ${sc['pnl']:>+10,.2f}")


# ── Detailed per-strategy sport breakdown ──────────────────────────────

for r in results:
    print(f"\n{'='*95}")
    print(f"SPORT BREAKDOWN: {r['name']}")
    print("=" * 95)
    extra = " | {'PT Hits':>7s} | {'PT Rate':>7s}" if r["profit_take_mult"] else ""
    print(f"  {'Sport':<10s} | {'Trades':>6s} | {'Wins':>5s} | {'WR':>6s} | {'Wagered':>10s} | {'P&L':>12s} | {'ROI':>7s}", end="")
    if r["profit_take_mult"]:
        print(f" | {'PT Hits':>7s} | {'PT Rate':>7s}", end="")
    print()
    print("  " + "-" * (85 if r["profit_take_mult"] else 65))

    for sp in sorted(r["sport_stats"].keys(), key=lambda s: -r["sport_stats"][s]["pnl"]):
        s = r["sport_stats"][sp]
        if s["trades"] == 0:
            continue
        wr = s["wins"] / s["trades"] * 100
        roi = s["pnl"] / s["wagered"] * 100 if s["wagered"] > 0 else 0
        line = f"  {sp:<10s} | {s['trades']:>6d} | {s['wins']:>5d} | {wr:>5.1f}% | ${s['wagered']:>8,.2f} | ${s['pnl']:>+10,.2f} | {roi:>+6.1f}%"
        if r["profit_take_mult"]:
            pt_rate = s["pt_hits"] / s["trades"] * 100 if s["trades"] > 0 else 0
            line += f" | {s['pt_hits']:>7d} | {pt_rate:>6.1f}%"
        print(line)


elapsed = time.time() - t0
print(f"\n{'='*95}")
print(f"DONE in {elapsed:.1f}s")
print("=" * 95)
