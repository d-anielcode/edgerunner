"""
Recent-data-only backtest: Compare Kelly caps, dollar caps, and current params.
Only uses data from 2025-01-01 onward (when most sports have real volume).
Drop CPI (market dead). Keep all other 12.
"""
import duckdb
from collections import defaultdict

con = duckdb.connect()
mp = "data/trevorjs/markets-*.parquet"
tp = "data/trevorjs/trades-*.parquet"

EDGE_TABLES = {
    "NBA":    {(61, 75): 0.608, (76, 90): 0.719},
    "NHL":    {(61, 75): 0.545, (76, 90): 0.563},
    "EPL":    {(71, 85): 0.485},
    "UCL":    {(66, 70): 0.400, (76, 85): 0.641},
    "LALIGA": {(81, 90): 0.588},
    "WNBA":   {(61, 65): 0.559, (71, 75): 0.596, (81, 90): 0.735},
    "UFC":    {(76, 85): 0.622},
    "NCAAMB": {(61, 70): 0.579, (71, 80): 0.656},
    "NCAAWB": {(61, 70): 0.600, (71, 80): 0.680, (81, 90): 0.780},
    "WTA":    {(61, 75): 0.650, (76, 85): 0.680},
    "WEATHER":{(55, 65): 0.404, (66, 75): 0.417, (76, 85): 0.417, (86, 95): 0.419},
    "NFLTD":  {(55, 65): 0.492, (66, 75): 0.452, (76, 85): 0.545, (86, 95): 0.286},
}

# Current params
CURRENT_PARAMS = {
    "NBA":    {"kelly_mult": 0.10, "max_position": 0.05, "min_edge": 0.08},
    "NHL":    {"kelly_mult": 0.30, "max_position": 0.12, "min_edge": 0.05},
    "EPL":    {"kelly_mult": 0.25, "max_position": 0.10, "min_edge": 0.10},
    "UCL":    {"kelly_mult": 0.12, "max_position": 0.06, "min_edge": 0.08},
    "LALIGA": {"kelly_mult": 0.08, "max_position": 0.04, "min_edge": 0.15},
    "WNBA":   {"kelly_mult": 0.12, "max_position": 0.06, "min_edge": 0.05},
    "UFC":    {"kelly_mult": 0.12, "max_position": 0.06, "min_edge": 0.08},
    "NCAAMB": {"kelly_mult": 0.10, "max_position": 0.05, "min_edge": 0.08},
    "NCAAWB": {"kelly_mult": 0.12, "max_position": 0.06, "min_edge": 0.08},
    "WTA":    {"kelly_mult": 0.08, "max_position": 0.04, "min_edge": 0.10},
    "WEATHER":{"kelly_mult": 0.25, "max_position": 0.10, "min_edge": 0.10},
    "NFLTD":  {"kelly_mult": 0.20, "max_position": 0.10, "min_edge": 0.05},
}

# Load RECENT data only (2025+)
sport_patterns = {
    "NBA": "KXNBAGAME%", "NHL": "KXNHLGAME%",
    "EPL": "KXEPLGAME%", "UCL": "KXUCLGAME%", "LALIGA": "KXLALIGAGAME%",
    "WNBA": "KXWNBAGAME%", "UFC": "KXUFCFIGHT%",
    "NCAAMB": "KXNCAAMBGAME%", "NCAAWB": "KXNCAAWBGAME%",
    "WTA": "KXWTAMATCH%",
    "WEATHER_NY": "KXHIGHNY%", "WEATHER_CHI": "KXHIGHCHI%",
    "WEATHER_MIA": "KXHIGHMIA%", "WEATHER_LA": "KXHIGHLA%",
    "WEATHER_SF": "KXHIGHSF%", "WEATHER_HOU": "KXHIGHHOU%",
    "WEATHER_DEN": "KXHIGHDEN%", "WEATHER_DC": "KXHIGHDC%",
    "WEATHER_DAL": "KXHIGHDAL%",
    "NFLTD": "KXNFLANYTD%",
}

sport_map = {}
for k in sport_patterns:
    if k.startswith("WEATHER"): sport_map[k] = "WEATHER"
    else: sport_map[k] = k

case_stmts = " ".join(f"WHEN event_ticker LIKE '{p}' THEN '{k}'" for k, p in sport_patterns.items())
like_clauses = " OR ".join(f"event_ticker LIKE '{p}'" for p in sport_patterns.values())

print("Loading recent data (2025+)...")
all_trades = con.sql(f"""
    WITH game_markets AS (
        SELECT ticker, result, event_ticker, volume,
               CASE {case_stmts} END as sport_key
        FROM '{mp}'
        WHERE ({like_clauses}) AND status = 'finalized' AND result IN ('yes','no')
    ),
    first_trades AS (
        SELECT t.ticker, t.yes_price, t.no_price, t.created_time,
               CAST(t.created_time AS DATE) as trade_date,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
        FROM '{tp}' t
        WHERE t.ticker IN (SELECT ticker FROM game_markets)
              AND t.created_time >= '2025-01-01'
    )
    SELECT ft.*, gm.result, gm.sport_key, gm.volume
    FROM first_trades ft JOIN game_markets gm ON ft.ticker = gm.ticker
    WHERE ft.rn = 1 AND gm.sport_key IS NOT NULL
    ORDER BY ft.created_time
""").fetchdf()

all_trades["sport"] = all_trades["sport_key"].map(sport_map)
print(f"Loaded {len(all_trades)} recent markets")
for s, c in all_trades["sport"].value_counts().items():
    print(f"  {s}: {c}")
print()


def run_backtest(df, params, kelly_scale=1.0, max_bet=None, max_daily=15, label=""):
    bankroll = 100.0
    peak = 100.0
    max_dd = 0.0
    min_br = 100.0
    total_trades = 0
    total_wins = 0
    daily_count = 0
    last_date = None
    traded_tickers = set()
    sport_stats = defaultdict(lambda: {"trades": 0, "wins": 0, "pnl": 0.0, "wagered": 0.0})
    monthly = {}

    for _, row in df.iterrows():
        date = str(row["trade_date"])
        ticker = row["ticker"]
        yes_p = int(row["yes_price"])
        no_p = int(row["no_price"])
        sport = row["sport"]
        result = row["result"]

        if date != last_date:
            if last_date:
                monthly[last_date[:7]] = bankroll
            daily_count = 0
            last_date = date

        if daily_count >= max_daily or bankroll < 2:
            continue
        if ticker in traded_tickers:
            continue

        # NHL playoff veto
        if sport == "NHL":
            mo = int(date[5:7]); dy = int(date[8:10])
            if (mo > 4 or (mo == 4 and dy > 16)) and mo < 10:
                continue

        p = params.get(sport, params.get("NBA"))
        edge_table = EDGE_TABLES.get(sport, {})

        min_price = 55 if sport in ("WEATHER", "NFLTD") else 61
        max_price = 95 if sport in ("WEATHER", "NFLTD") else 90

        if yes_p < min_price or yes_p > max_price:
            continue

        actual_yes = None
        for (lo, hi), rate in edge_table.items():
            if lo <= yes_p <= hi:
                actual_yes = rate
                break
        if actual_yes is None:
            continue

        edge = (yes_p / 100.0) - actual_yes
        if edge < p["min_edge"]:
            continue

        no_cost = no_p / 100.0
        fee = 0.07 * no_cost * (1 - no_cost)
        friction = (fee + 0.005) / no_cost if no_cost > 0 else 0
        if edge - friction < 0.03:
            continue

        yes_cost = yes_p / 100.0
        b = yes_cost / no_cost if no_cost > 0 else 0
        pw = 1 - actual_yes
        ql = actual_yes
        kr = (b * pw - ql) / b if b > 0 else 0
        ka = max(0, min(kr * p["kelly_mult"] * kelly_scale, p["max_position"]))
        if ka <= 0:
            continue

        bet_size = bankroll * ka
        if max_bet and bet_size > max_bet:
            bet_size = max_bet

        contracts = max(1, int(bet_size / no_cost))
        cost = contracts * no_cost
        if cost > bankroll:
            contracts = max(1, int(bankroll / no_cost))
            cost = contracts * no_cost
        if cost > bankroll:
            continue

        total_fee = 0.07 * no_cost * (1 - no_cost) * contracts

        if result == "no":
            pnl = contracts * (1.0 - no_cost) - total_fee
            total_wins += 1
            sport_stats[sport]["wins"] += 1
        else:
            pnl = -(cost + total_fee)

        bankroll += pnl
        total_trades += 1
        daily_count += 1
        traded_tickers.add(ticker)
        sport_stats[sport]["trades"] += 1
        sport_stats[sport]["pnl"] += pnl
        sport_stats[sport]["wagered"] += cost

        if bankroll > peak: peak = bankroll
        if bankroll < min_br: min_br = bankroll
        dd = (peak - bankroll) / peak if peak > 0 else 0
        if dd > max_dd: max_dd = dd

    if last_date:
        monthly[last_date[:7]] = bankroll

    wr = total_wins / max(total_trades, 1) * 100
    return {
        "label": label, "final": bankroll, "trades": total_trades,
        "wins": total_wins, "wr": wr, "max_dd": max_dd * 100,
        "min_br": min_br, "monthly": monthly, "sport_stats": dict(sport_stats),
    }


# ================================================================
# TEST 1: Current params (baseline)
# ================================================================
print("=" * 95)
print("TEST 1: CURRENT PARAMS (baseline)")
print("=" * 95)
r = run_backtest(all_trades, CURRENT_PARAMS, label="Current")
print(f"  Final: ${r['final']:>14,.2f} | Trades: {r['trades']} | WR: {r['wr']:.1f}% | MaxDD: {r['max_dd']:.1f}% | Low: ${r['min_br']:.2f}")

# ================================================================
# TEST 2: Kelly scaling (reduce all Kelly multipliers by a factor)
# ================================================================
print()
print("=" * 95)
print("TEST 2: KELLY SCALING (multiply all Kelly by a factor)")
print("=" * 95)
print()
print(f"  {'Scale':>6s} | {'Final':>16s} | {'Trades':>6s} | {'WR':>5s} | {'MaxDD':>6s} | {'Lowest':>10s}")
print("  " + "-" * 65)

for scale in [0.25, 0.33, 0.50, 0.67, 0.75, 1.0, 1.25, 1.5, 2.0]:
    r = run_backtest(all_trades, CURRENT_PARAMS, kelly_scale=scale, label=f"Kelly x{scale}")
    final_s = f"${r['final']:>14,.2f}" if r['final'] < 1e12 else f"${r['final']:>14.2e}"
    print(f"  x{scale:<5.2f} | {final_s} | {r['trades']:>6d} | {r['wr']:>4.1f}% | {r['max_dd']:>5.1f}% | ${r['min_br']:>9.2f}")

# ================================================================
# TEST 3: Hard dollar cap per trade
# ================================================================
print()
print("=" * 95)
print("TEST 3: HARD DOLLAR CAP PER TRADE (with current Kelly)")
print("=" * 95)
print()
print(f"  {'Cap':>8s} | {'Final':>16s} | {'Trades':>6s} | {'WR':>5s} | {'MaxDD':>6s} | {'Lowest':>10s}")
print("  " + "-" * 65)

for cap in [5, 10, 25, 50, 100, 250, 500, None]:
    r = run_backtest(all_trades, CURRENT_PARAMS, max_bet=cap, label=f"Cap ${cap}" if cap else "No cap")
    final_s = f"${r['final']:>14,.2f}" if r['final'] < 1e12 else f"${r['final']:>14.2e}"
    cap_s = f"${cap:>6d}" if cap else "  None"
    print(f"  {cap_s} | {final_s} | {r['trades']:>6d} | {r['wr']:>4.1f}% | {r['max_dd']:>5.1f}% | ${r['min_br']:>9.2f}")

# ================================================================
# TEST 4: Kelly scaling + dollar cap combos
# ================================================================
print()
print("=" * 95)
print("TEST 4: BEST COMBOS (Kelly scale + dollar cap)")
print("=" * 95)
print()
print(f"  {'Kelly':>6s} {'Cap':>6s} | {'Final':>16s} | {'Trades':>6s} | {'WR':>5s} | {'MaxDD':>6s} | {'Lowest':>10s} | {'ROI':>8s}")
print("  " + "-" * 80)

combos = []
for scale in [0.33, 0.50, 0.67, 0.75, 1.0]:
    for cap in [25, 50, 100, 250, None]:
        r = run_backtest(all_trades, CURRENT_PARAMS, kelly_scale=scale, max_bet=cap)
        roi = (r['final'] - 100) / 100 * 100
        combos.append((scale, cap, r))
        final_s = f"${r['final']:>14,.2f}" if r['final'] < 1e12 else f"${r['final']:>14.2e}"
        cap_s = f"${cap:>4d}" if cap else "None"
        print(f"  x{scale:<4.2f} {cap_s} | {final_s} | {r['trades']:>6d} | {r['wr']:>4.1f}% | {r['max_dd']:>5.1f}% | ${r['min_br']:>9.2f} | {roi:>+7.0f}%")

# ================================================================
# TEST 5: Best config — detailed sport breakdown
# ================================================================
# Find config with best risk-adjusted return (ROI / MaxDD)
best = max(combos, key=lambda x: (x[2]['final'] - 100) / max(x[2]['max_dd'], 1))
print()
print("=" * 95)
print(f"BEST RISK-ADJUSTED CONFIG: Kelly x{best[0]}, Cap ${best[1]}")
print("=" * 95)

r = run_backtest(all_trades, CURRENT_PARAMS, kelly_scale=best[0], max_bet=best[1])
print(f"\n  Final: ${r['final']:>14,.2f}")
print(f"  Trades: {r['trades']} | WR: {r['wr']:.1f}% | MaxDD: {r['max_dd']:.1f}% | Lowest: ${r['min_br']:.2f}")

print(f"\n  SPORT BREAKDOWN:")
print(f"    {'Sport':10s} | {'Trades':>6s} | {'Wins':>5s} | {'WR':>6s} | {'Wagered':>12s} | {'P&L':>12s} | {'ROI':>7s}")
print("    " + "-" * 70)

for sport, s in sorted(r['sport_stats'].items(), key=lambda x: -x[1].get('pnl', 0)):
    if s['trades'] > 0:
        swr = s['wins'] / s['trades'] * 100
        roi = s['pnl'] / s['wagered'] * 100 if s['wagered'] > 0 else 0
        print(f"    {sport:10s} | {s['trades']:>6d} | {s['wins']:>5d} | {swr:>5.1f}% | ${s['wagered']:>10,.2f} | ${s['pnl']:>+10,.2f} | {roi:>+6.1f}%")

print(f"\n  MONTHLY PROGRESSION:")
for mo in sorted(r['monthly'].keys()):
    val = r['monthly'][mo]
    max_val = max(r['monthly'].values())
    bar = "#" * min(50, max(1, int(val / max_val * 50)))
    print(f"    {mo}: ${val:>12,.2f}  {bar}")

# Also show the MOST PROFITABLE config (regardless of risk)
most_profit = max(combos, key=lambda x: x[2]['final'])
print()
print("=" * 95)
print(f"MOST PROFITABLE CONFIG: Kelly x{most_profit[0]}, Cap ${most_profit[1]}")
print("=" * 95)
r2 = most_profit[2]
print(f"  Final: ${r2['final']:>14,.2f}" if r2['final'] < 1e12 else f"  Final: ${r2['final']:>14.2e}")
print(f"  Trades: {r2['trades']} | WR: {r2['wr']:.1f}% | MaxDD: {r2['max_dd']:.1f}% | Lowest: ${r2['min_br']:.2f}")

print()
print("=" * 95)
