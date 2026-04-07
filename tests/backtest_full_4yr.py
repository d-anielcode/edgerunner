"""
Full 4-year backtest: All 13 market types, $100 start, daily compounding.
Hold to settlement (no early sells). One trade per ticker.
"""
import duckdb
from collections import defaultdict

con = duckdb.connect()
mp = "data/trevorjs/markets-*.parquet"
tp = "data/trevorjs/trades-*.parquet"

# === Edge tables (from signals/rules.py) ===
EDGE_TABLES = {
    "NBA":    {(61, 75): 0.608, (76, 90): 0.719},
    "NHL":    {(61, 75): 0.545, (76, 90): 0.563},
    "EPL":    {(71, 85): 0.485},
    "UCL":    {(66, 70): 0.400, (76, 85): 0.641},
    "LALIGA": {(81, 90): 0.588},
    "WNBA":   {(61, 65): 0.559, (71, 75): 0.596, (81, 90): 0.735},
    "UFC":    {(76, 85): 0.622},
    "NCAAMB": {(61, 70): 0.577, (71, 80): 0.654, (81, 90): 0.781},
    "NCAAWB": {(61, 70): 0.600, (71, 80): 0.680, (81, 90): 0.780},
    "WTA":    {(61, 75): 0.650, (76, 85): 0.680},
    "WEATHER":{(55, 65): 0.404, (66, 75): 0.417, (76, 85): 0.417, (86, 95): 0.419},
    "CPI":    {(55, 75): 0.591, (76, 90): 0.703, (91, 95): 0.873},
    "NFLTD":  {(55, 65): 0.492, (66, 75): 0.452, (76, 85): 0.545, (86, 95): 0.286},
}

SPORT_PARAMS = {
    "NBA":    {"kelly_mult": 0.10, "max_position": 0.05, "min_edge": 0.08},
    "NHL":    {"kelly_mult": 0.30, "max_position": 0.12, "min_edge": 0.05},
    "EPL":    {"kelly_mult": 0.25, "max_position": 0.10, "min_edge": 0.10},
    "UCL":    {"kelly_mult": 0.12, "max_position": 0.06, "min_edge": 0.08},
    "LALIGA": {"kelly_mult": 0.08, "max_position": 0.04, "min_edge": 0.15},
    "WNBA":   {"kelly_mult": 0.12, "max_position": 0.06, "min_edge": 0.05},
    "UFC":    {"kelly_mult": 0.12, "max_position": 0.06, "min_edge": 0.08},
    "NCAAMB": {"kelly_mult": 0.15, "max_position": 0.08, "min_edge": 0.08},
    "NCAAWB": {"kelly_mult": 0.12, "max_position": 0.06, "min_edge": 0.08},
    "WTA":    {"kelly_mult": 0.12, "max_position": 0.06, "min_edge": 0.08},
    "WEATHER":{"kelly_mult": 0.25, "max_position": 0.10, "min_edge": 0.10},
    "CPI":    {"kelly_mult": 0.15, "max_position": 0.08, "min_edge": 0.08},
    "NFLTD":  {"kelly_mult": 0.20, "max_position": 0.10, "min_edge": 0.05},
}

# Build sport mappings
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
    "CPI_1": "CPI-%", "CPI_2": "CPICORE-%", "CPI_3": "CPICOREYOY-%",
    "NFLTD": "KXNFLANYTD%",
}

# Map sub-categories to main sport
sport_map = {}
for k in sport_patterns:
    if k.startswith("WEATHER"):
        sport_map[k] = "WEATHER"
    elif k.startswith("CPI"):
        sport_map[k] = "CPI"
    else:
        sport_map[k] = k

case_stmts = " ".join(f"WHEN event_ticker LIKE '{p}' THEN '{k}'" for k, p in sport_patterns.items())
like_clauses = " OR ".join(f"event_ticker LIKE '{p}'" for p in sport_patterns.values())

print("Loading all trades...")
all_trades = con.sql(f"""
    WITH game_markets AS (
        SELECT ticker, result, event_ticker, volume,
               CASE {case_stmts} END as sport_key
        FROM '{mp}'
        WHERE ({like_clauses})
              AND status = 'finalized' AND result IN ('yes','no')
    ),
    first_trades AS (
        SELECT t.ticker, t.yes_price, t.no_price, t.created_time,
               CAST(t.created_time AS DATE) as trade_date,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
        FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM game_markets)
    )
    SELECT ft.*, gm.result, gm.sport_key, gm.volume
    FROM first_trades ft JOIN game_markets gm ON ft.ticker = gm.ticker
    WHERE ft.rn = 1 AND gm.sport_key IS NOT NULL
    ORDER BY ft.created_time
""").fetchdf()

# Map to main sport
all_trades["sport"] = all_trades["sport_key"].map(sport_map)

print(f"Loaded {len(all_trades)} markets")
for s, c in all_trades["sport"].value_counts().items():
    print(f"  {s}: {c}")
print()

# === SIMULATION ===
bankroll = 100.0
peak = 100.0
max_dd = 0.0
min_br = 100.0
total_trades = 0
total_wins = 0
daily_trades_count = 0
last_date = None
max_daily = 15  # More markets now, raise daily cap

sport_stats = defaultdict(lambda: {"trades": 0, "wins": 0, "pnl": 0.0})
monthly = {}
traded_tickers = set()  # One trade per ticker

for _, row in all_trades.iterrows():
    date = str(row["trade_date"])
    ticker = row["ticker"]
    yes_p = int(row["yes_price"])
    no_p = int(row["no_price"])
    sport = row["sport"]
    result = row["result"]

    if date != last_date:
        if last_date:
            monthly[last_date[:7]] = bankroll
        daily_trades_count = 0
        last_date = date

    if daily_trades_count >= max_daily or bankroll < 2:
        continue

    # One trade per ticker
    if ticker in traded_tickers:
        continue

    # NHL playoff veto
    if sport == "NHL":
        mo = int(date[5:7])
        dy = int(date[8:10])
        if (mo > 4 or (mo == 4 and dy > 16)) and mo < 10:
            continue

    # Get params
    params = SPORT_PARAMS.get(sport, SPORT_PARAMS["NBA"])
    edge_table = EDGE_TABLES.get(sport, {})

    # Min price (weather/CPI/NFLTD start at 55c)
    min_price = 55 if sport in ("WEATHER", "CPI", "NFLTD") else 61
    max_price = 95 if sport in ("WEATHER", "CPI", "NFLTD") else 90

    if yes_p < min_price or yes_p > max_price:
        continue

    # Edge lookup
    actual_yes = None
    for (lo, hi), rate in edge_table.items():
        if lo <= yes_p <= hi:
            actual_yes = rate
            break
    if actual_yes is None:
        continue

    edge = (yes_p / 100.0) - actual_yes
    if edge < params["min_edge"]:
        continue

    # Fee-adjusted edge
    no_cost = no_p / 100.0
    fee = 0.07 * no_cost * (1 - no_cost)
    slippage = 0.005
    friction = (fee + slippage) / no_cost if no_cost > 0 else 0
    net_edge = edge - friction
    if net_edge < 0.03:
        continue

    # Kelly sizing
    yes_cost = yes_p / 100.0
    b = yes_cost / no_cost if no_cost > 0 else 0
    p = 1 - actual_yes
    q = actual_yes
    kelly_raw = (b * p - q) / b if b > 0 else 0
    kelly_adj = max(0, min(kelly_raw * params["kelly_mult"], params["max_position"]))
    if kelly_adj <= 0:
        continue

    bet_size = bankroll * kelly_adj
    contracts = max(1, int(bet_size / no_cost))
    cost = contracts * no_cost
    if cost > bankroll:
        contracts = max(1, int(bankroll / no_cost))
        cost = contracts * no_cost
    if cost > bankroll:
        continue

    # Execute (hold to settlement)
    total_fee = 0.07 * no_cost * (1 - no_cost) * contracts
    pnl = 0.0
    if result == "no":
        pnl = contracts * (1.0 - no_cost) - total_fee
        total_wins += 1
        sport_stats[sport]["wins"] += 1
    else:
        pnl = -(cost + total_fee)

    bankroll += pnl
    total_trades += 1
    daily_trades_count += 1
    traded_tickers.add(ticker)
    sport_stats[sport]["trades"] += 1
    sport_stats[sport]["pnl"] += pnl

    if bankroll > peak:
        peak = bankroll
    if bankroll < min_br:
        min_br = bankroll
    dd = (peak - bankroll) / peak if peak > 0 else 0
    if dd > max_dd:
        max_dd = dd

if last_date:
    monthly[last_date[:7]] = bankroll

# === REPORT ===
print("=" * 90)
print("4-YEAR BACKTEST: ALL 13 MARKET TYPES")
dates = sorted(monthly.keys())
print(f"Period: {dates[0]} to {dates[-1]} ({len(dates)} months)")
print("=" * 90)
print()

wr = total_wins / max(total_trades, 1) * 100
print(f"Starting:         $100.00")
print(f"Final:            ${bankroll:>14,.2f}")
print(f"Total trades:     {total_trades:>14,}")
print(f"Win rate:         {wr:>13.1f}%")
print(f"Max drawdown:     {max_dd * 100:>13.1f}%")
print(f"Lowest balance:   ${min_br:>13.2f}")
print()

# Sport breakdown
print("SPORT BREAKDOWN:")
print(f"  {'Sport':10s} | {'Trades':>7s} | {'Wins':>5s} | {'WR':>6s} | {'P&L':>14s}")
print("  " + "-" * 55)
sorted_sports = sorted(sport_stats.items(), key=lambda x: -x[1]["pnl"])
for sport, s in sorted_sports:
    if s["trades"] > 0:
        swr = s["wins"] / s["trades"] * 100
        print(f"  {sport:10s} | {s['trades']:>7d} | {s['wins']:>5d} | {swr:>5.1f}% | ${s['pnl']:>+13,.2f}")

# Monthly
print()
print("MONTHLY BANKROLL:")
max_val = max(monthly.values()) if monthly else 1
for mo in sorted(monthly.keys()):
    val = monthly[mo]
    bar = "#" * min(50, max(1, int(val / max_val * 50)))
    print(f"  {mo}: ${val:>14,.2f}  {bar}")

# Annualize
months = len(monthly)
if months > 0 and bankroll > 100:
    monthly_g = (bankroll / 100) ** (1 / months)
    annual = 100 * (monthly_g ** 12)
    print(f"\n  Avg monthly growth: {(monthly_g - 1) * 100:.1f}%")
    print(f"  12-month projection: ~${annual:>14,.2f}")

# Calendar coverage
print()
print("CALENDAR COVERAGE:")
month_sports = defaultdict(set)
for _, row in all_trades.iterrows():
    d = str(row["trade_date"])
    mo = int(d[5:7])
    month_sports[mo].add(row["sport"])

month_names = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",
               7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}
for m in range(1, 13):
    sports = sorted(month_sports.get(m, set()))
    print(f"  {month_names[m]:3s}: {', '.join(sports) if sports else '-- none --'}")

# Daily stats
daily_pnl = []
running = 100.0
prev = 100.0
for mo in sorted(monthly.keys()):
    daily_pnl.append(monthly[mo] - prev)
    prev = monthly[mo]

win_months = sum(1 for p in daily_pnl if p > 0)
lose_months = sum(1 for p in daily_pnl if p < 0)
print(f"\n  Winning months: {win_months}/{len(daily_pnl)}")
print(f"  Losing months:  {lose_months}/{len(daily_pnl)}")

print()
print("=" * 90)
