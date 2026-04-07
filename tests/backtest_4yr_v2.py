"""
Full 4-year backtest v2: Updated params, comprehensive stats.
$100 start, daily compounding, hold to settlement, one trade per ticker.
"""
import duckdb
from collections import defaultdict

con = duckdb.connect()
mp = "data/trevorjs/markets-*.parquet"
tp = "data/trevorjs/trades-*.parquet"

# === CURRENT Edge tables (after optimization) ===
EDGE_TABLES = {
    "NBA":    {(61, 75): 0.608, (76, 90): 0.719},
    "NHL":    {(61, 75): 0.545, (76, 90): 0.563},
    "EPL":    {(71, 85): 0.485},
    "UCL":    {(66, 70): 0.400, (76, 85): 0.641},
    "LALIGA": {(81, 90): 0.588},
    "WNBA":   {(61, 65): 0.559, (71, 75): 0.596, (81, 90): 0.735},
    "UFC":    {(76, 85): 0.622},
    "NCAAMB": {(61, 70): 0.579, (71, 80): 0.656},  # Removed 81-90c
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
    "NCAAMB": {"kelly_mult": 0.10, "max_position": 0.05, "min_edge": 0.08},
    "NCAAWB": {"kelly_mult": 0.12, "max_position": 0.06, "min_edge": 0.08},
    "WTA":    {"kelly_mult": 0.08, "max_position": 0.04, "min_edge": 0.10},
    "WEATHER":{"kelly_mult": 0.25, "max_position": 0.10, "min_edge": 0.10},
    "CPI":    {"kelly_mult": 0.08, "max_position": 0.04, "min_edge": 0.15},
    "NFLTD":  {"kelly_mult": 0.20, "max_position": 0.10, "min_edge": 0.05},
}

# Build SQL
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

sport_map = {}
for k in sport_patterns:
    if k.startswith("WEATHER"): sport_map[k] = "WEATHER"
    elif k.startswith("CPI"): sport_map[k] = "CPI"
    else: sport_map[k] = k

case_stmts = " ".join(f"WHEN event_ticker LIKE '{p}' THEN '{k}'" for k, p in sport_patterns.items())
like_clauses = " OR ".join(f"event_ticker LIKE '{p}'" for p in sport_patterns.values())

print("Loading trades...")
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
        FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM game_markets)
    )
    SELECT ft.*, gm.result, gm.sport_key, gm.volume
    FROM first_trades ft JOIN game_markets gm ON ft.ticker = gm.ticker
    WHERE ft.rn = 1 AND gm.sport_key IS NOT NULL
    ORDER BY ft.created_time
""").fetchdf()

all_trades["sport"] = all_trades["sport_key"].map(sport_map)
print(f"Loaded {len(all_trades)} markets\n")

# === SIMULATION ===
bankroll = 100.0
peak = 100.0
max_dd = 0.0
min_br = 100.0
total_trades = 0
total_wins = 0
daily_trades_count = 0
last_date = None
max_daily = 15

sport_stats = defaultdict(lambda: {"trades": 0, "wins": 0, "losses": 0, "pnl": 0.0,
                                    "total_wagered": 0.0, "biggest_win": 0.0, "biggest_loss": 0.0})
monthly = {}
yearly = {}
traded_tickers = set()
trade_log = []

# Streak tracking
current_streak = 0
max_win_streak = 0
max_lose_streak = 0
cur_win_streak = 0
cur_lose_streak = 0

# Daily P&L tracking
daily_pnl_list = []
day_start_br = 100.0

for _, row in all_trades.iterrows():
    date = str(row["trade_date"])
    ticker = row["ticker"]
    yes_p = int(row["yes_price"])
    no_p = int(row["no_price"])
    sport = row["sport"]
    result = row["result"]
    year = date[:4]

    if date != last_date:
        if last_date:
            monthly[last_date[:7]] = bankroll
            daily_pnl_list.append({"date": last_date, "pnl": bankroll - day_start_br,
                                   "bankroll": bankroll, "trades": daily_trades_count})
        daily_trades_count = 0
        day_start_br = bankroll
        last_date = date

    if daily_trades_count >= max_daily or bankroll < 2:
        continue
    if ticker in traded_tickers:
        continue

    # NHL playoff veto
    if sport == "NHL":
        mo = int(date[5:7]); dy = int(date[8:10])
        if (mo > 4 or (mo == 4 and dy > 16)) and mo < 10:
            continue

    params = SPORT_PARAMS.get(sport, SPORT_PARAMS["NBA"])
    edge_table = EDGE_TABLES.get(sport, {})

    min_price = 55 if sport in ("WEATHER", "CPI", "NFLTD") else 61
    max_price = 95 if sport in ("WEATHER", "CPI", "NFLTD") else 90

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
    if edge < params["min_edge"]:
        continue

    no_cost = no_p / 100.0
    fee = 0.07 * no_cost * (1 - no_cost)
    friction = (fee + 0.005) / no_cost if no_cost > 0 else 0
    if edge - friction < 0.03:
        continue

    yes_cost = yes_p / 100.0
    b = yes_cost / no_cost if no_cost > 0 else 0
    p_win = 1 - actual_yes
    q_lose = actual_yes
    kelly_raw = (b * p_win - q_lose) / b if b > 0 else 0
    kelly_adj = max(0, min(kelly_raw * params["kelly_mult"], params["max_position"]))
    if kelly_adj <= 0:
        continue

    contracts = max(1, int(bankroll * kelly_adj / no_cost))
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
        cur_win_streak += 1
        cur_lose_streak = 0
        if cur_win_streak > max_win_streak:
            max_win_streak = cur_win_streak
    else:
        pnl = -(cost + total_fee)
        sport_stats[sport]["losses"] += 1
        cur_lose_streak += 1
        cur_win_streak = 0
        if cur_lose_streak > max_lose_streak:
            max_lose_streak = cur_lose_streak

    bankroll += pnl
    total_trades += 1
    daily_trades_count += 1
    traded_tickers.add(ticker)
    sport_stats[sport]["trades"] += 1
    sport_stats[sport]["pnl"] += pnl
    sport_stats[sport]["total_wagered"] += cost
    if pnl > sport_stats[sport]["biggest_win"]:
        sport_stats[sport]["biggest_win"] = pnl
    if pnl < sport_stats[sport]["biggest_loss"]:
        sport_stats[sport]["biggest_loss"] = pnl

    if bankroll > peak: peak = bankroll
    if bankroll < min_br: min_br = bankroll
    dd = (peak - bankroll) / peak if peak > 0 else 0
    if dd > max_dd: max_dd = dd

    trade_log.append({"date": date, "sport": sport, "yes_p": yes_p,
                       "pnl": pnl, "bankroll": bankroll, "result": "W" if result == "no" else "L"})

if last_date:
    monthly[last_date[:7]] = bankroll
    daily_pnl_list.append({"date": last_date, "pnl": bankroll - day_start_br,
                           "bankroll": bankroll, "trades": daily_trades_count})

# Year-end snapshots
for mo, br in sorted(monthly.items()):
    yr = mo[:4]
    yearly[yr] = br

# === REPORT ===
print("=" * 95)
print("4-YEAR BACKTEST v2: OPTIMIZED PARAMS, ALL 13 MARKETS")
dates = sorted(monthly.keys())
print(f"Period: {dates[0]} to {dates[-1]} ({len(dates)} months)")
print("=" * 95)

wr = total_wins / max(total_trades, 1) * 100
print(f"""
  Starting bankroll:    $100.00
  Final bankroll:       ${bankroll:>18,.2f}
  Total trades:         {total_trades:>18,}
  Win rate:             {wr:>17.1f}%
  Max drawdown:         {max_dd*100:>17.1f}%
  Lowest balance:       ${min_br:>17.2f}
  Max win streak:       {max_win_streak:>18}
  Max lose streak:      {max_lose_streak:>18}
  Avg trades/day:       {total_trades/max(len(daily_pnl_list),1):>17.1f}
""")

# Year-end balances
print("YEAR-END BALANCES:")
prev_yr_br = 100.0
for yr in sorted(yearly.keys()):
    br = yearly[yr]
    yr_return = (br - prev_yr_br) / prev_yr_br * 100
    print(f"  End of {yr}: ${br:>18,.2f}  ({yr_return:>+8.1f}% that year)")
    prev_yr_br = br

# Sport breakdown sorted by ROI
print("\nSPORT BREAKDOWN (sorted by ROI):")
print(f"  {'Sport':10s} | {'Trades':>6s} | {'W':>4s} | {'L':>4s} | {'WR':>6s} | {'Wagered':>12s} | {'P&L':>14s} | {'ROI':>7s} | {'BigW':>8s} | {'BigL':>9s}")
print("  " + "-" * 100)

sport_list = []
for sport, s in sport_stats.items():
    if s["trades"] > 0:
        roi = s["pnl"] / s["total_wagered"] * 100 if s["total_wagered"] > 0 else 0
        sport_list.append((sport, s, roi))

for sport, s, roi in sorted(sport_list, key=lambda x: -x[2]):
    swr = s["wins"] / s["trades"] * 100
    wager_s = f"${s['total_wagered']:>10,.2f}" if s['total_wagered'] < 1e9 else f"${s['total_wagered']:>.1e}"
    pnl_s = f"${s['pnl']:>+12,.2f}" if abs(s['pnl']) < 1e9 else f"${s['pnl']:>+.2e}"
    bw = f"${s['biggest_win']:>7,.2f}" if s['biggest_win'] < 1e7 else f"${s['biggest_win']:>.1e}"
    bl = f"${s['biggest_loss']:>8,.2f}" if abs(s['biggest_loss']) < 1e7 else f"${s['biggest_loss']:>.1e}"
    print(f"  {sport:10s} | {s['trades']:>6d} | {s['wins']:>4d} | {s['losses']:>4d} | {swr:>5.1f}% | {wager_s} | {pnl_s} | {roi:>+6.1f}% | {bw} | {bl}")

# Monthly progression
print("\nMONTHLY BANKROLL:")
max_val = max(monthly.values())
for mo in sorted(monthly.keys()):
    val = monthly[mo]
    bar = "#" * min(50, max(1, int(val / max_val * 50)))
    if val < 1e9:
        print(f"  {mo}: ${val:>16,.2f}  {bar}")
    else:
        print(f"  {mo}: ${val:>16.2e}  {bar}")

# Daily P&L stats
print("\nDAILY P&L STATISTICS:")
pnls = [d["pnl"] for d in daily_pnl_list if d["trades"] > 0]
win_days = [p for p in pnls if p > 0]
lose_days = [p for p in pnls if p < 0]
zero_days = len(daily_pnl_list) - len(pnls)

print(f"  Trading days:     {len(pnls)}")
print(f"  No-trade days:    {zero_days}")
print(f"  Winning days:     {len(win_days)} ({len(win_days)/max(len(pnls),1)*100:.0f}%)")
print(f"  Losing days:      {len(lose_days)} ({len(lose_days)/max(len(pnls),1)*100:.0f}%)")
if win_days:
    print(f"  Avg win day:      ${sum(win_days)/len(win_days):>+.2f}")
    print(f"  Best day:         ${max(win_days):>+,.2f}")
if lose_days:
    print(f"  Avg lose day:     ${sum(lose_days)/len(lose_days):>+.2f}")
    print(f"  Worst day:        ${min(lose_days):>+,.2f}")

# Winning/losing months
print("\nMONTHLY WIN/LOSS:")
prev_br = 100.0
win_months = 0
lose_months = 0
for mo in sorted(monthly.keys()):
    br = monthly[mo]
    if br > prev_br: win_months += 1
    elif br < prev_br: lose_months += 1
    prev_br = br
print(f"  Winning months: {win_months}/{len(monthly)} ({win_months/max(len(monthly),1)*100:.0f}%)")
print(f"  Losing months:  {lose_months}/{len(monthly)} ({lose_months/max(len(monthly),1)*100:.0f}%)")

# Biggest single trades
print("\nTOP 10 BIGGEST WINS:")
top_wins = sorted(trade_log, key=lambda x: -x["pnl"])[:10]
for t in top_wins:
    print(f"  {t['date']} | {t['sport']:10s} | YES={t['yes_p']}c | P&L ${t['pnl']:>+14,.2f} | BR ${t['bankroll']:>14,.2f}")

print("\nTOP 10 BIGGEST LOSSES:")
top_losses = sorted(trade_log, key=lambda x: x["pnl"])[:10]
for t in top_losses:
    print(f"  {t['date']} | {t['sport']:10s} | YES={t['yes_p']}c | P&L ${t['pnl']:>+14,.2f} | BR ${t['bankroll']:>14,.2f}")

# Calendar coverage
print("\nCALENDAR COVERAGE:")
month_sports = defaultdict(set)
for t in trade_log:
    mo = int(t["date"][5:7])
    month_sports[mo].add(t["sport"])
month_names = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",
               7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}
for m in range(1, 13):
    sports = sorted(month_sports.get(m, set()))
    count = sum(1 for t in trade_log if int(t["date"][5:7]) == m)
    print(f"  {month_names[m]:3s}: {count:>4d} trades | {', '.join(sports) if sports else '-- none --'}")

print("\n" + "=" * 95)
