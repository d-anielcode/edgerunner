"""
Final backtest: Current live strategy with all improvements.
Per-price Kelly for NBA/NHL, NBA volume filter, 95c cap, $100 max bet.
$100 start, recent data only (2025+), hold to settlement.
"""
import duckdb
from collections import defaultdict

con = duckdb.connect()
mp = "data/trevorjs/markets-*.parquet"
tp = "data/trevorjs/trades-*.parquet"

# Current EDGE_TABLES (for non-NBA/NHL sports)
EDGE_TABLES = {
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

SPORT_PARAMS = {
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


def per_price_yes_rate(sport, yes_p):
    """Per-price linear model for NBA/NHL."""
    if sport == "NBA":
        return max(0.20, 0.50 - (yes_p - 60) * 0.004)
    if sport == "NHL":
        return max(0.30, 0.55 - (yes_p - 60) * 0.003)
    return None


# Load ALL sports
sport_patterns = {
    "NBA": "KXNBAGAME%", "NHL": "KXNHLGAME%",
    "EPL": "KXEPLGAME%", "UCL": "KXUCLGAME%", "LALIGA": "KXLALIGAGAME%",
    "WNBA": "KXWNBAGAME%", "UFC": "KXUFCFIGHT%",
    "NCAAMB": "KXNCAAMBGAME%", "NCAAWB": "KXNCAAWBGAME%",
    "WTA": "KXWTAMATCH%",
    "W1": "KXHIGHNY%", "W2": "KXHIGHCHI%", "W3": "KXHIGHMIA%",
    "W4": "KXHIGHLA%", "W5": "KXHIGHSF%", "W6": "KXHIGHHOU%",
    "W7": "KXHIGHDEN%", "W8": "KXHIGHDC%", "W9": "KXHIGHDAL%",
    "NFLTD": "KXNFLANYTD%",
}
sport_map = {k: ("WEATHER" if k.startswith("W") and len(k) <= 2 else k) for k in sport_patterns}
case_stmts = " ".join(f"WHEN event_ticker LIKE '{p}' THEN '{k}'" for k, p in sport_patterns.items())
like_clauses = " OR ".join(f"event_ticker LIKE '{p}'" for p in sport_patterns.values())

print("Loading recent data (2025+)...")
all_trades = con.sql(f"""
    WITH gm AS (
        SELECT ticker, result, event_ticker, volume,
               CASE {case_stmts} END as sk
        FROM '{mp}' WHERE ({like_clauses}) AND status='finalized' AND result IN ('yes','no')
    ),
    ft AS (
        SELECT t.ticker, t.yes_price, t.no_price, t.created_time,
               CAST(t.created_time AS DATE) as trade_date,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
        FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM gm) AND t.created_time >= '2025-01-01'
    )
    SELECT ft.*, gm.result, gm.sk, gm.volume
    FROM ft JOIN gm ON ft.ticker = gm.ticker WHERE ft.rn=1 AND gm.sk IS NOT NULL
    ORDER BY ft.created_time
""").fetchdf()
all_trades["sport"] = all_trades["sk"].map(sport_map)

print(f"Loaded {len(all_trades)} markets\n")

# === SIMULATION ===
bankroll = 100.0
peak = 100.0
max_dd = 0.0
min_br = 100.0
total_t = 0
total_w = 0
dc = 0
ld = None
traded = set()
monthly = {}
mt = defaultdict(lambda: {"t": 0, "w": 0, "pnl": 0.0, "sbr": 0})
sport_stats = defaultdict(lambda: {"t": 0, "w": 0, "pnl": 0.0, "wagered": 0.0})

for _, row in all_trades.iterrows():
    d = str(row["trade_date"])
    mo = d[:7]
    t = row["ticker"]
    yp = int(row["yes_price"])
    np_ = int(row["no_price"])
    sp = row["sport"]
    res = row["result"]
    vol = float(row["volume"])

    if d != ld:
        if ld: monthly[ld[:7]] = bankroll
        dc = 0
        ld = d
    if mt[mo]["sbr"] == 0:
        mt[mo]["sbr"] = bankroll
    if dc >= 15 or bankroll < 2 or t in traded:
        continue

    # NHL playoff veto
    m_num = int(d[5:7])
    d_num = int(d[8:10])
    if sp == "NHL" and ((m_num > 4 or (m_num == 4 and d_num > 16)) and m_num < 10):
        continue

    # NBA volume filter: skip 500K-2M
    if sp == "NBA" and 500_000 <= vol <= 2_000_000:
        continue

    # Price range (95c for NBA/NHL/WEATHER/CPI/NFLTD, 90c for others)
    min_price = 55 if sp in ("WEATHER", "NFLTD") else 61
    max_price = 95 if sp in ("WEATHER", "NFLTD", "NBA", "NHL") else 90
    if yp < min_price or yp > max_price:
        continue

    # Edge lookup: per-price for NBA/NHL, buckets for others
    pp = per_price_yes_rate(sp, yp)
    if pp is not None:
        actual_yes = pp
        km = 0.25
        mp_ = 0.12
        me = 0.05 if sp == "NHL" else 0.08
    else:
        params = SPORT_PARAMS.get(sp)
        if not params:
            continue
        et = EDGE_TABLES.get(sp, {})
        actual_yes = None
        for (lo, hi), rate in et.items():
            if lo <= yp <= hi:
                actual_yes = rate
                break
        if actual_yes is None:
            continue
        km = params["kelly_mult"]
        mp_ = params["max_position"]
        me = params["min_edge"]

    edge = (yp / 100.0) - actual_yes
    if edge < me:
        continue

    # Fee-adjusted edge
    no_cost = np_ / 100.0
    fee = 0.07 * no_cost * (1 - no_cost)
    friction = (fee + 0.005) / no_cost if no_cost > 0 else 0
    if edge - friction < 0.03:
        continue

    # Kelly
    yes_cost = yp / 100.0
    b = yes_cost / no_cost if no_cost > 0 else 0
    pw = 1 - actual_yes
    kr = (b * pw - actual_yes) / b if b > 0 else 0
    ka = max(0, min(kr * km, mp_))
    if ka <= 0:
        continue

    bet = min(bankroll * ka, 100)  # $100 max bet
    contracts = max(1, int(bet / no_cost))
    cost = contracts * no_cost
    if cost > bankroll:
        contracts = max(1, int(bankroll / no_cost))
        cost = contracts * no_cost
    if cost > bankroll:
        continue

    tf = 0.07 * no_cost * (1 - no_cost) * contracts
    if res == "no":
        pnl = contracts * (1.0 - no_cost) - tf
        total_w += 1
        mt[mo]["w"] += 1
        sport_stats[sp]["w"] += 1
    else:
        pnl = -(cost + tf)

    bankroll += pnl
    total_t += 1
    dc += 1
    traded.add(t)
    mt[mo]["t"] += 1
    mt[mo]["pnl"] += pnl
    sport_stats[sp]["t"] += 1
    sport_stats[sp]["pnl"] += pnl
    sport_stats[sp]["wagered"] += cost

    if bankroll > peak: peak = bankroll
    if bankroll < min_br: min_br = bankroll
    dd = (peak - bankroll) / peak if peak > 0 else 0
    if dd > max_dd: max_dd = dd

if ld:
    monthly[ld[:7]] = bankroll

wr = total_w / max(total_t, 1) * 100

print("=" * 90)
print("FINAL BACKTEST: CURRENT LIVE STRATEGY")
print("Per-price Kelly (NBA/NHL), volume filter, 95c cap, $100 max bet")
print(f"Period: {min(monthly.keys())} to {max(monthly.keys())}")
print("=" * 90)
print(f"""
  Starting:       $100.00
  Final:          ${bankroll:>12,.2f}
  Total P&L:      ${bankroll-100:>+12,.2f}
  Trades:         {total_t:>12,}
  Win rate:       {wr:>11.1f}%
  Max drawdown:   {max_dd*100:>11.1f}%
  Lowest balance: ${min_br:>11.2f}
""")

print("SPORT BREAKDOWN (sorted by P&L):")
print(f"  {'Sport':10s} | {'Trades':>6s} | {'Wins':>5s} | {'WR':>6s} | {'Wagered':>10s} | {'P&L':>12s} | {'ROI':>7s}")
print("  " + "-" * 70)
for sp, s in sorted(sport_stats.items(), key=lambda x: -x[1]["pnl"]):
    if s["t"] > 0:
        swr = s["w"] / s["t"] * 100
        roi = s["pnl"] / s["wagered"] * 100 if s["wagered"] > 0 else 0
        print(f"  {sp:10s} | {s['t']:>6d} | {s['w']:>5d} | {swr:>5.1f}% | ${s['wagered']:>8,.2f} | ${s['pnl']:>+10,.2f} | {roi:>+6.1f}%")

print()
print("MONTHLY PROGRESSION:")
print(f"  {'Month':>7s} | {'Bankroll':>12s} | {'P&L':>10s} | {'Return':>8s} | {'Trades':>6s} | {'WR':>5s}")
print("  " + "-" * 65)
for mo in sorted(monthly.keys()):
    m = mt[mo]
    br = monthly[mo]
    ret = m["pnl"] / max(m["sbr"], 1) * 100
    mwr = m["w"] / max(m["t"], 1) * 100
    print(f"  {mo:>7s} | ${br:>10,.2f} | ${m['pnl']:>+8,.2f} | {ret:>+7.1f}% | {m['t']:>6d} | {mwr:>4.0f}%")

print()
print("MILESTONES:")
for target in [200, 500, 1000, 5000, 10000, 25000, 50000, 100000]:
    for mo in sorted(monthly.keys()):
        if monthly[mo] >= target:
            print(f"  ${target:>7,} hit in {mo}")
            break
    else:
        print(f"  ${target:>7,} not reached")

# Annualize
months = len(monthly)
if months > 0 and bankroll > 100:
    capped = [(mo, m) for mo, m in mt.items() if monthly.get(mo, 0) > 1000]
    if capped:
        avg_pnl = sum(m["pnl"] for _, m in capped) / len(capped)
        print(f"\n  Avg monthly P&L (once >$1K): ${avg_pnl:>+,.2f}")
        cur = bankroll
        for m_name in ["Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]:
            cur += avg_pnl
        print(f"  End of 2026 projection: ~${cur:>,.2f}")

print("\n" + "=" * 90)
