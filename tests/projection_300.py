"""Your projection: $300 start, $100 max bet, recent data only."""
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
    "NFLTD":  {"kelly_mult": 0.20, "max_position": 0.10, "min_edge": 0.05},
}

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

bankroll = 300.0
peak = 300.0
max_dd = 0.0
min_br = 300.0
total_t = 0
total_w = 0
dc = 0
ld = None
traded = set()
monthly = {}
mt = defaultdict(lambda: {"t": 0, "w": 0, "pnl": 0.0, "sbr": 0})
sm = defaultdict(lambda: defaultdict(lambda: {"t": 0, "w": 0, "pnl": 0.0}))

for _, row in all_trades.iterrows():
    d = str(row["trade_date"]); mo = d[:7]; t = row["ticker"]
    yp = int(row["yes_price"]); np_ = int(row["no_price"])
    sp = row["sport"]; res = row["result"]

    if d != ld:
        if ld: monthly[ld[:7]] = bankroll
        dc = 0; ld = d
    if mt[mo]["sbr"] == 0: mt[mo]["sbr"] = bankroll
    if dc >= 15 or bankroll < 2 or t in traded: continue
    if sp == "NHL":
        m = int(d[5:7]); dy = int(d[8:10])
        if (m > 4 or (m == 4 and dy > 16)) and m < 10: continue

    p = SPORT_PARAMS.get(sp, SPORT_PARAMS["NBA"])
    et = EDGE_TABLES.get(sp, {})
    mnp = 55 if sp in ("WEATHER", "NFLTD") else 61
    mxp = 95 if sp in ("WEATHER", "NFLTD") else 90
    if yp < mnp or yp > mxp: continue

    ay = None
    for (lo, hi), rate in et.items():
        if lo <= yp <= hi: ay = rate; break
    if ay is None: continue
    edge = (yp / 100.0) - ay
    if edge < p["min_edge"]: continue

    nc = np_ / 100.0
    fee = 0.07 * nc * (1 - nc)
    friction = (fee + 0.005) / nc if nc > 0 else 0
    if edge - friction < 0.03: continue

    yc = yp / 100.0
    b = yc / nc if nc > 0 else 0
    kr = (b * (1-ay) - ay) / b if b > 0 else 0
    ka = max(0, min(kr * p["kelly_mult"], p["max_position"]))
    if ka <= 0: continue

    bet = min(bankroll * ka, 100)
    contracts = max(1, int(bet / nc))
    cost = contracts * nc
    if cost > bankroll:
        contracts = max(1, int(bankroll / nc)); cost = contracts * nc
    if cost > bankroll: continue

    tf = 0.07 * nc * (1 - nc) * contracts
    if res == "no":
        pnl = contracts * (1.0 - nc) - tf
        total_w += 1; mt[mo]["w"] += 1; sm[sp][mo]["w"] += 1
    else:
        pnl = -(cost + tf)

    bankroll += pnl; total_t += 1; dc += 1; traded.add(t)
    mt[mo]["t"] += 1; mt[mo]["pnl"] += pnl
    sm[sp][mo]["t"] += 1; sm[sp][mo]["pnl"] += pnl
    if bankroll > peak: peak = bankroll
    if bankroll < min_br: min_br = bankroll
    dd = (peak - bankroll) / peak if peak > 0 else 0
    if dd > max_dd: max_dd = dd

if ld: monthly[ld[:7]] = bankroll

wr = total_w / max(total_t, 1) * 100
print("=" * 100)
print("YOUR PROJECTION: $300 start ($114 + $200), $100 max bet")
print("=" * 100)
print(f"  Starting:    $300.00")
print(f"  Final:       ${bankroll:>12,.2f}")
print(f"  Total P&L:   ${bankroll-300:>+12,.2f}")
print(f"  Trades:      {total_t}")
print(f"  Win rate:    {wr:.1f}%")
print(f"  Max DD:      {max_dd*100:.1f}%")
print(f"  Lowest:      ${min_br:.2f}")
print()

print("MONTHLY BREAKDOWN:")
print(f"  {'Month':>7s} | {'Bankroll':>10s} | {'P&L':>10s} | {'Return':>7s} | {'Trades':>6s} | {'W':>3s} | {'WR':>4s} | Top Sport")
print("  " + "-" * 90)

for mo in sorted(monthly.keys()):
    m = mt[mo]
    br = monthly[mo]
    ret = m["pnl"] / max(m["sbr"], 1) * 100
    mwr = m["w"] / max(m["t"], 1) * 100
    # Best sport
    best = ""
    bp = -9e9
    for s in sm:
        if mo in sm[s] and sm[s][mo]["pnl"] > bp:
            bp = sm[s][mo]["pnl"]
            best = f"{s} (${sm[s][mo]['pnl']:+,.0f})"
    print(f"  {mo:>7s} | ${br:>8,.2f} | ${m['pnl']:>+8,.2f} | {ret:>+6.1f}% | {m['t']:>6d} | {m['w']:>3d} | {mwr:>3.0f}% | {best}")

# Most/least profitable
print()
srt = sorted(mt.items(), key=lambda x: -x[1]["pnl"])
print("TOP 3 MONTHS:")
for mo, m in srt[:3]:
    print(f"  {mo}: ${m['pnl']:>+10,.2f} | {m['t']} trades, {m['w']} wins ({m['w']/max(m['t'],1)*100:.0f}%)")
print("WORST 3 MONTHS:")
for mo, m in srt[-3:]:
    print(f"  {mo}: ${m['pnl']:>+10,.2f} | {m['t']} trades, {m['w']} wins ({m['w']/max(m['t'],1)*100:.0f}%)")

# Milestones
print()
print("MILESTONE TRACKER:")
for target in [500, 1000, 5000, 10000, 25000, 50000, 100000]:
    for mo in sorted(monthly.keys()):
        if monthly[mo] >= target:
            print(f"  ${target:>7,} hit in {mo}")
            break
    else:
        print(f"  ${target:>7,} not reached in data period")

# Forward projection
print()
print("FORWARD PROJECTION (Apr-Dec 2026):")
capped_months = [(mo, m) for mo, m in mt.items() if monthly.get(mo, 0) > 1000]
if capped_months:
    avg_pnl = sum(m["pnl"] for _, m in capped_months) / len(capped_months)
    print(f"  Avg monthly P&L (once bankroll >$1K): ${avg_pnl:>+,.2f}")
    cur = bankroll
    for m_num, m_name in [(4,"Apr"),(5,"May"),(6,"Jun"),(7,"Jul"),(8,"Aug"),(9,"Sep"),(10,"Oct"),(11,"Nov"),(12,"Dec")]:
        cur += avg_pnl
        marker = " <-- $100K!" if cur >= 100000 and (cur - avg_pnl) < 100000 else ""
        print(f"    {m_name} 2026: ${cur:>12,.2f}{marker}")
    print(f"\n  End of 2026: ${cur:>12,.2f}")
    if cur >= 100000:
        print("  --> $100K GOAL: ACHIEVABLE")
    else:
        need = (100000 - bankroll) / 9
        print(f"  --> Need ${need:>,.2f}/month to hit $100K by Dec 2026")
else:
    # Use all months
    avg_pnl = sum(m["pnl"] for m in mt.values()) / max(len(mt), 1)
    print(f"  Avg monthly P&L: ${avg_pnl:>+,.2f}")
    cur = bankroll
    for m_num, m_name in [(4,"Apr"),(5,"May"),(6,"Jun"),(7,"Jul"),(8,"Aug"),(9,"Sep"),(10,"Oct"),(11,"Nov"),(12,"Dec")]:
        cur += avg_pnl
        print(f"    {m_name} 2026: ${cur:>12,.2f}")

print()
# Sports season guide
print("WHICH SPORTS DRIVE PROFITS BY SEASON:")
for mo in sorted(monthly.keys()):
    sports_active = []
    for s in sm:
        if mo in sm[s] and sm[s][mo]["t"] > 0:
            sports_active.append((s, sm[s][mo]["pnl"], sm[s][mo]["t"]))
    sports_active.sort(key=lambda x: -x[1])
    top3 = ", ".join(f"{s}(${p:+.0f})" for s, p, _ in sports_active[:3])
    print(f"  {mo}: {top3}")
