"""
REALISTIC BACKTEST — Simulates actual live agent constraints.

Differences from primary backtest:
1. 3c slippage on entry (first-trade price is 1-2 days early, real price ~3c worse)
2. $1 minimum bet (live agent rejects <$1)
3. Max 25 concurrent positions (live agent limit)
4. 60% max exposure (concentration gate)
5. Position tracking (can't re-enter same ticker)
6. Double fees on profit-take exits (entry + exit fee)
7. Start at $180 (current actual bankroll)
8. Realistic daily trade limit (15/day — agent polls every ~60s, evaluates ~200 tickers)
9. Spread cost: 2c additional slippage on profit-take exits
"""
import math, time, duckdb
from collections import defaultdict

t0 = time.time()

# ═══ CONFIG — EXACT LIVE AGENT CONSTRAINTS ═══
STARTING_BANKROLL = 180.0  # Current actual bankroll
MAX_BET = 200.0
KELLY_SCALE = 0.50
APRIL_REDUCTION = 0.5
DAILY_CAP = 15
MIN_BET = 1.00  # $1 minimum bet
MAX_POSITIONS = 25
MAX_EXPOSURE_PCT = 0.60
ENTRY_SLIPPAGE_CENTS = 3  # YES price is 3c lower at game time vs first-trade
EXIT_SLIPPAGE_CENTS = 2   # Spread cost when selling
MIN_TRADES_PER_MARKET = 20

SP = {
    "NBA": {"km": 0.04, "mp": 0.03, "me": 0.15},
    "NHL": {"km": 0.15, "mp": 0.08, "me": 0.12},
    "EPL": {"km": 0.25, "mp": 0.10, "me": 0.10},
    "UCL": {"km": 0.12, "mp": 0.06, "me": 0.08},
    "LALIGA": {"km": 0.08, "mp": 0.04, "me": 0.15},
    "WNBA": {"km": 0.15, "mp": 0.08, "me": 0.08},
    "UFC": {"km": 0.06, "mp": 0.03, "me": 0.12},
    "NCAAMB": {"km": 0.10, "mp": 0.05, "me": 0.08},
    "NCAAWB": {"km": 0.12, "mp": 0.06, "me": 0.08},
    "ATP": {"km": 0.12, "mp": 0.06, "me": 0.08},
    "WTA": {"km": 0.10, "mp": 0.05, "me": 0.08},
    "NFLTD": {"km": 0.20, "mp": 0.10, "me": 0.05},
    "NHLSPREAD": {"km": 0.15, "mp": 0.08, "me": 0.05},
    "NBASPREAD": {"km": 0.06, "mp": 0.03, "me": 0.12},
    "NFLSPREAD": {"km": 0.06, "mp": 0.03, "me": 0.12},
    "MLB": {"km": 0.06, "mp": 0.03, "me": 0.12},
    "MLBTOTAL": {"km": 0.15, "mp": 0.08, "me": 0.05},
    "NFLGW": {"km": 0.12, "mp": 0.06, "me": 0.08},
    "NFLTT": {"km": 0.10, "mp": 0.05, "me": 0.08},
    "CBA": {"km": 0.12, "mp": 0.06, "me": 0.08},
    "LIGUE1": {"km": 0.10, "mp": 0.05, "me": 0.08},
    "LOL": {"km": 0.12, "mp": 0.06, "me": 0.08},
    "ATPCH": {"km": 0.10, "mp": 0.05, "me": 0.08},
}

ET = {
    "EPL": {(71, 85): 0.485}, "UCL": {(66, 70): 0.400, (76, 85): 0.641},
    "LALIGA": {(81, 90): 0.588},
    "WNBA": {(55, 62): 0.380, (71, 77): 0.550, (83, 87): 0.540},
    "UFC": {(76, 85): 0.622},
    "NCAAMB": {(66, 70): 0.536, (71, 80): 0.656, (82, 90): 0.770},
    "NCAAWB": {(61, 70): 0.600, (71, 80): 0.680, (81, 85): 0.750},
    "ATP": {(71, 75): 0.650, (76, 80): 0.654, (81, 85): 0.765},
    "WTA": {(76, 79): 0.695, (80, 84): 0.803, (85, 90): 0.790},
    "NFLTD": {(55, 65): 0.492, (66, 75): 0.452, (76, 85): 0.545, (86, 95): 0.286},
    "NHLSPREAD": {(55, 65): 0.500, (66, 75): 0.450, (76, 90): 0.400},
    "NBASPREAD": {(55, 65): 0.480, (66, 75): 0.440, (76, 90): 0.380},
    "NFLSPREAD": {(55, 65): 0.480, (66, 75): 0.440, (76, 90): 0.380},
    "MLB": {(76, 84): 0.640},
    "MLBTOTAL": {(55, 65): 0.500, (66, 75): 0.480, (76, 85): 0.450},
    "NFLGW": {(55, 65): 0.520, (66, 75): 0.580, (76, 90): 0.650},
    "NFLTT": {(55, 65): 0.500, (66, 75): 0.480, (76, 85): 0.450},
    "CBA": {(55, 65): 0.500, (66, 75): 0.550, (76, 85): 0.620},
    "LIGUE1": {(55, 65): 0.480, (66, 75): 0.500, (76, 85): 0.550},
    "LOL": {(55, 65): 0.500, (66, 75): 0.520, (76, 85): 0.550},
    "ATPCH": {(55, 65): 0.520, (66, 75): 0.550, (76, 85): 0.620},
}

SPORT_PT = {
    "EPL": 100, "NBA": 150, "NBASPREAD": 150, "NCAAMB": 100,
    "NFLSPREAD": 200, "NFLTD": 100, "NHL": 100, "NHLSPREAD": 300,
    "UCL": 100, "UFC": 200, "WNBA": 100, "ATP": 100, "WTA": 150,
    "CFB": 200, "MLB": 50, "LALIGA": 200,
    "MLBTOTAL": 100, "NFLGW": 100, "NFLTT": 150, "CBA": 100,
    "LIGUE1": 100, "LOL": 100, "ATPCH": 50,
}

LOW_EDGE = ("NFLTD", "MLBTOTAL", "NFLGW", "NFLTT", "CBA", "LIGUE1", "LOL", "ATPCH")

PFX = {
    "KXNBAGAME": "NBA", "KXNHLGAME": "NHL", "KXEPLGAME": "EPL",
    "KXUCLGAME": "UCL", "KXLALIGAGAME": "LALIGA", "KXWNBAGAME": "WNBA",
    "KXUFCFIGHT": "UFC", "KXNCAAMBGAME": "NCAAMB", "KXNCAAWBGAME": "NCAAWB",
    "KXATPCHALLENGERMATCH": "ATPCH", "KXATPMATCH": "ATP", "KXWTAMATCH": "WTA",
    "KXNFLANYTD": "NFLTD", "KXNHLSPREAD": "NHLSPREAD", "KXNBASPREAD": "NBASPREAD",
    "KXNFLSPREAD": "NFLSPREAD", "KXMLBGAME": "MLB", "KXMLBTOTAL": "MLBTOTAL",
    "KXNFLGAME": "NFLGW", "KXNFLTEAMTOTAL": "NFLTT",
    "KXCBAGAME": "CBA", "KXLIGUE": "LIGUE1", "KXLOLMAP": "LOL",
}

def pp(s, yp):
    if s == "NBA": return max(0.20, 0.50 - (yp - 60) * 0.004)
    if s == "NHL": return max(0.30, 0.55 - (yp - 60) * 0.003)
    return None

def fee(c):
    p = c / 100.0
    return math.ceil(0.07 * p * (1 - p) * 100) / 100

def get_ay(s, yp):
    r = pp(s, yp)
    if r: return r
    for (lo, hi), v in ET.get(s, {}).items():
        if lo <= yp <= hi:
            return v[0] if isinstance(v, tuple) else v
    return None

# ═══ LOAD DATA ═══
con = duckdb.connect()
mp = "data/trevorjs/markets-*.parquet"; tp = "data/trevorjs/trades-*.parquet"
cp = " ".join(f"WHEN event_ticker LIKE '{p}%' THEN '{s}'" for p, s in PFX.items())
lk = " OR ".join(f"event_ticker LIKE '{p}%'" for p in PFX.keys())

print("Loading data...")
mdf = con.sql(f"""
    WITH gm AS (SELECT ticker,result,event_ticker,volume,CASE {cp} END as sport
        FROM '{mp}' WHERE ({lk}) AND status='finalized' AND result IN ('yes','no')),
    ts AS (SELECT ticker,MIN(created_time) as ft,COUNT(*) as tc FROM '{tp}'
        WHERE ticker IN (SELECT ticker FROM gm) AND created_time>='2025-01-01'
        GROUP BY ticker HAVING COUNT(*)>={MIN_TRADES_PER_MARKET}),
    ft AS (SELECT t.ticker,t.yes_price,CAST(t.created_time AS DATE) as td
        FROM '{tp}' t JOIN ts ON t.ticker=ts.ticker AND t.created_time=ts.ft)
    SELECT ft.ticker,ft.yes_price,ft.td,gm.result,gm.sport,gm.volume
    FROM ft JOIN gm ON ft.ticker=gm.ticker JOIN ts ON ft.ticker=ts.ticker
    WHERE gm.sport IS NOT NULL ORDER BY ft.td
""").fetchdf()

# Apply slippage: YES price is LOWER at game time (favorite weakens toward fair)
# This means NO cost is HIGHER (worse for us)
markets = []
for _, row in mdf.iterrows():
    raw_yp = int(row["yes_price"])
    # Slippage: YES drifts down 3c by game time
    yp = max(1, raw_yp - ENTRY_SLIPPAGE_CENTS)
    s = row["sport"]; d = str(row["td"]); vol = float(row["volume"])
    mn = int(d[5:7]); dn = int(d[8:10])
    if s == "NHL" and ((mn > 4 or (mn == 4 and dn > 16)) and mn < 10): continue
    if s == "NBA" and ((mn > 4 or (mn == 4 and dn > 19)) and mn < 10): continue
    if s == "NBA" and 500000 <= vol <= 2000000: continue
    min_p = 55 if s in LOW_EDGE else 61
    if yp < min_p or yp > 95: continue
    ay = get_ay(s, yp)
    if not ay: continue
    p = SP.get(s)
    if not p: continue
    edge = (yp / 100.0) - ay
    nc = 100 - yp; noc = nc / 100.0
    f = 0.07 * noc * (1 - noc); fr = (f + 0.005) / noc if noc > 0 else 0
    if edge - fr < p["me"] or edge < 0.03: continue
    b = (yp / 100.0) / noc if noc > 0 else 0
    kr = (b * (1 - ay) - ay) / b if b > 0 else 0
    markets.append({"tk": row["ticker"], "yp": yp, "raw_yp": raw_yp, "sp": s, "d": d,
        "res": row["result"], "mn": mn, "nc": nc, "noc": noc, "kr": kr})

print(f"  {len(markets)} tradeable (after 3c slippage)")

# Load PT crossings (using RAW yes_price, not slippage-adjusted)
tks = list(set(m["tk"] for m in markets))
con.execute("CREATE TEMP TABLE tks (ticker VARCHAR)")
for t in tks: con.execute("INSERT INTO tks VALUES (?)", [t])

print("Loading trajectories...")
min_df = con.sql(f"""
    WITH ft AS (SELECT ticker, MIN(created_time) as ft FROM '{tp}'
        WHERE ticker IN (SELECT ticker FROM tks) AND created_time >= '2025-01-01' GROUP BY ticker)
    SELECT t.ticker, MIN(t.yes_price) as min_yes
    FROM '{tp}' t JOIN ft ON t.ticker = ft.ticker
    WHERE t.created_time > ft.ft AND t.created_time >= '2025-01-01'
    GROUP BY t.ticker
""").fetchdf()
min_yes_map = {row["ticker"]: int(row["min_yes"]) for _, row in min_df.iterrows()}

ptx = {}
for pt_pct in [50, 100, 150, 200, 300]:
    mult = 1 + pt_pct / 100.0
    cands = [(m["tk"], int(100 - mult * m["nc"])) for m in markets if int(100 - mult * m["nc"]) > 0 and min_yes_map.get(m["tk"], 999) <= int(100 - mult * m["nc"])]
    if not cands: continue
    con.execute(f"DROP TABLE IF EXISTS ptc{pt_pct}")
    con.execute(f"CREATE TEMP TABLE ptc{pt_pct} (ticker VARCHAR, ym INTEGER)")
    for tk, ym in cands: con.execute(f"INSERT INTO ptc{pt_pct} VALUES (?, ?)", [tk, ym])
    cdf = con.sql(f"""
        WITH ft AS (SELECT ticker, MIN(created_time) as ft FROM '{tp}'
            WHERE ticker IN (SELECT ticker FROM ptc{pt_pct}) AND created_time >= '2025-01-01' GROUP BY ticker),
        cr AS (SELECT t.ticker, t.yes_price, ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
            FROM '{tp}' t JOIN ft ON t.ticker = ft.ticker JOIN ptc{pt_pct} c ON t.ticker = c.ticker
            WHERE t.created_time > ft.ft AND t.created_time >= '2025-01-01' AND t.yes_price <= c.ym)
        SELECT ticker, yes_price FROM cr WHERE rn = 1
    """).fetchdf()
    for _, r in cdf.iterrows():
        ptx.setdefault(r["ticker"], {})[pt_pct] = int(r["yes_price"])

print(f"  Done ({time.time()-t0:.1f}s)")

# ═══ SIMULATE WITH FULL CONSTRAINTS ═══
print("\n" + "=" * 80)
print("REALISTIC BACKTEST — Live Agent Constraints")
print(f"Start: ${STARTING_BANKROLL} | Slippage: {ENTRY_SLIPPAGE_CENTS}c entry + {EXIT_SLIPPAGE_CENTS}c exit")
print(f"Min bet: ${MIN_BET} | Max positions: {MAX_POSITIONS} | Max exposure: {MAX_EXPOSURE_PCT:.0%}")
print(f"Kelly: {KELLY_SCALE}x | Daily cap: {DAILY_CAP}")
print("=" * 80)

br = STARTING_BANKROLL; pk = STARTING_BANKROLL; mdd = 0.0
tt = 0; tw = 0; pnls = []; ld = None; dc = 0; lm = None
monthly = {}; sport_stats = defaultdict(lambda: {"t": 0, "w": 0, "pnl": 0.0, "wag": 0.0})
open_positions = {}  # ticker -> cost
rejected_minbet = 0; rejected_maxpos = 0; rejected_exposure = 0; pt_exits = 0

for m in sorted(markets, key=lambda x: x["d"]):
    s = m["sp"]; p = SP[s]
    pt_pct = SPORT_PT.get(s, 150)

    # Month tracking
    cm = m["d"][:7]
    if cm != lm:
        if lm: monthly[lm] = br
        lm = cm

    # Daily cap
    if m["d"] != ld: ld = m["d"]; dc = 0
    if dc >= DAILY_CAP: continue

    # Position limit
    if len(open_positions) >= MAX_POSITIONS:
        rejected_maxpos += 1
        continue

    # Duplicate check
    if m["tk"] in open_positions:
        continue

    # Kelly sizing
    km = p["km"] * KELLY_SCALE; mp_ = p["mp"] * KELLY_SCALE
    if m["mn"] == 4: km *= APRIL_REDUCTION; mp_ *= APRIL_REDUCTION
    ka = max(0.0, min(m["kr"] * km, mp_))
    if ka <= 0: continue

    bet = min(br * ka, MAX_BET)
    noc = m["noc"]; nc = m["nc"]
    contracts = max(1, int(bet / noc))
    cost = contracts * noc

    # Min bet check
    if cost < MIN_BET:
        rejected_minbet += 1
        continue

    # Exposure check
    total_exposure = sum(open_positions.values()) + cost
    if total_exposure > br * MAX_EXPOSURE_PCT:
        rejected_exposure += 1
        continue

    if cost > br: continue

    ef = fee(nc) * contracts
    dc += 1; tt += 1
    open_positions[m["tk"]] = cost
    sport_stats[s]["wag"] += cost

    # Check profit-take
    pt_trig = False
    cross = ptx.get(m["tk"], {}).get(pt_pct)
    if cross is not None:
        # Exit slippage: actual exit price is 2c worse
        exit_yes = cross + EXIT_SLIPPAGE_CENTS
        exit_noc = max(0.01, (100 - exit_yes) / 100.0)
        xf = fee(100 - exit_yes) * contracts
        pnl = contracts * (exit_noc - noc) - ef - xf
        pt_trig = True
        pt_exits += 1
        if pnl > 0: tw += 1; sport_stats[s]["w"] += 1

    if not pt_trig:
        if m["res"] == "no":
            pnl = contracts * (1.0 - noc) - ef
            tw += 1; sport_stats[s]["w"] += 1
        else:
            pnl = -(cost + ef)

    # Remove from open positions (simplified: assume same-day settlement)
    del open_positions[m["tk"]]

    br += pnl; pnls.append(pnl)
    sport_stats[s]["t"] += 1; sport_stats[s]["pnl"] += pnl
    if br > pk: pk = br
    dd = (pk - br) / pk if pk > 0 else 0
    if dd > mdd: mdd = dd
    if br < 2: break

if lm: monthly[lm] = br

avg = sum(pnls) / len(pnls) if pnls else 0
std = (sum((p2 - avg) ** 2 for p2 in pnls) / len(pnls)) ** 0.5 if len(pnls) > 1 else 1
sharpe = avg / std if std > 0 else 0

# ═══ OUTPUT ═══
print(f"\n  Starting:    ${STARTING_BANKROLL:.2f}")
print(f"  Final:       ${br:>14,.2f}")
print(f"  Total P&L:   ${br - STARTING_BANKROLL:>14,.2f}")
print(f"  Trades:      {tt:>14,}")
print(f"  Win rate:    {tw/tt:>13.1%}" if tt > 0 else "")
print(f"  Max DD:      {mdd:>13.1%}")
print(f"  Sharpe:      {sharpe:>13.3f}")
print(f"  PT exits:    {pt_exits:>14,} ({pt_exits/tt*100:.0f}% of trades)" if tt > 0 else "")

print(f"\n  Rejections:")
print(f"    Min bet ($1):     {rejected_minbet:>6}")
print(f"    Max positions:    {rejected_maxpos:>6}")
print(f"    Max exposure:     {rejected_exposure:>6}")

print(f"\n  MONTHLY:")
prev = STARTING_BANKROLL
for mo in sorted(monthly.keys()):
    b = monthly[mo]; ch = b - prev; pct = (ch / prev * 100) if prev > 0 else 0
    marker = "" if ch >= 0 else " <-- LOSS"
    print(f"    {mo} | ${b:>12,.2f} | ${ch:>+10,.2f} | {pct:>+6.1f}%{marker}")
    prev = b

print(f"\n  PER-SPORT (ranked by P&L):")
print(f"    {'Sport':<12} {'Trades':>6} {'WR':>5} {'P&L':>10} {'ROI':>7} {'PT':>5}")
print(f"    {'-'*50}")
for s, ss in sorted(sport_stats.items(), key=lambda x: -x[1]["pnl"]):
    if ss["t"] == 0: continue
    wr = ss["w"] / ss["t"]
    roi = (ss["pnl"] / ss["wag"] * 100) if ss["wag"] > 0 else 0
    pt_str = f"{SPORT_PT.get(s, '?')}%"
    print(f"    {s:<12} {ss['t']:>6} {wr:>4.0%} ${ss['pnl']:>9,.0f} {roi:>+6.1f}% {pt_str:>5}")

# Milestones
print(f"\n  MILESTONES:")
cum = STARTING_BANKROLL
milestones = [500, 1000, 5000, 10000, 50000, 100000]
hit = set()
for i, p2 in enumerate(pnls):
    cum += p2
    for m in milestones:
        if cum >= m and m not in hit:
            hit.add(m)
            print(f"    ${m:>7,} reached at trade #{i+1}")

print(f"\n  Time: {time.time()-t0:.1f}s")
