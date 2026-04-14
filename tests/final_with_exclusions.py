"""
FINAL SIMULATION — with per-bucket exclusions for negative edge ranges.
Compares: Current agent vs Optimized (with exclusions) vs Optimized (without)
"""
import duckdb, math, time

t0 = time.time()
con = duckdb.connect()
mp = 'data/trevorjs/markets-*.parquet'
tp = 'data/trevorjs/trades-*.parquet'

ALL_PREFIXES = {
    "KXNBAGAME": "NBA", "KXNHLGAME": "NHL", "KXUCLGAME": "UCL",
    "KXWNBAGAME": "WNBA", "KXATPMATCH": "ATP", "KXNFLANYTD": "NFLTD",
    "KXNHLSPREAD": "NHLSPREAD", "KXNBASPREAD": "NBASPREAD",
    "KXNFLSPREAD": "NFLSPREAD", "KXMLBGAME": "MLB",
    "KXNFLTEAMTOTAL": "NFLTT", "KXCFBGAME": "CFB",
    "KXNBAPTS": "NBA_PTS", "KXNBA3PT": "NBA_3PT",
    "KXNBAREB": "NBA_REB", "KXNBAAST": "NBA_AST",
    "KXNCAAMBGAME": "NCAAMB", "KXNCAAWBGAME": "NCAAWB",
    "KXWTAMATCH": "WTA", "KXNFLGAME": "NFLGW",
    "KXNFLFIRSTTD": "NFL_1ST_TD", "KXNHLGOAL": "NHL_GOAL",
    "KXNHLAST": "NHL_AST", "KXNHLPTS": "NHL_PTS",
    "KXNBASTL": "NBA_STL", "KXNFLRECYDS": "NFL_REC_YDS",
    "KXNCAAFTOTAL": "NCAAF_TOTAL", "KXCS": "CS2",
    "KXMLSGAME": "MLS", "KXEUROLEAGUEGAME": "EUROLEAGUE",
    "KXLOLGAME": "LOL_GAME", "KXDARTSMATCH": "DARTS",
    "KXEREDIVISIEGAME": "EREDIVISIE",
}

ALL_EDGE_TABLES = {
    "UCL": {(66, 70): 0.400, (76, 85): 0.641},
    "WNBA": {(55, 62): 0.380, (71, 77): 0.550, (83, 87): 0.540},
    "ATP": {(71, 75): 0.650, (76, 80): 0.654, (81, 85): 0.765},
    "NFLTD": {(55, 65): 0.492, (66, 75): 0.452, (76, 85): 0.545, (86, 95): 0.286},
    "NHLSPREAD": {(55, 65): 0.500, (66, 75): 0.450, (76, 90): 0.400},
    "NBASPREAD": {(55, 65): 0.480, (66, 75): 0.440, (76, 90): 0.380},
    "NFLSPREAD": {(55, 65): 0.480, (66, 75): 0.440, (76, 90): 0.380},
    "MLB": {(76, 84): 0.640},
    "NFLTT": {(55, 65): 0.500, (66, 75): 0.480, (76, 85): 0.450},
    "CFB": {(55, 65): 0.520, (66, 75): 0.550, (76, 85): 0.620},
    "NBA_3PT": {(55, 64): 0.497, (65, 74): 0.594, (75, 84): 0.707, (85, 95): 0.771},
    "NBA_PTS": {(55, 64): 0.538, (65, 74): 0.657, (75, 84): 0.736, (85, 95): 0.765},
    "NBA_REB": {(55, 64): 0.574, (65, 74): 0.629, (75, 84): 0.701, (85, 95): 0.864},
    "NBA_AST": {(55, 64): 0.582, (65, 74): 0.644, (75, 84): 0.747, (85, 95): 0.827},
    "NCAAMB": {(66, 70): 0.536, (71, 80): 0.656, (82, 90): 0.770},
    "NCAAWB": {(61, 70): 0.600, (71, 80): 0.680, (81, 85): 0.750},
    "WTA": {(76, 79): 0.695, (80, 84): 0.803, (85, 90): 0.790},
    "NFLGW": {(55, 65): 0.520, (66, 75): 0.580, (76, 90): 0.650},
    "NFL_1ST_TD": {(55, 95): 0.000},
    "NHL_GOAL": {(85, 95): 0.410},
    "NHL_AST": {(55, 65): 0.412, (66, 75): 0.308},
    "NHL_PTS": {(55, 65): 0.497, (66, 75): 0.585, (76, 85): 0.714, (85, 95): 0.850},
    "NBA_STL": {(55, 65): 0.563, (66, 75): 0.435, (85, 95): 0.200},
    "NFL_REC_YDS": {(55, 65): 0.430, (66, 75): 0.584, (76, 85): 0.640, (85, 95): 0.748},
    "NCAAF_TOTAL": {(55, 65): 0.500, (66, 75): 0.480, (76, 85): 0.450},
    "CS2": {(55, 65): 0.548, (66, 75): 0.630, (76, 85): 0.694, (85, 95): 0.704},
    "MLS": {(55, 65): 0.543, (66, 75): 0.622, (76, 85): 0.633},
    "EUROLEAGUE": {(55, 65): 0.530, (66, 75): 0.580, (76, 85): 0.700},
    "LOL_GAME": {(55, 65): 0.520, (66, 75): 0.560, (76, 85): 0.620},
    "DARTS": {(55, 65): 0.400, (76, 85): 0.522},
    "EREDIVISIE": {(55, 65): 0.429, (66, 75): 0.500, (76, 85): 0.500},
}

# Negative edge buckets to EXCLUDE: (sport, yes_lo, yes_hi)
EXCLUDE_BUCKETS = {
    ("NFLTD", 55, 64),     # 11.9% NO WR, est loss -$24
    ("NFLSPREAD", 55, 64), # 34.4% NO WR, est loss -$19
    ("NFLSPREAD", 65, 74), # 28.1% NO WR, est loss -$6
    ("NCAAMB", 65, 74),    # 28.5% NO WR, est loss -$10
    ("NCAAMB", 75, 84),    # 18.7% NO WR, est loss -$9
    ("MLB", 75, 84),       # 18.5% NO WR, est loss -$8
    ("MLB", 55, 64),       # 40.0% NO WR, barely negative
    ("NFLGW", 55, 64),     # 34.8% NO WR, est loss -$4
    ("NFLGW", 65, 74),     # 28.8% NO WR
    ("NFLGW", 85, 95),     # 8.8% NO WR
    ("WTA", 55, 64),       # 38.7% NO WR, est loss -$7
    ("WTA", 75, 84),       # 20.1% NO WR
    ("UCL", 65, 74),       # 24.0% NO WR
    ("UCL", 55, 64),       # 40.0% NO WR, barely negative
    ("DARTS", 65, 74),     # 25.0% NO WR
    ("LOL_GAME", 75, 84),  # 17.9% NO WR
    ("LOL_GAME", 85, 95),  # 3.6% NO WR
    ("LOL_GAME", 55, 64),  # 39.4% NO WR, barely negative
    ("WNBA", 65, 74),      # 30.2% NO WR
    ("NBA_STL", 75, 84),   # 13.3% NO WR
}

OPTIMIZED_PARAMS = {
    "NFL_1ST_TD": {"km": 0.25, "mp": 0.12, "me": 0.05},
    "NHL_GOAL": {"km": 0.20, "mp": 0.10, "me": 0.08},
    "NFLTD": {"km": 0.20, "mp": 0.10, "me": 0.10},
    "NHL_AST": {"km": 0.15, "mp": 0.08, "me": 0.08},
    "NFLTT": {"km": 0.12, "mp": 0.06, "me": 0.10},
    "NHLSPREAD": {"km": 0.15, "mp": 0.08, "me": 0.10},
    "EREDIVISIE": {"km": 0.12, "mp": 0.06, "me": 0.10},
    "NBA_STL": {"km": 0.12, "mp": 0.06, "me": 0.10},
    "NFL_REC_YDS": {"km": 0.15, "mp": 0.08, "me": 0.08},
    "NBA_3PT": {"km": 0.12, "mp": 0.06, "me": 0.10},
    "DARTS": {"km": 0.10, "mp": 0.05, "me": 0.10},
    "NHL_PTS": {"km": 0.10, "mp": 0.05, "me": 0.10},
    "CS2": {"km": 0.10, "mp": 0.05, "me": 0.10},
    "NBA_REB": {"km": 0.08, "mp": 0.04, "me": 0.10},
    "MLS": {"km": 0.10, "mp": 0.05, "me": 0.10},
    "NBA_PTS": {"km": 0.10, "mp": 0.05, "me": 0.10},
    "NBASPREAD": {"km": 0.06, "mp": 0.03, "me": 0.12},
    "EUROLEAGUE": {"km": 0.10, "mp": 0.05, "me": 0.10},
    "NBA": {"km": 0.04, "mp": 0.03, "me": 0.15},
    "NBA_AST": {"km": 0.08, "mp": 0.04, "me": 0.10},
    "WNBA": {"km": 0.15, "mp": 0.08, "me": 0.10},
    "NCAAWB": {"km": 0.10, "mp": 0.05, "me": 0.10},
    "ATP": {"km": 0.12, "mp": 0.06, "me": 0.10},
    "WTA": {"km": 0.10, "mp": 0.05, "me": 0.10},
    "LOL_GAME": {"km": 0.08, "mp": 0.04, "me": 0.10},
    "NHL": {"km": 0.15, "mp": 0.08, "me": 0.12},
    "NFLGW": {"km": 0.10, "mp": 0.05, "me": 0.10},
    "NCAAF_TOTAL": {"km": 0.08, "mp": 0.04, "me": 0.10},
    "NFLSPREAD": {"km": 0.06, "mp": 0.03, "me": 0.12},
    "MLB": {"km": 0.04, "mp": 0.02, "me": 0.12},
    "NCAAMB": {"km": 0.06, "mp": 0.03, "me": 0.10},
    "UCL": {"km": 0.08, "mp": 0.04, "me": 0.12},
    "CFB": {"km": 0.08, "mp": 0.04, "me": 0.12},
}

CURRENT_PARAMS = {
    "NHL": {"km": 0.15, "mp": 0.08, "me": 0.12},
    "NHLSPREAD": {"km": 0.15, "mp": 0.08, "me": 0.10},
    "NBASPREAD": {"km": 0.06, "mp": 0.03, "me": 0.12},
    "NFLTD": {"km": 0.20, "mp": 0.10, "me": 0.10},
    "NFLTT": {"km": 0.10, "mp": 0.05, "me": 0.10},
    "NFLSPREAD": {"km": 0.06, "mp": 0.03, "me": 0.12},
    "NBA": {"km": 0.04, "mp": 0.03, "me": 0.15},
    "ATP": {"km": 0.12, "mp": 0.06, "me": 0.10},
    "UCL": {"km": 0.12, "mp": 0.06, "me": 0.10},
    "WNBA": {"km": 0.15, "mp": 0.08, "me": 0.10},
    "CFB": {"km": 0.08, "mp": 0.04, "me": 0.12},
    "MLB": {"km": 0.06, "mp": 0.03, "me": 0.12},
    "NBA_3PT": {"km": 0.12, "mp": 0.06, "me": 0.10},
    "NBA_PTS": {"km": 0.10, "mp": 0.05, "me": 0.10},
    "NBA_REB": {"km": 0.08, "mp": 0.04, "me": 0.10},
}

SPORT_OPTIMAL_PT = {
    "NHLSPREAD": 200, "NFLTD": None, "NBASPREAD": 100, "NBA": 200,
    "NHL": 50, "ATP": 500, "NFLTT": None, "NFLSPREAD": 75,
    "NCAAMB": 100, "NCAAWB": 300, "MLB": 100, "NFLGW": 100,
    "WTA": 150, "UCL": 300, "WNBA": 200, "CFB": 200,
    "NBA_3PT": None, "NBA_PTS": None, "NBA_REB": None, "NBA_AST": None,
    "NBA_STL": 200,
    "NFL_1ST_TD": None, "NHL_GOAL": None, "NHL_AST": 500, "NHL_PTS": 500,
    "NFL_REC_YDS": 500, "NCAAF_TOTAL": 200,
    "CS2": 500, "MLS": 200, "EUROLEAGUE": 200, "LOL_GAME": 200,
    "DARTS": 500, "EREDIVISIE": 300,
}


def per_price_yes_rate(sport, yp):
    if sport == "NBA": return max(0.20, 0.50 - (yp - 60) * 0.004)
    if sport == "NHL": return max(0.30, 0.55 - (yp - 60) * 0.003)
    return None

def get_yes_rate(sport, yp):
    pp = per_price_yes_rate(sport, yp)
    if pp is not None: return pp
    et = ALL_EDGE_TABLES.get(sport, {})
    for (lo, hi), v in et.items():
        if lo <= yp <= hi: return v if not isinstance(v, tuple) else v[0]
    return None

def kalshi_fee(yp_cents):
    p = yp_cents / 100.0
    return math.ceil(0.07 * p * (1 - p) * 100) / 100.0

def dynamic_pt(sport, entry_no_cents):
    base = SPORT_OPTIMAL_PT.get(sport, 150)
    if base is None: return None
    if entry_no_cents <= 15: return min(base * 3, 500)
    elif entry_no_cents <= 25: return base
    elif entry_no_cents <= 35: return max(base // 2, 50)
    else: return 50

def current_pt_fn(sport, entry_no):
    pt_map = {"NBA": 150, "NBASPREAD": 150, "NFLSPREAD": 200, "NFLTD": 100,
              "NHL": 100, "NHLSPREAD": 300, "UCL": 100, "WNBA": 100, "ATP": 100,
              "CFB": 200, "MLB": 50, "NFLTT": 150, "NBA_3PT": 200, "NBA_PTS": 150, "NBA_REB": 150}
    return pt_map.get(sport, 150)

def is_excluded(sport, yp, use_exclusions):
    if not use_exclusions:
        return False
    for (s, lo, hi) in EXCLUDE_BUCKETS:
        if s == sport and lo <= yp <= hi:
            return True
    return False


# Load data
case_parts = [f"WHEN event_ticker LIKE '{p}%' THEN '{s}'" for p, s in ALL_PREFIXES.items()]
case_stmt = ' '.join(case_parts)
like_clauses = ' OR '.join(f"event_ticker LIKE '{p}%'" for p in ALL_PREFIXES.keys())

print("Loading data...")
df = con.sql(f"""
    WITH gm AS (
        SELECT ticker, result, event_ticker, close_time,
               CASE {case_stmt} END as sport
        FROM '{mp}'
        WHERE ({like_clauses}) AND status='finalized' AND result IN ('yes','no')
    ),
    pregame AS (
        SELECT t.ticker, t.yes_price as yp,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time DESC) as rn
        FROM '{tp}' t
        JOIN gm ON t.ticker = gm.ticker
        WHERE t.created_time < gm.close_time - INTERVAL 2 HOURS
    ),
    max_no AS (
        SELECT t.ticker, MAX(100 - t.yes_price) as max_no_cents
        FROM '{tp}' t
        WHERE t.ticker IN (SELECT ticker FROM gm)
        GROUP BY t.ticker
    )
    SELECT gm.ticker, gm.result, gm.sport, gm.close_time,
           pg.yp, mn.max_no_cents
    FROM gm
    JOIN pregame pg ON gm.ticker = pg.ticker AND pg.rn = 1
    JOIN max_no mn ON gm.ticker = mn.ticker
    WHERE gm.sport IS NOT NULL AND pg.yp BETWEEN 55 AND 95
    ORDER BY gm.close_time
""").fetchdf()

import pandas as pd
for col in ['yp', 'max_no_cents']:
    df[col] = pd.to_numeric(df[col], errors='coerce')
df = df.dropna(subset=['yp', 'max_no_cents'])
df['yp'] = df['yp'].astype(int)
df['max_no_cents'] = df['max_no_cents'].astype(int)
print(f"  {len(df)} markets loaded ({time.time()-t0:.1f}s)")


def run_sim(label, params, pt_fn, use_exclusions=False, kelly_global=0.50, starting=150):
    bankroll = float(starting)
    peak = bankroll
    max_dd = 0.0
    trades = 0
    wins = 0
    excluded = 0
    total_cost = 0.0
    sport_trades = {}

    for _, row in df.iterrows():
        sport = row['sport']
        yp = int(row['yp'])
        result = row['result']
        max_no = int(row['max_no_cents'])
        entry_no_cents = 100 - yp

        p = params.get(sport)
        if not p: continue

        if is_excluded(sport, yp, use_exclusions):
            excluded += 1
            continue

        yes_rate = get_yes_rate(sport, yp)
        if yes_rate is None: continue

        no_price = entry_no_cents / 100.0
        fee = kalshi_fee(yp)
        edge = (yp / 100.0) - yes_rate
        fee_drag = fee / no_price if no_price > 0 else 0
        if edge - fee_drag < p["me"]: continue

        # Dynamic Kelly by entry price
        km = p["km"]
        mp_val = p["mp"]
        if entry_no_cents <= 15: km *= 1.3; mp_val *= 1.3
        elif entry_no_cents <= 25: km *= 1.1; mp_val *= 1.1
        elif entry_no_cents >= 36: km *= 0.6; mp_val *= 0.6

        bet = km * kelly_global * bankroll
        bet = min(bet, 200, mp_val * bankroll)
        if bet < 0.10: continue

        if peak > 0:
            dd = (peak - bankroll) / peak
            if dd >= 0.40: continue
            elif dd >= 0.25: bet *= 0.25
            elif dd >= 0.15: bet *= 0.50

        contracts = bet / no_price
        cost = no_price * contracts + fee
        if cost > bankroll: continue

        bankroll -= cost
        total_cost += cost
        trades += 1
        sport_trades[sport] = sport_trades.get(sport, 0) + 1

        pt_pct = pt_fn(sport, entry_no_cents)
        if pt_pct is not None:
            pt_price = no_price * (1 + pt_pct / 100.0)
            if max_no / 100.0 >= pt_price:
                exit_fee = kalshi_fee(int((1 - pt_price) * 100))
                revenue = pt_price * contracts - exit_fee
                bankroll += revenue
                if revenue > cost: wins += 1
                if bankroll > peak: peak = bankroll
                dd = (peak - bankroll) / peak if peak > 0 else 0
                if dd > max_dd: max_dd = dd
                continue

        if result == "no":
            bankroll += 1.0 * contracts
            wins += 1

        if bankroll > peak: peak = bankroll
        dd = (peak - bankroll) / peak if peak > 0 else 0
        if dd > max_dd: max_dd = dd

    wr = wins / trades * 100 if trades > 0 else 0
    roi = (bankroll - starting) / total_cost * 100 if total_cost > 0 else 0
    return {"label": label, "final": bankroll, "trades": trades, "wr": wr,
            "max_dd": max_dd * 100, "roi": roi, "excluded": excluded,
            "sports": len(sport_trades)}


results = []

# A: Current agent (baseline)
r = run_sim("A. CURRENT AGENT", CURRENT_PARAMS, current_pt_fn)
results.append(r)

# B: Optimized WITHOUT exclusions
r = run_sim("B. OPTIMIZED (no exclusions)", OPTIMIZED_PARAMS, dynamic_pt)
results.append(r)

# C: Optimized WITH exclusions
r = run_sim("C. OPTIMIZED + EXCLUSIONS", OPTIMIZED_PARAMS, dynamic_pt, use_exclusions=True)
results.append(r)

# D: Optimized + exclusions + Kelly 0.75
r = run_sim("D. OPT + EXCL + Kelly 0.75", OPTIMIZED_PARAMS, dynamic_pt,
            use_exclusions=True, kelly_global=0.75)
results.append(r)

print()
print("=" * 100)
print("  FINAL COMPARISON: Current vs Optimized vs Optimized+Exclusions")
print(f"  $150 start, 12 months, pre-game pricing, dynamic Kelly by entry price")
print("=" * 100)
print()
print(f"{'Strategy':<40} {'Final':>12} {'Trades':>7} {'Excl':>6} {'WR':>6} {'MaxDD':>6} {'ROI':>7} {'Return':>8}")
print("-" * 100)
for r in sorted(results, key=lambda x: -x['final']):
    ret = r['final'] / 150
    print(f"{r['label']:<40} ${r['final']:>11,.0f} {r['trades']:>7} {r['excluded']:>6} {r['wr']:>5.1f}% {r['max_dd']:>5.1f}% {r['roi']:>6.1f}% {ret:>7.0f}x")

print()
print("RISK-ADJUSTED RANKING:")
for r in sorted(results, key=lambda x: -x['final'] / max(x['max_dd'], 1)):
    ratio = r['final'] / max(r['max_dd'], 1)
    print(f"  {r['label']:<40} ratio={ratio:>9,.0f}  DD={r['max_dd']:.1f}%  Final=${r['final']:>11,.0f}")

print()
c = results[2]  # Optimized + exclusions
a = results[0]  # Current
improvement = (c['final'] - a['final']) / a['final'] * 100
dd_change = c['max_dd'] - a['max_dd']
print(f"IMPROVEMENT (Optimized+Exclusions vs Current):")
print(f"  Return:    ${a['final']:,.0f} -> ${c['final']:,.0f}  ({improvement:+.0f}%)")
print(f"  Max DD:    {a['max_dd']:.1f}% -> {c['max_dd']:.1f}%  ({dd_change:+.1f}%)")
print(f"  Trades:    {a['trades']} -> {c['trades']}  ({c['trades'] - a['trades']:+d})")
print(f"  Excluded:  {c['excluded']} negative-edge trades filtered out")
print(f"  Win Rate:  {a['wr']:.1f}% -> {c['wr']:.1f}%")

print()
print("1-YEAR PROJECTION FROM $150:")
for r in sorted(results, key=lambda x: -x['final']):
    if r['final'] > 150:
        monthly = (r['final'] / 150) ** (1/12) - 1
        print(f"  {r['label']:<40} {monthly*100:>6.1f}%/mo  ${r['final']:>11,.0f}")

print(f"\n  Runtime: {time.time()-t0:.1f}s")
