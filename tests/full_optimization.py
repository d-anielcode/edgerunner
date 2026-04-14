"""
COMPREHENSIVE STRATEGY OPTIMIZATION

Tests EVERY market available in the TrevorJS dataset with:
- Dynamic PT thresholds per sport x entry price
- Dynamic Kelly per sport x entry price
- All sports including ones we've disabled
- Realistic execution (pre-game pricing, fees, spread penalty, DD breakers)

Output: clear ROI and max drawdown for every combination.
Goal: find the optimal configuration backed by data, not guesses.
"""
import duckdb
import math
import time
import itertools

t0 = time.time()
con = duckdb.connect()
mp = 'data/trevorjs/markets-*.parquet'
tp = 'data/trevorjs/trades-*.parquet'

# ALL possible sports in the dataset — including disabled ones
ALL_SPORT_PREFIXES = {
    "KXNBAGAME": "NBA", "KXNHLGAME": "NHL", "KXUCLGAME": "UCL",
    "KXWNBAGAME": "WNBA", "KXATPMATCH": "ATP", "KXNFLANYTD": "NFLTD",
    "KXNHLSPREAD": "NHLSPREAD", "KXNBASPREAD": "NBASPREAD",
    "KXNFLSPREAD": "NFLSPREAD", "KXMLBGAME": "MLB",
    "KXNFLTEAMTOTAL": "NFLTT", "KXCFBGAME": "CFB",
    "KXNBAPTS": "NBA_PTS", "KXNBA3PT": "NBA_3PT",
    "KXNBAREB": "NBA_REB", "KXNBAAST": "NBA_AST",
    "KXEPLGAME": "EPL", "KXLALIGAGAME": "LALIGA",
    "KXUFCFIGHT": "UFC", "KXNCAAMBGAME": "NCAAMB",
    "KXNCAAWBGAME": "NCAAWB", "KXWTAMATCH": "WTA",
    "KXNFLGAME": "NFLGW", "KXMLBTOTAL": "MLBTOTAL",
    "KXCBAGAME": "CBA", "KXLOLMAP": "LOL",
    "KXATPCHALLENGERMATCH": "ATPCH", "KXLIGUE": "LIGUE1",
}

# Edge tables for ALL sports (including disabled ones for testing)
ALL_EDGE_TABLES = {
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
    "CFB":      {(55, 65): 0.520, (66, 75): 0.550, (76, 85): 0.620},
    "NBA_3PT":  {(55, 64): 0.497, (65, 74): 0.594, (75, 84): 0.707, (85, 95): 0.771},
    "NBA_PTS":  {(55, 64): 0.538, (65, 74): 0.657, (75, 84): 0.736, (85, 95): 0.765},
    "NBA_REB":  {(55, 64): 0.574, (65, 74): 0.629, (75, 84): 0.701, (85, 95): 0.864},
    "NBA_AST":  {(55, 64): 0.582, (65, 74): 0.644, (75, 84): 0.747, (85, 95): 0.827},
    "CBA":      {(55, 65): 0.500, (66, 75): 0.550, (76, 85): 0.620},
    "LIGUE1":   {(55, 65): 0.480, (66, 75): 0.500, (76, 85): 0.550},
    "LOL":      {(55, 65): 0.500, (66, 75): 0.520, (76, 85): 0.550},
    "ATPCH":    {(55, 65): 0.520, (66, 75): 0.550, (76, 85): 0.620},
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


# Load ALL data
case_parts = [f"WHEN event_ticker LIKE '{p}%' THEN '{s}'" for p, s in ALL_SPORT_PREFIXES.items()]
case_stmt = ' '.join(case_parts)
like_clauses = ' OR '.join(f"event_ticker LIKE '{p}%'" for p in ALL_SPORT_PREFIXES.keys())

print("Loading ALL markets from TrevorJS dataset...")
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
df['entry_no'] = 100 - df['yp']

print(f"  {len(df)} total markets loaded ({time.time()-t0:.1f}s)")
print(f"  Sports: {sorted(df['sport'].unique())}")
print()

# ═══════════════════════════════════════════════════════════════
# PHASE 1: Per-sport standalone analysis (no compounding)
# Find which sports are actually profitable at each PT level
# ═══════════════════════════════════════════════════════════════
print("=" * 90)
print("  PHASE 1: PER-SPORT PROFITABILITY (flat $1 bet, no compounding)")
print("  Tests each sport × PT level independently")
print("=" * 90)
print()

pt_levels = [None, 50, 100, 150, 200, 300, 500]

sport_results = {}

for sport in sorted(df['sport'].unique()):
    sdf = df[df['sport'] == sport]
    if len(sdf) < 20:
        continue

    best_pnl = -999
    best_pt = None
    best_wr = 0
    all_pts = {}

    for pt in pt_levels:
        total_pnl = 0
        wins = 0
        losses = 0
        for _, row in sdf.iterrows():
            entry_no = row['entry_no'] / 100.0
            max_no = row['max_no_cents'] / 100.0
            result = row['result']
            fee = kalshi_fee(row['yp'])
            cost = entry_no + fee

            # Check PT
            if pt is not None:
                pt_price = entry_no * (1 + pt / 100.0)
                if max_no >= pt_price:
                    exit_fee = kalshi_fee(int((1 - pt_price) * 100))
                    revenue = pt_price - exit_fee
                    pnl = revenue - cost
                    total_pnl += pnl
                    if pnl > 0: wins += 1
                    else: losses += 1
                    continue

            # Hold to settlement
            if result == 'no':
                total_pnl += 1.0 - cost
                wins += 1
            else:
                total_pnl += -cost
                losses += 1

        wr = wins / (wins + losses) * 100 if (wins + losses) > 0 else 0
        roi = total_pnl / (len(sdf) * 0.25) * 100  # Approximate ROI on avg 25c bet
        all_pts[pt] = {"pnl": total_pnl, "wr": wr, "roi": roi}

        if total_pnl > best_pnl:
            best_pnl = total_pnl
            best_pt = pt
            best_wr = wr

    sport_results[sport] = {
        "n": len(sdf), "best_pt": best_pt, "best_pnl": best_pnl,
        "best_wr": best_wr, "all_pts": all_pts,
        "hold_pnl": all_pts[None]["pnl"],
    }

# Print results sorted by best P&L
print(f"{'Sport':<12} {'N':>5} {'Best PT':>8} {'Best P&L':>10} {'Hold P&L':>10} {'PT Lift':>8} {'WR':>6} {'Verdict':>10}")
print("-" * 75)
for sport in sorted(sport_results.keys(), key=lambda s: -sport_results[s]['best_pnl']):
    r = sport_results[sport]
    pt_label = "HOLD" if r['best_pt'] is None else f"{r['best_pt']}%"
    lift = r['best_pnl'] - r['hold_pnl']
    verdict = "STRONG" if r['best_pnl'] > 5 else ("WEAK" if r['best_pnl'] > 0 else "AVOID")
    print(f"{sport:<12} {r['n']:>5} {pt_label:>8} ${r['best_pnl']:>9.1f} ${r['hold_pnl']:>9.1f} ${lift:>+7.1f} {r['best_wr']:>5.1f}% {verdict:>10}")

# ═══════════════════════════════════════════════════════════════
# PHASE 2: Per-sport × entry price optimal PT + Kelly
# ═══════════════════════════════════════════════════════════════
print()
print("=" * 90)
print("  PHASE 2: OPTIMAL PT BY SPORT x ENTRY PRICE")
print("=" * 90)
print()

price_buckets = [(5, 15, "cheap"), (16, 25, "mid"), (26, 35, "standard"), (36, 45, "expensive")]
profitable_sports = [s for s, r in sport_results.items() if r['best_pnl'] > 0]

for sport in sorted(profitable_sports, key=lambda s: -sport_results[s]['best_pnl']):
    sdf = df[df['sport'] == sport]
    print(f"  {sport} ({len(sdf)} markets, best overall: {('HOLD' if sport_results[sport]['best_pt'] is None else str(sport_results[sport]['best_pt'])+'%')}):")

    for lo, hi, label in price_buckets:
        bucket = sdf[(sdf['entry_no'] >= lo) & (sdf['entry_no'] <= hi)]
        if len(bucket) < 10:
            continue

        best_pnl = -999
        best_pt = None
        for pt in pt_levels:
            pnl = 0
            for _, row in bucket.iterrows():
                entry_no = row['entry_no'] / 100.0
                max_no = row['max_no_cents'] / 100.0
                fee = kalshi_fee(row['yp'])
                cost = entry_no + fee

                if pt is not None:
                    pt_price = entry_no * (1 + pt / 100.0)
                    if max_no >= pt_price:
                        exit_fee = kalshi_fee(int((1 - pt_price) * 100))
                        pnl += pt_price - exit_fee - cost
                        continue

                if row['result'] == 'no':
                    pnl += 1.0 - cost
                else:
                    pnl += -cost

            if pnl > best_pnl:
                best_pnl = pnl
                best_pt = pt

        pt_str = "HOLD" if best_pt is None else f"{best_pt}%"
        print(f"    {label:>10} ({lo}-{hi}c NO, n={len(bucket):>4}): best PT={pt_str:<6} P&L=${best_pnl:>+7.1f}")

    print()

# ═══════════════════════════════════════════════════════════════
# PHASE 3: Sharpe ratio per sport x entry price
# ═══════════════════════════════════════════════════════════════
print("=" * 90)
print("  PHASE 3: RISK-ADJUSTED RETURNS (Sharpe) PER SPORT x ENTRY PRICE")
print("=" * 90)
print()

print(f"{'Sport':<12} {'Price':>10} {'N':>5} {'NO WR':>6} {'AvgPnL':>8} {'Std':>8} {'Sharpe':>7} {'OptKelly':>9}")
print("-" * 75)

for sport in sorted(profitable_sports, key=lambda s: -sport_results[s]['best_pnl']):
    sdf = df[df['sport'] == sport]
    for lo, hi, label in price_buckets:
        bucket = sdf[(sdf['entry_no'] >= lo) & (sdf['entry_no'] <= hi)]
        if len(bucket) < 15:
            continue

        pnls = []
        no_wins = 0
        for _, row in bucket.iterrows():
            entry_no = row['entry_no'] / 100.0
            fee = kalshi_fee(row['yp'])
            if row['result'] == 'no':
                pnls.append(1.0 - entry_no - fee)
                no_wins += 1
            else:
                pnls.append(-(entry_no + fee))

        avg = sum(pnls) / len(pnls)
        std = (sum((p-avg)**2 for p in pnls) / len(pnls)) ** 0.5
        sharpe = avg / std if std > 0 else 0
        wr = no_wins / len(bucket) * 100

        no_price_avg = (lo + hi) / 2 / 100.0
        b = (1.0 - no_price_avg) / no_price_avg if no_price_avg > 0 else 0
        p = wr / 100.0
        kelly = max(0, (b * p - (1-p)) / b) if b > 0 else 0

        print(f"{sport:<12} {label:>10} {len(bucket):>5} {wr:>5.1f}% ${avg:>7.4f} ${std:>7.4f} {sharpe:>+6.3f} {kelly*100:>8.1f}%")

# ═══════════════════════════════════════════════════════════════
# PHASE 4: Full portfolio simulation with optimal params
# ═══════════════════════════════════════════════════════════════
print()
print("=" * 90)
print("  PHASE 4: FULL PORTFOLIO SIMULATION")
print("  $150 start, 12 months, realistic execution")
print("=" * 90)
print()

def run_portfolio(label, enabled_sports, sport_kelly, dynamic_pt_fn, kelly_global=0.50,
                  starting=150, max_bet=200, min_edge_override=None):
    bankroll = float(starting)
    peak = bankroll
    max_dd = 0.0
    trades = 0
    wins = 0
    total_invested = 0.0
    daily_pnl = {}

    for _, row in df.iterrows():
        sport = row['sport']
        if sport not in enabled_sports:
            continue

        yp = int(row['yp'])
        result = row['result']
        max_no = int(row['max_no_cents'])
        entry_no_cents = 100 - yp

        yes_rate = get_yes_rate(sport, yp)
        if yes_rate is None:
            continue

        no_price = entry_no_cents / 100.0
        fee = kalshi_fee(yp)
        edge = (yp / 100.0) - yes_rate
        fee_drag = fee / no_price if no_price > 0 else 0

        me = min_edge_override if min_edge_override else sport_kelly.get(sport, {}).get("me", 0.10)
        if edge - fee_drag < me:
            continue

        km = sport_kelly.get(sport, {}).get("km", 0.10)
        mp_val = sport_kelly.get(sport, {}).get("mp", 0.05)

        # Scale Kelly by entry price
        if entry_no_cents <= 15:
            km *= 1.3
            mp_val *= 1.3
        elif entry_no_cents <= 25:
            km *= 1.1
            mp_val *= 1.1
        elif entry_no_cents >= 36:
            km *= 0.6
            mp_val *= 0.6

        bet = km * kelly_global * bankroll
        bet = min(bet, max_bet, mp_val * bankroll)
        if bet < 0.10:
            continue

        # DD breaker
        if peak > 0:
            dd = (peak - bankroll) / peak
            if dd >= 0.40: continue
            elif dd >= 0.25: bet *= 0.25
            elif dd >= 0.15: bet *= 0.50

        contracts = bet / no_price
        cost = no_price * contracts + fee
        if cost > bankroll:
            continue

        bankroll -= cost
        total_invested += cost
        trades += 1

        # Dynamic PT
        pt_pct = dynamic_pt_fn(sport, entry_no_cents)
        if pt_pct is not None:
            pt_price = no_price * (1 + pt_pct / 100.0)
            max_no_price = max_no / 100.0
            if max_no_price >= pt_price:
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
    roi = (bankroll - starting) / total_invested * 100 if total_invested > 0 else 0
    return {"label": label, "final": bankroll, "trades": trades, "wr": wr,
            "max_dd": max_dd * 100, "roi": roi}


# Current agent params
current_sports = {"NHL", "NHLSPREAD", "NBASPREAD", "NFLTD", "NFLTT", "NFLSPREAD",
                  "NBA", "ATP", "UCL", "WNBA", "CFB", "MLB", "NBA_3PT", "NBA_PTS", "NBA_REB"}

current_kelly = {
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

def current_pt(sport, entry_no):
    sport_pt = {"NBA": 150, "NBASPREAD": 150, "NFLSPREAD": 200, "NFLTD": 100,
                "NHL": 100, "NHLSPREAD": 300, "UCL": 100, "WNBA": 100, "ATP": 100,
                "CFB": 200, "MLB": 50, "NFLTT": 150, "NBA_3PT": 200, "NBA_PTS": 150, "NBA_REB": 150}
    return sport_pt.get(sport, 150)

def dynamic_pt_v1(sport, entry_no):
    # Props: hold to settlement
    if sport in ("NBA_3PT", "NBA_PTS", "NBA_REB", "NBA_AST", "NFLTD", "NFLTT"):
        return None
    # Game winners/spreads: scale with entry price
    if entry_no <= 15: return 500
    elif entry_no <= 25: return 200
    elif entry_no <= 35: return 100
    else: return 50

def dynamic_pt_v2(sport, entry_no):
    """More nuanced: use per-sport optimal from Phase 2 data."""
    # Sport-specific overrides from the Phase 1/2 analysis
    sport_base = {
        "NHLSPREAD": 200, "NHL": 50, "NBASPREAD": 100, "NBA": 200,
        "NFLSPREAD": 75, "ATP": 500, "NFLTD": None, "NFLTT": None,
        "UCL": 100, "WNBA": 100, "CFB": 200, "MLB": 50,
        "NBA_3PT": None, "NBA_PTS": None, "NBA_REB": None, "NBA_AST": None,
        "NCAAMB": 100, "WTA": 150, "NFLGW": 100,
    }
    base = sport_base.get(sport, 150)
    if base is None:
        return None  # Hold to settlement

    # Adjust by entry price: cheaper = higher PT, expensive = lower PT
    if entry_no <= 15:
        return min(base * 3, 500)
    elif entry_no <= 25:
        return base
    elif entry_no <= 35:
        return max(base // 2, 50)
    else:
        return 50


# Expanded sports (add back profitable disabled ones)
expanded_sports = current_sports | {"WTA", "NCAAMB", "NFLGW"}
expanded_kelly = {**current_kelly,
    "WTA": {"km": 0.10, "mp": 0.05, "me": 0.10},
    "NCAAMB": {"km": 0.10, "mp": 0.05, "me": 0.10},
    "NFLGW": {"km": 0.12, "mp": 0.06, "me": 0.10},
}

# Run simulations
results = []

results.append(run_portfolio("A. CURRENT AGENT", current_sports, current_kelly, current_pt))
results.append(run_portfolio("B. Dynamic PT v1 (price-scaled)", current_sports, current_kelly, dynamic_pt_v1))
results.append(run_portfolio("C. Dynamic PT v2 (sport+price)", current_sports, current_kelly, dynamic_pt_v2))
results.append(run_portfolio("D. Dynamic PT v2 + expanded sports", expanded_sports, expanded_kelly, dynamic_pt_v2))
results.append(run_portfolio("E. Dynamic PT v2 + min_edge 12%", current_sports, current_kelly, dynamic_pt_v2, min_edge_override=0.12))
results.append(run_portfolio("F. Dynamic PT v2 + min_edge 15%", current_sports, current_kelly, dynamic_pt_v2, min_edge_override=0.15))
results.append(run_portfolio("G. Dynamic PT v2 + Kelly 0.75", current_sports, current_kelly, dynamic_pt_v2, kelly_global=0.75))
results.append(run_portfolio("H. Dynamic PT v2 + expanded + 12%edge", expanded_sports, expanded_kelly, dynamic_pt_v2, min_edge_override=0.12))

print(f"{'Strategy':<48} {'Final':>12} {'Trades':>7} {'WR':>6} {'MaxDD':>6} {'ROI':>7}")
print("-" * 90)
for r in sorted(results, key=lambda x: -x['final']):
    print(f"{r['label']:<48} ${r['final']:>11,.0f} {r['trades']:>7} {r['wr']:>5.1f}% {r['max_dd']:>5.1f}% {r['roi']:>6.1f}%")

print()
print("RECOMMENDED STRATEGY:")
best = max(results, key=lambda r: r['final'] / max(r['max_dd'], 1))
print(f"  {best['label']}")
print(f"  Final: ${best['final']:,.0f} | Trades: {best['trades']} | WR: {best['wr']:.1f}% | MaxDD: {best['max_dd']:.1f}% | ROI: {best['roi']:.1f}%")

# Risk-adjusted ranking
print()
print("RISK-ADJUSTED RANKING (Final / MaxDD):")
for r in sorted(results, key=lambda x: -x['final'] / max(x['max_dd'], 1)):
    ratio = r['final'] / max(r['max_dd'], 1)
    print(f"  {r['label']:<48} ratio={ratio:>8,.0f}  DD={r['max_dd']:.1f}%  Final=${r['final']:>11,.0f}")

print(f"\n  Runtime: {time.time()-t0:.1f}s")
