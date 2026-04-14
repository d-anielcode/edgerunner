"""
Deep profit-take analysis: find the OPTIMAL exit threshold per sport × entry price.

Questions:
1. Does optimal PT vary by entry price? (buying NO at 15c vs 30c = different dynamics)
2. Does optimal PT vary by sport? (NHL low-scoring vs NBA high-scoring)
3. What's the max unrealized gain distribution? (how high do winning trades go?)
4. Is there a "sweet spot" where selling captures most of the upside?
5. Are there correlations between entry price and max gain?
"""
import duckdb
import math
import time

t0 = time.time()
con = duckdb.connect()
mp = 'data/trevorjs/markets-*.parquet'
tp = 'data/trevorjs/trades-*.parquet'

SPORT_FROM_PREFIX = {
    "KXNBAGAME": "NBA", "KXNHLGAME": "NHL", "KXUCLGAME": "UCL",
    "KXWNBAGAME": "WNBA", "KXATPMATCH": "ATP", "KXNFLANYTD": "NFLTD",
    "KXNHLSPREAD": "NHLSPREAD", "KXNBASPREAD": "NBASPREAD",
    "KXNFLSPREAD": "NFLSPREAD", "KXMLBGAME": "MLB",
    "KXNFLTEAMTOTAL": "NFLTT", "KXCFBGAME": "CFB",
    "KXNBAPTS": "NBA_PTS", "KXNBA3PT": "NBA_3PT", "KXNBAREB": "NBA_REB",
}

case_parts = [f"WHEN event_ticker LIKE '{p}%' THEN '{s}'" for p, s in SPORT_FROM_PREFIX.items()]
case_stmt = ' '.join(case_parts)
like_clauses = ' OR '.join(f"event_ticker LIKE '{p}%'" for p in SPORT_FROM_PREFIX.keys())

print("Loading data with intra-game price paths...")

# For each market: get entry price, max NO price during game, settlement result
df = con.sql(f"""
    WITH gm AS (
        SELECT ticker, result, event_ticker, close_time,
               CASE {case_stmt} END as sport
        FROM '{mp}'
        WHERE ({like_clauses}) AND status='finalized' AND result IN ('yes','no')
    ),
    pregame AS (
        SELECT t.ticker, t.yes_price as entry_yes,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time DESC) as rn
        FROM '{tp}' t
        JOIN gm ON t.ticker = gm.ticker
        WHERE t.created_time < gm.close_time - INTERVAL 2 HOURS
    ),
    ingame_max AS (
        SELECT t.ticker,
               MAX(100 - t.yes_price) as max_no_cents,
               MIN(100 - t.yes_price) as min_no_cents
        FROM '{tp}' t
        JOIN gm ON t.ticker = gm.ticker
        WHERE t.created_time >= gm.close_time - INTERVAL 6 HOURS
        GROUP BY t.ticker
    )
    SELECT gm.ticker, gm.result, gm.sport,
           pg.entry_yes,
           (100 - pg.entry_yes) as entry_no,
           ig.max_no_cents,
           ig.min_no_cents
    FROM gm
    JOIN pregame pg ON gm.ticker = pg.ticker AND pg.rn = 1
    LEFT JOIN ingame_max ig ON gm.ticker = ig.ticker
    WHERE gm.sport IS NOT NULL AND pg.entry_yes BETWEEN 55 AND 95
    ORDER BY gm.sport
""").fetchdf()

import pandas as pd
for col in ['entry_yes', 'entry_no', 'max_no_cents', 'min_no_cents']:
    df[col] = pd.to_numeric(df[col], errors='coerce')
df = df.dropna(subset=['entry_yes', 'entry_no', 'max_no_cents'])
df['entry_yes'] = df['entry_yes'].astype(int)
df['entry_no'] = df['entry_no'].astype(int)
df['max_no_cents'] = df['max_no_cents'].astype(int)
df['min_no_cents'] = df['min_no_cents'].astype(int)

# Compute max gain ratio for each market
df['max_gain_pct'] = ((df['max_no_cents'] - df['entry_no']) / df['entry_no'] * 100).clip(lower=0)

print(f"  {len(df)} markets loaded ({time.time()-t0:.1f}s)")
print()

# ═══════════════════════════════════════════════════════════════
# ANALYSIS 1: Optimal PT by sport (grid search)
# ═══════════════════════════════════════════════════════════════
print("=" * 80)
print("  ANALYSIS 1: OPTIMAL PROFIT-TAKE % BY SPORT")
print("  (Which PT% maximizes total P&L for each sport?)")
print("=" * 80)
print()

def simulate_pt(sport_df, pt_pct):
    """Simulate P&L at a given PT% for a set of markets."""
    total_pnl = 0
    trades = 0
    wins = 0
    for _, row in sport_df.iterrows():
        entry_no = row['entry_no'] / 100.0
        max_no = row['max_no_cents'] / 100.0
        result = row['result']
        fee = math.ceil(0.07 * row['entry_yes']/100.0 * entry_no * 100) / 100.0

        cost = entry_no + fee
        trades += 1

        if pt_pct is not None:
            pt_price = entry_no * (1 + pt_pct / 100.0)
            if max_no >= pt_price:
                # PT triggered — sell at PT price
                exit_fee = math.ceil(0.07 * (1 - pt_price) * pt_price * 100) / 100.0
                revenue = pt_price - exit_fee
                total_pnl += revenue - cost
                if revenue > cost:
                    wins += 1
                continue

        # Hold to settlement
        if result == 'no':
            total_pnl += 1.0 - cost
            wins += 1
        else:
            total_pnl += -cost

    wr = wins / trades * 100 if trades > 0 else 0
    return total_pnl, trades, wr

pt_levels = [None, 25, 50, 75, 100, 150, 200, 300, 500]
active_sports = ['NHLSPREAD', 'NFLTD', 'NBASPREAD', 'NBA', 'NHL', 'ATP',
                 'NBA_3PT', 'NBA_PTS', 'NBA_REB', 'NFLTT', 'NFLSPREAD']

print(f"  {'Sport':<12}", end="")
for pt in pt_levels:
    label = "HOLD" if pt is None else f"{pt}%"
    print(f" {label:>8}", end="")
print(f" {'BEST':>8}")
print(f"  {'-'*108}")

sport_optimal = {}
for sport in active_sports:
    sdf = df[df['sport'] == sport]
    if len(sdf) < 20:
        continue

    print(f"  {sport:<12}", end="")
    best_pnl = -999999
    best_pt = None
    for pt in pt_levels:
        pnl, trades, wr = simulate_pt(sdf, pt)
        print(f" ${pnl:>7.0f}", end="")
        if pnl > best_pnl:
            best_pnl = pnl
            best_pt = pt

    label = "HOLD" if best_pt is None else f"{best_pt}%"
    print(f" {label:>8}")
    sport_optimal[sport] = best_pt

print()
print("OPTIMAL PT PER SPORT:")
for sport, pt in sport_optimal.items():
    label = "Hold to settlement" if pt is None else f"{pt}% profit-take"
    print(f"  {sport:<12} -> {label}")

# ═══════════════════════════════════════════════════════════════
# ANALYSIS 2: Optimal PT by entry price bucket
# ═══════════════════════════════════════════════════════════════
print()
print("=" * 80)
print("  ANALYSIS 2: OPTIMAL PT BY ENTRY PRICE (across all sports)")
print("  (Does the best exit point change based on how cheap/expensive we enter?)")
print("=" * 80)
print()

price_buckets = [(5, 15), (16, 25), (26, 35), (36, 45)]

print(f"  {'Entry NO':<12}", end="")
for pt in pt_levels:
    label = "HOLD" if pt is None else f"{pt}%"
    print(f" {label:>8}", end="")
print(f" {'BEST':>8} {'N':>5}")
print(f"  {'-'*115}")

for lo, hi in price_buckets:
    bucket_df = df[(df['entry_no'] >= lo) & (df['entry_no'] <= hi)]
    if len(bucket_df) < 20:
        continue

    print(f"  {lo}-{hi}c NO  ", end="")
    best_pnl = -999999
    best_pt = None
    for pt in pt_levels:
        pnl, trades, wr = simulate_pt(bucket_df, pt)
        print(f" ${pnl:>7.0f}", end="")
        if pnl > best_pnl:
            best_pnl = pnl
            best_pt = pt

    label = "HOLD" if best_pt is None else f"{best_pt}%"
    print(f" {label:>8} {len(bucket_df):>5}")

# ═══════════════════════════════════════════════════════════════
# ANALYSIS 3: Max gain distribution (how high do winners go?)
# ═══════════════════════════════════════════════════════════════
print()
print("=" * 80)
print("  ANALYSIS 3: MAX UNREALIZED GAIN DISTRIBUTION")
print("  (How high do positions go before settling?)")
print("=" * 80)
print()

for sport in ['NHLSPREAD', 'NFLTD', 'NBASPREAD', 'NBA_3PT', 'NBA_PTS', 'NHL', 'ATP']:
    sdf = df[df['sport'] == sport]
    if len(sdf) < 20:
        continue

    gains = sdf['max_gain_pct']
    winners = sdf[sdf['result'] == 'no']
    losers = sdf[sdf['result'] == 'yes']

    print(f"  {sport} ({len(sdf)} markets):")
    print(f"    All markets:  median max gain={gains.median():.0f}%  mean={gains.mean():.0f}%  p75={gains.quantile(0.75):.0f}%  p90={gains.quantile(0.90):.0f}%")
    if len(winners) > 5:
        wg = winners['max_gain_pct']
        print(f"    Winners only: median max gain={wg.median():.0f}%  mean={wg.mean():.0f}%  p75={wg.quantile(0.75):.0f}%  p90={wg.quantile(0.90):.0f}%")
    if len(losers) > 5:
        lg = losers['max_gain_pct']
        print(f"    Losers only:  median max gain={lg.median():.0f}%  mean={lg.mean():.0f}%  p75={lg.quantile(0.75):.0f}%  p90={lg.quantile(0.90):.0f}%")
    print()

# ═══════════════════════════════════════════════════════════════
# ANALYSIS 4: Sport × Entry Price optimal PT grid
# ═══════════════════════════════════════════════════════════════
print("=" * 80)
print("  ANALYSIS 4: SPORT x ENTRY PRICE OPTIMAL PT")
print("  (The most granular view: best exit for each specific situation)")
print("=" * 80)
print()

for sport in ['NHLSPREAD', 'NFLTD', 'NBASPREAD', 'NBA_3PT', 'NBA_PTS']:
    sdf = df[df['sport'] == sport]
    if len(sdf) < 30:
        continue

    print(f"  {sport}:")
    for lo, hi in [(5, 20), (21, 35), (36, 50)]:
        bucket = sdf[(sdf['entry_no'] >= lo) & (sdf['entry_no'] <= hi)]
        if len(bucket) < 10:
            continue

        best_pnl = -999999
        best_pt = None
        for pt in [None, 50, 100, 150, 200, 300, 500]:
            pnl, _, _ = simulate_pt(bucket, pt)
            if pnl > best_pnl:
                best_pnl = pnl
                best_pt = pt

        label = "HOLD" if best_pt is None else f"{best_pt}%"
        print(f"    Entry {lo}-{hi}c NO: best={label} (P&L=${best_pnl:.0f}, n={len(bucket)})")
    print()

print(f"  Runtime: {time.time()-t0:.1f}s")
