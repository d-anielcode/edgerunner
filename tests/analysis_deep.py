"""
Deep analysis: prop edge stability, risk-adjusted returns, optimal timing.
"""
import duckdb
import math
import time

t0 = time.time()
con = duckdb.connect()
mp = 'data/trevorjs/markets-*.parquet'
tp = 'data/trevorjs/trades-*.parquet'

SPORT_FROM_PREFIX = {
    'KXNHLSPREAD': 'NHLSPREAD', 'KXNBASPREAD': 'NBASPREAD',
    'KXNFLANYTD': 'NFLTD', 'KXNFLSPREAD': 'NFLSPREAD',
    'KXNHLGAME': 'NHL', 'KXNBAGAME': 'NBA',
    'KXNFLGAME': 'NFLGW', 'KXNFLTEAMTOTAL': 'NFLTT',
    'KXNCAAMBGAME': 'NCAAMB', 'KXUCLGAME': 'UCL',
    'KXWNBAGAME': 'WNBA', 'KXATPMATCH': 'ATP', 'KXCFBGAME': 'CFB',
    'KXNBAPTS': 'NBA_PTS', 'KXNBAREB': 'NBA_REB',
    'KXNBAAST': 'NBA_AST', 'KXNBA3PT': 'NBA_3PT',
}

case_parts = [f"WHEN event_ticker LIKE '{p}%' THEN '{s}'" for p, s in SPORT_FROM_PREFIX.items()]
case_stmt = ' '.join(case_parts)
like_clauses = ' OR '.join(f"event_ticker LIKE '{p}%'" for p in SPORT_FROM_PREFIX.keys())

print("=" * 70)
print("  DEEP ANALYSIS: Props + Game Winners Combined")
print("=" * 70)
print()

# ── ANALYSIS 1: Prop edge stability over time ──
print("=== 1. PROP EDGE STABILITY BY HALF-YEAR ===")
print("   (Does the edge decay or persist?)")
print()

props_stability = con.sql(f"""
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
    )
    SELECT
        gm.sport,
        CASE
            WHEN gm.close_time < '2025-07-01' THEN 'H1-2025'
            WHEN gm.close_time < '2026-01-01' THEN 'H2-2025'
            ELSE 'Q1-2026'
        END as period,
        CASE
            WHEN pg.yp BETWEEN 60 AND 74 THEN '60-74c'
            WHEN pg.yp BETWEEN 75 AND 89 THEN '75-89c'
            WHEN pg.yp BETWEEN 90 AND 95 THEN '90-95c'
        END as bucket,
        COUNT(*) as n,
        ROUND(100.0 * SUM(CASE WHEN gm.result = 'no' THEN 1 ELSE 0 END) / COUNT(*), 1) as no_pct
    FROM gm
    JOIN pregame pg ON gm.ticker = pg.ticker AND pg.rn = 1
    WHERE gm.sport IN ('NBA_PTS','NBA_3PT','NBA_AST','NBA_REB')
      AND pg.yp BETWEEN 60 AND 95
    GROUP BY gm.sport, period, bucket
    HAVING COUNT(*) >= 10
    ORDER BY gm.sport, bucket, period
""").fetchdf()

for ptype in sorted(props_stability['sport'].unique()):
    subset = props_stability[props_stability['sport'] == ptype]
    print(f"  {ptype}:")
    for bucket in ['60-74c', '75-89c', '90-95c']:
        bdata = subset[subset['bucket'] == bucket]
        if len(bdata) == 0:
            continue
        parts = []
        for _, row in bdata.iterrows():
            parts.append(f"{row['period']}={row['no_pct']:.0f}%({int(row['n'])})")
        print(f"    {bucket}: {' | '.join(parts)}")
    print()


# ── ANALYSIS 2: Risk-adjusted returns (all sports + props) ──
print("=== 2. RISK-ADJUSTED RETURNS — ALL MARKETS (Pre-Game, 60-90c) ===")
print("   (Flat $1 NO bet, Sharpe = avg_pnl / std_pnl)")
print()

risk_adj = con.sql(f"""
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
    )
    SELECT
        gm.sport,
        COUNT(*) as trades,
        ROUND(100.0 * SUM(CASE WHEN gm.result = 'no' THEN 1 ELSE 0 END) / COUNT(*), 1) as no_pct,
        ROUND(AVG(pg.yp), 1) as avg_yp,
        ROUND(AVG(CASE WHEN gm.result = 'no' THEN 1.0 - (100.0 - pg.yp)/100.0
                       ELSE -(100.0 - pg.yp)/100.0 END), 4) as avg_pnl,
        ROUND(STDDEV(CASE WHEN gm.result = 'no' THEN 1.0 - (100.0 - pg.yp)/100.0
                          ELSE -(100.0 - pg.yp)/100.0 END), 4) as std_pnl
    FROM gm
    JOIN pregame pg ON gm.ticker = pg.ticker AND pg.rn = 1
    WHERE gm.sport IS NOT NULL AND pg.yp BETWEEN 60 AND 90
    GROUP BY gm.sport
    HAVING COUNT(*) >= 20
    ORDER BY avg_pnl / NULLIF(std_pnl, 0) DESC
""").fetchdf()

print(f"  {'Sport':<12} {'Trades':>6} {'NO%':>5} {'AvgYP':>6} {'AvgPnL':>8} {'StdPnL':>8} {'Sharpe':>7} {'Verdict':>10}")
print(f"  {'-'*70}")
for _, row in risk_adj.iterrows():
    sport = row['sport']
    trades = int(row['trades'])
    no_pct = float(row['no_pct'])
    avg_yp = float(row['avg_yp'])
    avg_pnl = float(row['avg_pnl'])
    std_pnl = float(row['std_pnl'])
    sharpe = avg_pnl / std_pnl if std_pnl > 0 else 0
    verdict = "STRONG" if sharpe > 0.05 else ("WEAK" if sharpe > 0 else "AVOID")
    print(f"  {sport:<12} {trades:>6} {no_pct:>4.1f}% {avg_yp:>5.1f}c ${avg_pnl:>+7.4f} ${std_pnl:>7.4f} {sharpe:>+6.3f} {verdict:>10}")

print()


# ── ANALYSIS 3: Optimal entry timing ──
print("=== 3. ENTRY TIMING — HOURS BEFORE CLOSE (Top 4 Sports) ===")
print("   (When should we enter to maximize edge?)")
print()

timing = con.sql(f"""
    WITH gm AS (
        SELECT ticker, result, close_time,
               CASE {case_stmt} END as sport
        FROM '{mp}'
        WHERE ({like_clauses}) AND status='finalized' AND result IN ('yes','no')
    ),
    timed AS (
        SELECT t.ticker, t.yes_price as yp, gm.result, gm.sport,
               EXTRACT(EPOCH FROM (gm.close_time - t.created_time)) / 3600.0 as hrs
        FROM '{tp}' t
        JOIN gm ON t.ticker = gm.ticker
        WHERE t.yes_price BETWEEN 60 AND 90
    ),
    bucketed AS (
        SELECT *,
            CASE
                WHEN hrs BETWEEN 0.5 AND 2 THEN '0.5-2h'
                WHEN hrs BETWEEN 2 AND 6 THEN '2-6h'
                WHEN hrs BETWEEN 6 AND 12 THEN '6-12h'
                WHEN hrs BETWEEN 12 AND 24 THEN '12-24h'
                WHEN hrs BETWEEN 24 AND 48 THEN '24-48h'
            END as tb
        FROM timed WHERE hrs BETWEEN 0.5 AND 48
    )
    SELECT sport, tb,
           COUNT(*) as n,
           ROUND(100.0 * SUM(CASE WHEN result = 'no' THEN 1 ELSE 0 END) / COUNT(*), 1) as no_pct
    FROM bucketed
    WHERE tb IS NOT NULL AND sport IN ('NHLSPREAD','NFLTD','NBASPREAD','NHL')
    GROUP BY sport, tb
    HAVING COUNT(*) >= 20
    ORDER BY sport, tb
""").fetchdf()

for sport in ['NHLSPREAD', 'NFLTD', 'NBASPREAD', 'NHL']:
    subset = timing[timing['sport'] == sport]
    if len(subset) == 0:
        continue
    print(f"  {sport}:")
    for _, row in subset.iterrows():
        tb = row['tb']
        n = int(row['n'])
        pct = float(row['no_pct'])
        bar = '#' * int(pct / 2)
        print(f"    {tb:>7}: {n:5d} trades, NO wins {pct:5.1f}% {bar}")
    print()


# ── ANALYSIS 4: Combined portfolio simulation ──
print("=== 4. COMBINED PORTFOLIO: Game Winners + Props (Pre-Game) ===")
print("   (What would our total portfolio look like?)")
print()

# Count available trades per month across all profitable sports + props
monthly = con.sql(f"""
    WITH gm AS (
        SELECT ticker, result, close_time,
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
    )
    SELECT
        EXTRACT(YEAR FROM gm.close_time) as yr,
        EXTRACT(MONTH FROM gm.close_time) as mo,
        gm.sport,
        COUNT(*) as trades,
        SUM(CASE WHEN gm.result = 'no' THEN 1 ELSE 0 END) as no_wins
    FROM gm
    JOIN pregame pg ON gm.ticker = pg.ticker AND pg.rn = 1
    WHERE gm.sport IN ('NHLSPREAD','NFLTD','NBASPREAD','NHL','NBA','NFLGW','NFLTT','NFLSPREAD',
                        'UCL','WNBA','ATP','NCAAMB','CFB',
                        'NBA_PTS','NBA_3PT','NBA_REB','NBA_AST')
      AND pg.yp BETWEEN 60 AND 90
    GROUP BY yr, mo, gm.sport
    ORDER BY yr, mo
""").fetchdf()

# Summarize: how many trades per month do we get with game winners vs game winners + props?
import pandas as pd
monthly['ym'] = monthly['yr'].astype(int).astype(str) + '-' + monthly['mo'].astype(int).astype(str).str.zfill(2)

game_sports = {'NHLSPREAD','NFLTD','NBASPREAD','NHL','NBA','NFLGW','NFLTT','NFLSPREAD','UCL','WNBA','ATP','NCAAMB','CFB'}
prop_sports = {'NBA_PTS','NBA_3PT','NBA_REB','NBA_AST'}

print(f"  {'Month':<8} {'GW Trades':>10} {'Prop Trades':>12} {'Combined':>10} {'GW NO%':>7} {'Prop NO%':>9}")
print(f"  {'-'*60}")

for ym in sorted(monthly['ym'].unique()):
    ym_data = monthly[monthly['ym'] == ym]
    gw = ym_data[ym_data['sport'].isin(game_sports)]
    pr = ym_data[ym_data['sport'].isin(prop_sports)]

    gw_trades = int(gw['trades'].sum())
    pr_trades = int(pr['trades'].sum())
    gw_wins = int(gw['no_wins'].sum())
    pr_wins = int(pr['no_wins'].sum())

    gw_pct = (gw_wins / gw_trades * 100) if gw_trades > 0 else 0
    pr_pct = (pr_wins / pr_trades * 100) if pr_trades > 0 else 0

    print(f"  {ym:<8} {gw_trades:>10} {pr_trades:>12} {gw_trades+pr_trades:>10} {gw_pct:>6.1f}% {pr_pct:>8.1f}%")

print()
print(f"  Runtime: {time.time()-t0:.1f}s")
print("=" * 70)
