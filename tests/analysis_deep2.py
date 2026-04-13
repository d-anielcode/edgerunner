"""
Deep analysis round 2:
- Volume vs edge (do thin markets have more edge?)
- Day of week effect
- Combined portfolio equity curve simulation
- Correlation between game winners and props on same game
- NFLGW / NFLSPREAD — should they stay or go?
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
    'KXUCLGAME': 'UCL', 'KXWNBAGAME': 'WNBA',
    'KXATPMATCH': 'ATP', 'KXCFBGAME': 'CFB',
    'KXNBAPTS': 'NBA_PTS', 'KXNBAREB': 'NBA_REB',
    'KXNBAAST': 'NBA_AST', 'KXNBA3PT': 'NBA_3PT',
}

case_parts = [f"WHEN event_ticker LIKE '{p}%' THEN '{s}'" for p, s in SPORT_FROM_PREFIX.items()]
case_stmt = ' '.join(case_parts)
like_clauses = ' OR '.join(f"event_ticker LIKE '{p}%'" for p in SPORT_FROM_PREFIX.keys())

print("=" * 70)
print("  DEEP ANALYSIS ROUND 2")
print("=" * 70)
print()

# ── ANALYSIS 5: Volume tier vs NO win rate ──
print("=== 5. VOLUME vs EDGE — Do Thin Markets Have More Edge? ===")
print()

vol_analysis = con.sql(f"""
    WITH gm AS (
        SELECT ticker, result, event_ticker, close_time, volume,
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
            WHEN gm.volume < 1000 THEN '<1K'
            WHEN gm.volume < 10000 THEN '1K-10K'
            WHEN gm.volume < 100000 THEN '10K-100K'
            WHEN gm.volume < 1000000 THEN '100K-1M'
            ELSE '>1M'
        END as vol_tier,
        COUNT(*) as n,
        ROUND(100.0 * SUM(CASE WHEN gm.result = 'no' THEN 1 ELSE 0 END) / COUNT(*), 1) as no_pct
    FROM gm
    JOIN pregame pg ON gm.ticker = pg.ticker AND pg.rn = 1
    WHERE gm.sport IN ('NHLSPREAD','NFLTD','NBASPREAD','NHL','NBA_3PT','NBA_PTS')
      AND pg.yp BETWEEN 60 AND 90
    GROUP BY gm.sport, vol_tier
    HAVING COUNT(*) >= 10
    ORDER BY gm.sport, vol_tier
""").fetchdf()

for sport in ['NHLSPREAD', 'NFLTD', 'NBASPREAD', 'NHL', 'NBA_3PT', 'NBA_PTS']:
    subset = vol_analysis[vol_analysis['sport'] == sport]
    if len(subset) == 0:
        continue
    print(f"  {sport}:")
    for _, row in subset.iterrows():
        vt = row['vol_tier']
        n = int(row['n'])
        pct = float(row['no_pct'])
        bar = '#' * int(pct / 2)
        print(f"    {vt:>8}: {n:5d} mkts, NO wins {pct:5.1f}% {bar}")
    print()


# ── ANALYSIS 6: Day of week ──
print("=== 6. DAY OF WEEK EFFECT ===")
print()

dow_analysis = con.sql(f"""
    WITH gm AS (
        SELECT ticker, result, close_time,
               CASE {case_stmt} END as sport,
               EXTRACT(DOW FROM close_time) as dow
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
        gm.dow,
        CASE gm.dow
            WHEN 0 THEN 'Sun' WHEN 1 THEN 'Mon' WHEN 2 THEN 'Tue'
            WHEN 3 THEN 'Wed' WHEN 4 THEN 'Thu' WHEN 5 THEN 'Fri' WHEN 6 THEN 'Sat'
        END as day_name,
        COUNT(*) as n,
        ROUND(100.0 * SUM(CASE WHEN gm.result = 'no' THEN 1 ELSE 0 END) / COUNT(*), 1) as no_pct,
        ROUND(AVG(CASE WHEN gm.result = 'no' THEN 1.0 - (100.0 - pg.yp)/100.0
                       ELSE -(100.0 - pg.yp)/100.0 END), 4) as avg_pnl
    FROM gm
    JOIN pregame pg ON gm.ticker = pg.ticker AND pg.rn = 1
    WHERE gm.sport IN ('NHLSPREAD','NFLTD','NBASPREAD','NHL','NBA','NBA_3PT','NBA_PTS','NBA_REB','NBA_AST')
      AND pg.yp BETWEEN 60 AND 90
    GROUP BY gm.dow, day_name
    ORDER BY gm.dow
""").fetchdf()

print(f"  {'Day':<5} {'Trades':>7} {'NO%':>6} {'AvgPnL':>8}")
print(f"  {'-'*30}")
for _, row in dow_analysis.iterrows():
    day = row['day_name']
    n = int(row['n'])
    pct = float(row['no_pct'])
    avg_pnl = float(row['avg_pnl'])
    bar = '#' * max(0, int((avg_pnl + 0.05) * 100))
    print(f"  {day:<5} {n:>7} {pct:>5.1f}% ${avg_pnl:>+7.4f} {bar}")

print()


# ── ANALYSIS 7: What happens if we enter 6-24h before close? ──
print("=== 7. SIMULATED PORTFOLIO: 6-24h Entry vs 2h Entry ===")
print("   (Comparing early entry vs current agent timing)")
print()

entry_comparison = con.sql(f"""
    WITH gm AS (
        SELECT ticker, result, close_time,
               CASE {case_stmt} END as sport
        FROM '{mp}'
        WHERE ({like_clauses}) AND status='finalized' AND result IN ('yes','no')
    ),
    early_entry AS (
        SELECT t.ticker, t.yes_price as yp,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time DESC) as rn
        FROM '{tp}' t
        JOIN gm ON t.ticker = gm.ticker
        WHERE t.created_time < gm.close_time - INTERVAL 6 HOURS
          AND t.created_time > gm.close_time - INTERVAL 24 HOURS
    ),
    late_entry AS (
        SELECT t.ticker, t.yes_price as yp,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time DESC) as rn
        FROM '{tp}' t
        JOIN gm ON t.ticker = gm.ticker
        WHERE t.created_time < gm.close_time - INTERVAL 2 HOURS
          AND t.created_time > gm.close_time - INTERVAL 6 HOURS
    )
    SELECT
        gm.sport,
        'early_6-24h' as timing,
        COUNT(*) as n,
        ROUND(100.0 * SUM(CASE WHEN gm.result = 'no' THEN 1 ELSE 0 END) / COUNT(*), 1) as no_pct,
        ROUND(AVG(CASE WHEN gm.result = 'no' THEN 1.0 - (100.0 - ee.yp)/100.0
                       ELSE -(100.0 - ee.yp)/100.0 END), 4) as avg_pnl
    FROM gm
    JOIN early_entry ee ON gm.ticker = ee.ticker AND ee.rn = 1
    WHERE gm.sport IN ('NHLSPREAD','NFLTD','NBASPREAD','NHL')
      AND ee.yp BETWEEN 60 AND 90
    GROUP BY gm.sport

    UNION ALL

    SELECT
        gm.sport,
        'late_2-6h' as timing,
        COUNT(*) as n,
        ROUND(100.0 * SUM(CASE WHEN gm.result = 'no' THEN 1 ELSE 0 END) / COUNT(*), 1) as no_pct,
        ROUND(AVG(CASE WHEN gm.result = 'no' THEN 1.0 - (100.0 - le.yp)/100.0
                       ELSE -(100.0 - le.yp)/100.0 END), 4) as avg_pnl
    FROM gm
    JOIN late_entry le ON gm.ticker = le.ticker AND le.rn = 1
    WHERE gm.sport IN ('NHLSPREAD','NFLTD','NBASPREAD','NHL')
      AND le.yp BETWEEN 60 AND 90
    GROUP BY gm.sport

    ORDER BY sport, timing
""").fetchdf()

print(f"  {'Sport':<12} {'Timing':<14} {'Trades':>6} {'NO%':>6} {'AvgPnL':>8} {'Edge Diff':>10}")
print(f"  {'-'*60}")
for sport in ['NHLSPREAD', 'NFLTD', 'NBASPREAD', 'NHL']:
    subset = entry_comparison[entry_comparison['sport'] == sport]
    if len(subset) < 2:
        # Print whatever we have
        for _, row in subset.iterrows():
            print(f"  {sport:<12} {row['timing']:<14} {int(row['n']):>6} {float(row['no_pct']):>5.1f}% ${float(row['avg_pnl']):>+7.4f}")
        continue
    early = subset[subset['timing'] == 'early_6-24h']
    late = subset[subset['timing'] == 'late_2-6h']
    for _, row in subset.iterrows():
        timing = row['timing']
        n = int(row['n'])
        pct = float(row['no_pct'])
        pnl = float(row['avg_pnl'])
        diff = ""
        if timing == 'early_6-24h' and len(late) > 0:
            late_pnl = float(late.iloc[0]['avg_pnl'])
            d = pnl - late_pnl
            diff = f"  {d:>+.4f}"
        print(f"  {sport:<12} {timing:<14} {n:>6} {pct:>5.1f}% ${pnl:>+7.4f}{diff}")
    print()


# ── ANALYSIS 8: Correlation risk — same-game props + game winners ──
print("=== 8. CORRELATION: Props + Game Winners on Same Game ===")
print()

# How often do NBA game winner NO and NBA prop NO co-win or co-lose?
corr_analysis = con.sql(f"""
    WITH gm AS (
        SELECT ticker, result, event_ticker, close_time,
               CASE {case_stmt} END as sport
        FROM '{mp}'
        WHERE ({like_clauses}) AND status='finalized' AND result IN ('yes','no')
    ),
    -- Extract game key from ticker (date + teams)
    nba_gw AS (
        SELECT ticker, result,
               REGEXP_EXTRACT(ticker, '\\d{{2}}[A-Z]{{3}}\\d{{2}}([A-Z]{{3,8}})-', 1) as game_key
        FROM gm WHERE sport = 'NBA'
    ),
    nba_props AS (
        SELECT ticker, result, sport,
               REGEXP_EXTRACT(ticker, '\\d{{2}}[A-Z]{{3}}\\d{{2}}([A-Z]{{3,8}})-', 1) as game_key
        FROM gm WHERE sport IN ('NBA_PTS','NBA_3PT','NBA_REB','NBA_AST')
    )
    SELECT
        np.sport as prop_type,
        COUNT(*) as pairs,
        -- Both NO win (underdog wins + player Under hits)
        SUM(CASE WHEN gw.result = 'no' AND np.result = 'no' THEN 1 ELSE 0 END) as both_no,
        -- Both YES win (favorite wins + player Over hits)
        SUM(CASE WHEN gw.result = 'yes' AND np.result = 'yes' THEN 1 ELSE 0 END) as both_yes,
        -- Split: GW=NO but prop=YES
        SUM(CASE WHEN gw.result = 'no' AND np.result = 'yes' THEN 1 ELSE 0 END) as gw_no_prop_yes,
        -- Split: GW=YES but prop=NO
        SUM(CASE WHEN gw.result = 'yes' AND np.result = 'no' THEN 1 ELSE 0 END) as gw_yes_prop_no
    FROM nba_gw gw
    JOIN nba_props np ON gw.game_key = np.game_key
    GROUP BY np.sport
    ORDER BY np.sport
""").fetchdf()

if len(corr_analysis) > 0:
    print(f"  {'Prop':<10} {'Pairs':>6} {'Both NO':>8} {'Both YES':>9} {'Split1':>7} {'Split2':>7} {'Corr':>6}")
    print(f"  {'-'*55}")
    for _, row in corr_analysis.iterrows():
        pairs = int(row['pairs'])
        both_no = int(row['both_no'])
        both_yes = int(row['both_yes'])
        s1 = int(row['gw_no_prop_yes'])
        s2 = int(row['gw_yes_prop_no'])
        # Phi coefficient approximation
        agreement = both_no + both_yes
        disagreement = s1 + s2
        corr = (agreement - disagreement) / pairs if pairs > 0 else 0
        print(f"  {row['prop_type']:<10} {pairs:>6} {both_no:>8} {both_yes:>9} {s1:>7} {s2:>7} {corr:>+5.2f}")
    print()
    print("  Corr > 0: props and game winners tend to co-move (risky to hold both)")
    print("  Corr ~ 0: independent (safe to diversify)")
    print("  Corr < 0: anti-correlated (natural hedge)")
else:
    print("  No matching game keys found for correlation analysis")

print()
print(f"  Runtime: {time.time()-t0:.1f}s")
print("=" * 70)
