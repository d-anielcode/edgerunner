"""Find all sport x price bucket combos with negative ROI to exclude."""
import duckdb

con = duckdb.connect()
mp = 'data/trevorjs/markets-*.parquet'
tp = 'data/trevorjs/trades-*.parquet'

ALL_PREFIXES = {
    'KXNBAGAME': 'NBA', 'KXNHLGAME': 'NHL', 'KXUCLGAME': 'UCL',
    'KXWNBAGAME': 'WNBA', 'KXATPMATCH': 'ATP', 'KXNFLANYTD': 'NFLTD',
    'KXNHLSPREAD': 'NHLSPREAD', 'KXNBASPREAD': 'NBASPREAD',
    'KXNFLSPREAD': 'NFLSPREAD', 'KXMLBGAME': 'MLB',
    'KXNFLTEAMTOTAL': 'NFLTT', 'KXCFBGAME': 'CFB',
    'KXNBAPTS': 'NBA_PTS', 'KXNBA3PT': 'NBA_3PT',
    'KXNBAREB': 'NBA_REB', 'KXNBAAST': 'NBA_AST',
    'KXNCAAMBGAME': 'NCAAMB', 'KXNCAAWBGAME': 'NCAAWB',
    'KXWTAMATCH': 'WTA', 'KXNFLGAME': 'NFLGW',
    'KXNFLFIRSTTD': 'NFL_1ST_TD', 'KXNHLGOAL': 'NHL_GOAL',
    'KXNHLAST': 'NHL_AST', 'KXNHLPTS': 'NHL_PTS',
    'KXNBASTL': 'NBA_STL', 'KXNFLRECYDS': 'NFL_REC_YDS',
    'KXNCAAFTOTAL': 'NCAAF_TOTAL', 'KXCS': 'CS2',
    'KXMLSGAME': 'MLS', 'KXEUROLEAGUEGAME': 'EUROLEAGUE',
    'KXLOLGAME': 'LOL_GAME', 'KXDARTSMATCH': 'DARTS',
    'KXEREDIVISIEGAME': 'EREDIVISIE',
}

case_parts = [f"WHEN event_ticker LIKE '{p}%' THEN '{s}'" for p, s in ALL_PREFIXES.items()]
case_stmt = ' '.join(case_parts)
like_clauses = ' OR '.join(f"event_ticker LIKE '{p}%'" for p in ALL_PREFIXES.keys())

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
    )
    SELECT gm.sport,
        CASE
            WHEN pg.yp BETWEEN 55 AND 64 THEN '55-64'
            WHEN pg.yp BETWEEN 65 AND 74 THEN '65-74'
            WHEN pg.yp BETWEEN 75 AND 84 THEN '75-84'
            WHEN pg.yp BETWEEN 85 AND 95 THEN '85-95'
        END as bucket,
        COUNT(*) as n,
        ROUND(100.0 * SUM(CASE WHEN gm.result = 'no' THEN 1 ELSE 0 END) / COUNT(*), 1) as no_wr,
        ROUND(AVG(CASE WHEN gm.result = 'no' THEN 1.0 - (100 - pg.yp)/100.0
                       ELSE -(100 - pg.yp)/100.0 END), 4) as avg_pnl
    FROM gm
    JOIN pregame pg ON gm.ticker = pg.ticker AND pg.rn = 1
    WHERE gm.sport IS NOT NULL AND pg.yp BETWEEN 55 AND 95
    GROUP BY gm.sport, bucket
    HAVING COUNT(*) >= 15
    ORDER BY avg_pnl ASC
""").fetchdf()

print("=" * 70)
print("  NEGATIVE EDGE BUCKETS — MUST EXCLUDE")
print("  (Sport x Price combos where avg P&L < 0, n >= 15)")
print("=" * 70)
print()

neg = df[df['avg_pnl'] < 0]
print(f"{'Sport':<15} {'Bucket':>8} {'N':>5} {'NO WR':>6} {'AvgPnL':>8}")
print("-" * 45)
for _, row in neg.iterrows():
    print(f"{row['sport']:<15} {row['bucket']:>8} {int(row['n']):>5} {row['no_wr']:>5.1f}% ${row['avg_pnl']:>7.4f}")

print(f"\nTotal: {len(neg)} buckets to exclude")
print()

# Count how many trades we'd skip
neg_trades = int(neg['n'].sum())
total_trades = int(df['n'].sum())
print(f"Trades excluded: {neg_trades} / {total_trades} ({neg_trades/total_trades*100:.1f}%)")
print()

# Show the WORST offenders
print("TOP 10 WORST BUCKETS (biggest money losers):")
print()
worst = neg.sort_values('avg_pnl').head(10)
for _, row in worst.iterrows():
    est_loss = row['avg_pnl'] * row['n']
    print(f"  {row['sport']:<15} {row['bucket']:>8}  n={int(row['n']):>4}  NO WR={row['no_wr']:>5.1f}%  est loss=${est_loss:.0f}")

print()
print("=" * 70)
print("  POSITIVE EDGE BUCKETS — KEEP")
print("=" * 70)
print()

pos = df[df['avg_pnl'] >= 0].sort_values(['sport', 'bucket'])
print(f"{'Sport':<15} {'Bucket':>8} {'N':>5} {'NO WR':>6} {'AvgPnL':>8}")
print("-" * 45)
for _, row in pos.iterrows():
    print(f"{row['sport']:<15} {row['bucket']:>8} {int(row['n']):>5} {row['no_wr']:>5.1f}% ${row['avg_pnl']:>+7.4f}")

print(f"\nTotal: {len(pos)} profitable buckets")
