import duckdb
import re
import sys
# Force UTF-8 output on Windows
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

con = duckdb.connect()

markets_path = "data/trevorjs/markets-*.parquet"
trades_path  = "data/trevorjs/trades-*.parquet"

# ─────────────────────────────────────────────────────────────────────────────
# BUILD BASE TABLE
# first trade per ticker (rn=1), 2024-01-01+, finalized, NBA2D only
# ─────────────────────────────────────────────────────────────────────────────
con.execute(f"""
CREATE OR REPLACE TABLE base AS
WITH first_trades AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY ticker
               ORDER BY created_time
           ) AS rn
    FROM read_parquet('{trades_path}')
    WHERE ticker LIKE 'KXNBA2D%'
      AND created_time >= '2024-01-01'
),
ft AS (
    SELECT ticker, yes_price, created_time
    FROM first_trades
    WHERE rn = 1
),
mkt AS (
    SELECT ticker, event_ticker, result, volume, close_time, title
    FROM read_parquet('{markets_path}')
    WHERE event_ticker LIKE 'KXNBA2D%'
      AND result IN ('yes','no')
)
SELECT
    m.ticker,
    m.event_ticker,
    m.result,
    m.volume,
    m.close_time,
    m.title,
    ft.yes_price,
    ft.created_time                         AS first_trade_time,
    -- NO cost = (100 - yes_price) / 100
    (100.0 - ft.yes_price) / 100.0          AS no_cost,
    -- payout if NO wins = $1 per contract
    CASE WHEN m.result = 'no' THEN 1.0 ELSE 0.0 END AS payout,
    -- ROI on NO bet: (payout - no_cost) / no_cost
    CASE WHEN m.result = 'no'
         THEN (1.0 - (100.0 - ft.yes_price)/100.0) / ((100.0 - ft.yes_price)/100.0)
         ELSE -1.0
    END AS roi,
    -- 5c price bucket (floor to nearest 5)
    FLOOR(ft.yes_price / 5.0) * 5           AS bucket5,
    -- calendar month
    STRFTIME(ft.created_time, '%Y-%m')      AS month,
    -- extract player info from ticker
    REGEXP_EXTRACT(m.ticker, 'KXNBA2D-\d{7}[A-Z]+-([A-Z]{3})(\w+)', 2) AS player_part
FROM mkt m
JOIN ft ON m.ticker = ft.ticker
""")

total = con.execute("SELECT COUNT(*) FROM base").fetchone()[0]
print(f"\n{'='*70}")
print(f"NBA Double-Double (KXNBA2D) — Fade Favorites (Buy NO) Analysis")
print(f"Total finalized markets with first-trade data: {total}")
print(f"Date range: 2024-01-01 onward")
print(f"{'='*70}")

# ─────────────────────────────────────────────────────────────────────────────
# 1. PER-PRICE BUCKET (5c buckets, 55-95)
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n{'='*70}")
print("1. PER-PRICE BUCKET ANALYSIS (5c buckets, yes_price 55-95)")
print(f"{'='*70}")
print(f"  Bucket means 'favorite was priced at YES=[bucket] to [bucket+4]'")
print(f"  We buy NO at cost = (100 - yes_price)/100")
print()

bucket = con.execute("""
SELECT
    bucket5                                 AS yes_bucket,
    COUNT(*)                                AS markets,
    ROUND(AVG(CASE WHEN result='no' THEN 100.0 ELSE 0.0 END), 1) AS no_win_pct,
    ROUND(AVG(no_cost) * 100, 1)           AS avg_no_cost_cents,
    ROUND(AVG(roi) * 100, 2)               AS avg_roi_pct,
    ROUND(SUM(payout - no_cost), 2)        AS total_pnl_per_contract,
    MIN(yes_price) || '-' || MAX(yes_price) AS price_range
FROM base
WHERE yes_price BETWEEN 55 AND 99
GROUP BY bucket5
ORDER BY bucket5
""").fetchdf()

print(f"{'Bucket':>8} {'Markets':>8} {'NO Win%':>9} {'Avg Cost¢':>10} {'ROI%':>8} {'Total P&L':>10}")
print("-" * 58)
for _, r in bucket.iterrows():
    flag = " ✓" if r['avg_roi_pct'] > 0 else " ✗"
    print(f"  {int(r['yes_bucket']):>3}-{int(r['yes_bucket'])+4:<3} "
          f"{int(r['markets']):>8} "
          f"{r['no_win_pct']:>8.1f}% "
          f"{r['avg_no_cost_cents']:>9.1f}¢ "
          f"{r['avg_roi_pct']:>7.2f}%{flag} "
          f"{r['total_pnl_per_contract']:>10.2f}")

# ─────────────────────────────────────────────────────────────────────────────
# 2. MONTHLY BREAKDOWN
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n{'='*70}")
print("2. MONTHLY BREAKDOWN (all price ranges)")
print(f"{'='*70}")

monthly = con.execute("""
SELECT
    month,
    COUNT(*)                                    AS markets,
    ROUND(AVG(CASE WHEN result='no' THEN 100.0 ELSE 0.0 END), 1) AS no_win_pct,
    ROUND(AVG(roi) * 100, 2)                    AS avg_roi_pct,
    ROUND(SUM(payout - no_cost), 2)             AS total_pnl
FROM base
GROUP BY month
ORDER BY month
""").fetchdf()

print(f"{'Month':>8} {'Markets':>8} {'NO Win%':>9} {'ROI%':>8} {'Total P&L':>10} {'Status':>8}")
print("-" * 60)
for _, r in monthly.iterrows():
    status = "PROFIT" if r['avg_roi_pct'] > 0 else "LOSS"
    print(f"  {r['month']:>7} "
          f"{int(r['markets']):>8} "
          f"{r['no_win_pct']:>8.1f}% "
          f"{r['avg_roi_pct']:>7.2f}% "
          f"{r['total_pnl']:>10.2f}  "
          f"{status:>8}")

# ─────────────────────────────────────────────────────────────────────────────
# 3. VOLUME FILTER TEST
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n{'='*70}")
print("3. VOLUME FILTER TEST")
print(f"{'='*70}")

vol_filters = [
    ("All volumes",   "1=1"),
    ("vol > 5000",    "volume > 5000"),
    ("vol > 2000",    "volume > 2000"),
    ("vol > 1000",    "volume > 1000"),
    ("vol 500-1000",  "volume BETWEEN 500 AND 1000"),
    ("vol < 500",     "volume < 500"),
]

print(f"{'Filter':>14} {'Markets':>8} {'NO Win%':>9} {'ROI%':>8} {'Total P&L':>10}")
print("-" * 54)
for label, flt in vol_filters:
    r = con.execute(f"""
        SELECT
            COUNT(*)                                    AS markets,
            AVG(CASE WHEN result='no' THEN 100.0 ELSE 0.0 END) AS no_win_pct,
            AVG(roi) * 100                              AS avg_roi_pct,
            SUM(payout - no_cost)                       AS total_pnl
        FROM base
        WHERE {flt}
    """).fetchone()
    n, win_pct, roi, pnl = r
    if n == 0 or win_pct is None:
        print(f"  {label:>13} {int(n):>8}  (no data)")
    else:
        print(f"  {label:>13} {int(n):>8} {win_pct:>8.1f}% {roi:>7.2f}% {pnl:>10.2f}")

# Also split by bucket + volume
print(f"\n  --- Volume filter applied to 55-95 bucket range only ---")
print(f"{'Filter':>14} {'Markets':>8} {'NO Win%':>9} {'ROI%':>8} {'Total P&L':>10}")
print("-" * 54)
for label, flt in vol_filters:
    r = con.execute(f"""
        SELECT
            COUNT(*)                                    AS markets,
            AVG(CASE WHEN result='no' THEN 100.0 ELSE 0.0 END) AS no_win_pct,
            AVG(roi) * 100                              AS avg_roi_pct,
            SUM(payout - no_cost)                       AS total_pnl
        FROM base
        WHERE yes_price BETWEEN 55 AND 99
          AND {flt}
    """).fetchone()
    n, win_pct, roi, pnl = r
    if n == 0 or win_pct is None:
        print(f"  {label:>13} {int(n):>8}  (no data)")
    else:
        print(f"  {label:>13} {int(n):>8} {win_pct:>8.1f}% {roi:>7.2f}% {pnl:>10.2f}")

# ─────────────────────────────────────────────────────────────────────────────
# 4. PRICE RANGE DEEP DIVE: 55-70, per cent
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n{'='*70}")
print("4. DEEP DIVE: yes_price 55-70, PER CENT")
print(f"{'='*70}")
print(f"  (favorite priced at YES=55¢ to YES=70¢)")
print()

per_cent = con.execute("""
SELECT
    yes_price,
    COUNT(*)                                            AS markets,
    ROUND(AVG(CASE WHEN result='no' THEN 100.0 ELSE 0.0 END),1) AS no_win_pct,
    ROUND((100.0 - yes_price), 1)                       AS no_cost_cents,
    ROUND(AVG(roi)*100, 2)                              AS avg_roi_pct,
    ROUND(SUM(payout-no_cost), 2)                       AS total_pnl
FROM base
WHERE yes_price BETWEEN 55 AND 70
GROUP BY yes_price
ORDER BY yes_price
""").fetchdf()

print(f"{'YES¢':>6} {'Markets':>8} {'NO Win%':>9} {'NO Cost¢':>9} {'ROI%':>8} {'Total P&L':>10}")
print("-" * 55)
for _, r in per_cent.iterrows():
    flag = " ✓" if r['avg_roi_pct'] > 0 else " ✗"
    print(f"  {int(r['yes_price']):>4}¢ "
          f"{int(r['markets']):>8} "
          f"{r['no_win_pct']:>8.1f}% "
          f"{r['no_cost_cents']:>8.1f}¢ "
          f"{r['avg_roi_pct']:>7.2f}%{flag} "
          f"{r['total_pnl']:>10.2f}")

# ─────────────────────────────────────────────────────────────────────────────
# 5. PLAYER ANALYSIS
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n{'='*70}")
print("5. PLAYER ANALYSIS")
print(f"{'='*70}")

# ticker format: KXNBA2D-25DEC03LACATL-LACKLEONARDPF
# player part is after second hyphen, after team code (3 chars)
con.execute("""
CREATE OR REPLACE TABLE base2 AS
SELECT *,
    -- ticker = KXNBA2D-25DEC03LACATL-LACKLEONARDPF
    -- segment after last '-' e.g. LACKLEONARDPF
    -- first 3 chars = team, rest = player+position
    SPLIT_PART(ticker, '-', 3)                                      AS player_segment,
    SUBSTRING(SPLIT_PART(ticker, '-', 3), 1, 3)                     AS team_abbr,
    -- remove last 2 chars (position code like PF, SG, C, etc.)
    SUBSTRING(SPLIT_PART(ticker, '-', 3), 4,
              LENGTH(SPLIT_PART(ticker, '-', 3)) - 5)               AS raw_player
FROM base
""")

# Show sample player parsing
print("\n  Sample ticker -> player extraction:")
samp = con.execute("""
    SELECT ticker, team_abbr, raw_player
    FROM base2
    LIMIT 8
""").fetchdf()
print(samp.to_string(index=False))

# Top players by market count
print(f"\n  --- Top 20 players by NO-bet profitability (min 5 markets) ---")
print(f"{'Player':>20} {'Team':>5} {'Markets':>8} {'NO Win%':>9} {'ROI%':>8} {'Total P&L':>10}")
print("-" * 68)

players = con.execute("""
SELECT
    raw_player                                              AS player,
    team_abbr                                               AS team,
    COUNT(*)                                                AS markets,
    ROUND(AVG(CASE WHEN result='no' THEN 100.0 ELSE 0.0 END),1) AS no_win_pct,
    ROUND(AVG(roi)*100, 2)                                  AS avg_roi_pct,
    ROUND(SUM(payout-no_cost), 2)                           AS total_pnl
FROM base2
WHERE raw_player IS NOT NULL AND raw_player != ''
GROUP BY raw_player, team_abbr
HAVING COUNT(*) >= 5
ORDER BY avg_roi_pct DESC
LIMIT 20
""").fetchdf()

for _, r in players.iterrows():
    flag = " ✓" if r['avg_roi_pct'] > 0 else " ✗"
    print(f"  {r['player']:>20} {r['team']:>5} "
          f"{int(r['markets']):>8} "
          f"{r['no_win_pct']:>8.1f}% "
          f"{r['avg_roi_pct']:>7.2f}%{flag} "
          f"{r['total_pnl']:>10.2f}")

print(f"\n  --- Bottom 10 players (worst for NO-bet / best YES players) ---")
players_worst = con.execute("""
SELECT
    raw_player                                              AS player,
    team_abbr                                               AS team,
    COUNT(*)                                                AS markets,
    ROUND(AVG(CASE WHEN result='no' THEN 100.0 ELSE 0.0 END),1) AS no_win_pct,
    ROUND(AVG(roi)*100, 2)                                  AS avg_roi_pct,
    ROUND(SUM(payout-no_cost), 2)                           AS total_pnl
FROM base2
WHERE raw_player IS NOT NULL AND raw_player != ''
GROUP BY raw_player, team_abbr
HAVING COUNT(*) >= 5
ORDER BY avg_roi_pct ASC
LIMIT 10
""").fetchdf()

print(f"{'Player':>20} {'Team':>5} {'Markets':>8} {'NO Win%':>9} {'ROI%':>8} {'Total P&L':>10}")
print("-" * 68)
for _, r in players_worst.iterrows():
    print(f"  {r['player']:>20} {r['team']:>5} "
          f"{int(r['markets']):>8} "
          f"{r['no_win_pct']:>8.1f}% "
          f"{r['avg_roi_pct']:>7.2f}%  "
          f"{r['total_pnl']:>10.2f}")

# ── Summary stats ────────────────────────────────────────────────────────────
print(f"\n{'='*70}")
print("OVERALL SUMMARY")
print(f"{'='*70}")
overall = con.execute("""
SELECT
    COUNT(*)                                            AS total_markets,
    ROUND(AVG(CASE WHEN result='no' THEN 100.0 ELSE 0.0 END),1) AS overall_no_win_pct,
    ROUND(AVG(roi)*100, 2)                              AS overall_roi_pct,
    ROUND(SUM(payout-no_cost), 2)                       AS total_pnl,
    ROUND(AVG(yes_price), 1)                            AS avg_yes_price,
    MIN(yes_price) || ' - ' || MAX(yes_price)           AS yes_price_range
FROM base
""").fetchone()
print(f"  Total markets:      {overall[0]}")
print(f"  Overall NO win%:    {overall[1]:.1f}%")
print(f"  Overall ROI (NO):   {overall[2]:.2f}%")
print(f"  Total P&L (1-unit): {overall[3]:.2f}")
print(f"  Avg YES price:      {overall[4]:.1f}¢")
print(f"  YES price range:    {overall[5]}¢")

# Best bucket summary
print(f"\n  Best 5c buckets for fading (by ROI%):")
best = con.execute("""
SELECT bucket5, COUNT(*) AS n,
    ROUND(AVG(CASE WHEN result='no' THEN 100.0 ELSE 0.0 END),1) AS no_win_pct,
    ROUND(AVG(roi)*100, 2) AS roi_pct
FROM base
WHERE yes_price BETWEEN 55 AND 99
GROUP BY bucket5
ORDER BY roi_pct DESC
LIMIT 5
""").fetchdf()
for _, r in best.iterrows():
    print(f"    YES={int(r['bucket5'])}-{int(r['bucket5'])+4}¢  "
          f"n={int(r['n'])}  "
          f"NO win={r['no_win_pct']:.1f}%  "
          f"ROI={r['roi_pct']:.2f}%")

print()
