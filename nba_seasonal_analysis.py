"""
NBA Seasonal Edge Decay Analysis -- DuckDB edition
Compares YES win rates across months and seasons for NBA game winner markets.
Research only -- no trading signals generated.

Data available: Apr 2025 - Jan 2026 (trevorjs parquet snapshot).
  2024-25 season: Apr-Jun 2025 only (late playoffs/finals)
  2025-26 season: Oct-Dec 2025 + Jan 2026 (early-mid season)
"""

import sys
import io
import duckdb
import pandas as pd

# Force UTF-8 output so box-drawing chars don't crash on Windows cp1252
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

MARKETS_GLOB = "C:/Users/dcho0/Documents/edgerunner/data/trevorjs/markets-*.parquet"
TRADES_GLOB  = "C:/Users/dcho0/Documents/edgerunner/data/trevorjs/trades-*.parquet"

con = duckdb.connect()

# ── Build main analysis dataset ───────────────────────────────────────────────
print("Building NBA game winner analysis via DuckDB ...")

analysis_sql = f"""
WITH
nba_markets AS (
    SELECT
        ticker,
        result,
        close_time,
        YEAR(close_time)  AS yr,
        MONTH(close_time) AS mo,
        CASE
            -- 2024-25 season data available in this snapshot (playoffs/finals only)
            WHEN YEAR(close_time) = 2025 AND MONTH(close_time) = 4  THEN '2024-25 Apr 2025 (playoffs)'
            WHEN YEAR(close_time) = 2025 AND MONTH(close_time) = 5  THEN '2024-25 May 2025 (conf finals)'
            WHEN YEAR(close_time) = 2025 AND MONTH(close_time) = 6  THEN '2024-25 Jun 2025 (NBA Finals)'
            -- 2025-26 season
            WHEN YEAR(close_time) = 2025 AND MONTH(close_time) = 10 THEN '2025-26 Oct 2025 (early)'
            WHEN YEAR(close_time) = 2025 AND MONTH(close_time) = 11 THEN '2025-26 Nov 2025 (early)'
            WHEN YEAR(close_time) = 2025 AND MONTH(close_time) = 12 THEN '2025-26 Dec 2025 (early)'
            WHEN YEAR(close_time) = 2026 AND MONTH(close_time) = 1  THEN '2025-26 Jan 2026 (mid)'
            ELSE NULL
        END AS season_month
    FROM read_parquet('{MARKETS_GLOB}')
    WHERE ticker LIKE '%KXNBAGAME%'
      AND result IN ('yes', 'no')
),
first_trades AS (
    SELECT ticker, yes_price
    FROM (
        SELECT
            ticker,
            yes_price,
            ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY created_time ASC) AS rn
        FROM read_parquet('{TRADES_GLOB}')
        WHERE ticker LIKE '%KXNBAGAME%'
    ) t
    WHERE rn = 1
),
joined AS (
    SELECT
        m.ticker,
        m.result,
        m.season_month,
        m.yr,
        m.mo,
        f.yes_price AS yes_price_c,   -- integer cents 1-99
        CASE WHEN m.result = 'yes' THEN 1 ELSE 0 END AS yes_win,
        CASE WHEN m.result = 'no'  THEN 1 ELSE 0 END AS no_win,
        CASE
            WHEN f.yes_price BETWEEN 55 AND 59 THEN '55-59c'
            WHEN f.yes_price BETWEEN 60 AND 64 THEN '60-64c'
            WHEN f.yes_price BETWEEN 65 AND 69 THEN '65-69c'
            WHEN f.yes_price BETWEEN 70 AND 74 THEN '70-74c'
            WHEN f.yes_price BETWEEN 75 AND 79 THEN '75-79c'
            WHEN f.yes_price BETWEEN 80 AND 84 THEN '80-84c'
            WHEN f.yes_price BETWEEN 85 AND 89 THEN '85-89c'
            WHEN f.yes_price >= 90             THEN '90+c'
            ELSE 'below-55c'
        END AS price_bucket
    FROM nba_markets m
    JOIN first_trades f ON m.ticker = f.ticker
    WHERE m.season_month IS NOT NULL
      AND f.yes_price BETWEEN 1 AND 99
)
SELECT * FROM joined
"""

print("  Running query ...")
df = con.execute(analysis_sql).fetchdf()
print(f"  Result: {len(df):,} market/trade pairs\n")

if len(df) == 0:
    print("No data returned. Check ticker matching.")
    sys.exit(1)

# NO ROI calculation
# Buy $1 of NO at (1 - yes_price/100).
# If NO wins: payout = 1 / no_price = 1 / (1 - yes_price/100), net = payout - 1
# If YES wins: lose $1, net = -1
# Per-market ROI = no_win / (1 - yes_price/100) - 1
df['no_price_frac'] = 1.0 - (df['yes_price_c'] / 100.0)
df['no_roi'] = df['no_win'] / df['no_price_frac'] - 1.0

# Season-month order
ORDER = [
    '2024-25 Apr 2025 (playoffs)',
    '2024-25 May 2025 (conf finals)',
    '2024-25 Jun 2025 (NBA Finals)',
    '2025-26 Oct 2025 (early)',
    '2025-26 Nov 2025 (early)',
    '2025-26 Dec 2025 (early)',
    '2025-26 Jan 2026 (mid)',
]

BUCKET_ORDER = ['55-59c','60-64c','65-69c','70-74c','75-79c','80-84c','85-89c','90+c']

SEP = "=" * 108
DASH = "-" * 108

# ──────────────────────────────────────────────────────────────────────────────
# 1. SEASON-MONTH DISTRIBUTION
# ──────────────────────────────────────────────────────────────────────────────
print(SEP)
print("DATA COVERAGE: NBA game winner markets with first-trade price (yes/no finalized)")
print(SEP)
dist = df.groupby('season_month').size().reindex(ORDER).fillna(0).astype(int)
for sm, n in dist.items():
    print(f"  {sm:<38} {n:>5} markets")
print()

# ──────────────────────────────────────────────────────────────────────────────
# 2. PER MONTH + PER BUCKET TABLE
# ──────────────────────────────────────────────────────────────────────────────
print(SEP)
print("NBA GAME WINNER -- YES WIN RATE BY SEASON-MONTH AND PRICE BUCKET")
print("(first trade price per ticker, finalized yes/no markets)")
print(SEP)

for sm in ORDER:
    sub = df[df['season_month'] == sm]
    if len(sub) == 0:
        print(f"\n  [{sm}]  -- no data in this snapshot")
        continue

    n_total   = len(sub)
    yes_rate  = sub['yes_win'].mean()
    no_roi    = sub['no_roi'].mean()

    print()
    print(DASH)
    print(f"  {sm}   |  N={n_total}  |  YES win rate: {yes_rate:.1%}  |  NO ROI: {no_roi:+.1%}")
    print(DASH)
    print(f"  {'Bucket':<12} {'N':>6}  {'YES%':>8}  {'NO ROI':>9}  {'Fair YES%':>10}  {'Edge (YES-Fair)':>16}")

    for bkt in BUCKET_ORDER:
        bsub = sub[sub['price_bucket'] == bkt]
        if len(bsub) == 0:
            continue
        lo   = int(bkt.rstrip('c+').split('-')[0])
        fair = (lo + 2.5) / 100.0
        ywr  = bsub['yes_win'].mean()
        nroi = bsub['no_roi'].mean()
        edge = ywr - fair
        n    = len(bsub)
        print(f"  {bkt:<12} {n:>6}  {ywr:>7.1%}  {nroi:>+8.1%}  {fair:>9.0%}  {edge:>+15.1%}")

# ──────────────────────────────────────────────────────────────────────────────
# 3. SUMMARY TABLE -- YES win rate progression through the season
# ──────────────────────────────────────────────────────────────────────────────
print()
print(SEP)
print("SUMMARY: YES WIN RATE PROGRESSION THROUGH THE NBA SEASON")
print(SEP)
print(f"  {'Season-Month':<38} {'N':>6}  {'YES Win%':>10}  {'NO ROI':>10}  Notes")
print(f"  {'-'*36:<38} {'-'*4:>6}  {'-'*8:>10}  {'-'*8:>10}")

prev_s = None
for sm in ORDER:
    sub = df[df['season_month'] == sm]
    s   = sm[:7]

    if prev_s and s != prev_s:
        print()
    prev_s = s

    if len(sub) == 0:
        print(f"  {sm:<38} {'(no data)':>6}")
        continue

    n    = len(sub)
    yr   = sub['yes_win'].mean()
    nroi = sub['no_roi'].mean()
    notes = ""
    if "playoffs" in sm or "Finals" in sm or "finals" in sm:
        notes = "  <- heavily favorites-skewed"
    print(f"  {sm:<38} {n:>6}  {yr:>9.1%}  {nroi:>+9.1%}{notes}")

# ──────────────────────────────────────────────────────────────────────────────
# 4. 2025-26 EARLY SEASON MONTH-BY-MONTH WITHIN 55-75c RANGE
#    (the key "edge" range for the model)
# ──────────────────────────────────────────────────────────────────────────────
print()
print(SEP)
print("EDGE RANGE ZOOM: 55-74c buckets only -- 2025-26 month-by-month")
print("(The question: does YES win rate drift up Jan vs Oct-Dec?)")
print(SEP)

edge_months = [
    '2025-26 Oct 2025 (early)',
    '2025-26 Nov 2025 (early)',
    '2025-26 Dec 2025 (early)',
    '2025-26 Jan 2026 (mid)',
]
edge_buckets = ['55-59c','60-64c','65-69c','70-74c']

print(f"  {'Season-Month':<32} {'Bucket':<12} {'N':>5}  {'YES%':>8}  {'NO ROI':>9}  {'Fair':>6}  {'Edge':>8}")
print(f"  {'-'*30:<32} {'-'*10:<12} {'-'*3:>5}  {'-'*6:>8}  {'-'*7:>9}  {'-'*4:>6}  {'-'*6:>8}")

for sm in edge_months:
    sub = df[df['season_month'] == sm]
    if len(sub) == 0:
        print(f"  {sm:<32} {'(no data)':>12}")
        continue
    for bkt in edge_buckets:
        bsub = sub[sub['price_bucket'] == bkt]
        if len(bsub) < 3:
            continue
        lo   = int(bkt.rstrip('c').split('-')[0])
        fair = (lo + 2.5) / 100.0
        ywr  = bsub['yes_win'].mean()
        nroi = bsub['no_roi'].mean()
        edge = ywr - fair
        n    = len(bsub)
        print(f"  {sm:<32} {bkt:<12} {n:>5}  {ywr:>7.1%}  {nroi:>+8.1%}  {fair:>5.0%}  {edge:>+7.1%}")
    # Row subtotal
    esub = sub[sub['price_bucket'].isin(edge_buckets)]
    if len(esub) > 0:
        print(f"  {'  -> 55-74c subtotal':<32} {'':12} {len(esub):>5}  {esub['yes_win'].mean():>7.1%}  {esub['no_roi'].mean():>+8.1%}")
    print()

# ──────────────────────────────────────────────────────────────────────────────
# 5. PRICE DISTRIBUTION BY MONTH
#    (check if market composition shifted -- fewer short favorites in Jan?)
# ──────────────────────────────────────────────────────────────────────────────
print(SEP)
print("PRICE COMPOSITION: % of markets in each bucket per month")
print("(sanity check -- are we comparing apples to apples?)")
print(SEP)

comp_months = [
    '2025-26 Oct 2025 (early)',
    '2025-26 Nov 2025 (early)',
    '2025-26 Dec 2025 (early)',
    '2025-26 Jan 2026 (mid)',
]
all_buckets = ['below-55c','55-59c','60-64c','65-69c','70-74c','75-79c','80-84c','85-89c','90+c']

header = f"  {'Bucket':<12}"
for sm in comp_months:
    label = sm.split(' ')[1] + ' ' + sm.split(' ')[2]  # e.g. "Oct 2025"
    header += f"  {label:>12}"
print(header)
print(f"  {'-'*10:<12}" + "  " + ("  " + "-"*10) * len(comp_months))

for bkt in all_buckets:
    row = f"  {bkt:<12}"
    for sm in comp_months:
        sub = df[df['season_month'] == sm]
        if len(sub) == 0:
            row += f"  {'n/a':>12}"
            continue
        n_bkt = (sub['price_bucket'] == bkt).sum()
        pct   = n_bkt / len(sub)
        row  += f"  {n_bkt:>4} ({pct:>5.1%})"
    print(row)

print()
print("Done.")
