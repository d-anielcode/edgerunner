import duckdb

# Use disk-backed database to handle large data without OOM
db = duckdb.connect("wnba_analysis.duckdb")
db.execute("PRAGMA memory_limit='2GB'")
db.execute("PRAGMA threads=4")

markets_path = "data/trevorjs/markets-*.parquet"
trades_path = "data/trevorjs/trades-*.parquet"

print("Building WNBA markets table...")
db.execute("""
    CREATE OR REPLACE TABLE wnba_markets AS
    SELECT ticker, event_ticker, result, volume, created_time AS mkt_created
    FROM read_parquet('data/trevorjs/markets-*.parquet')
    WHERE event_ticker LIKE 'KXWNBAGAME%'
      AND result IN ('yes','no')
""")
count = db.execute("SELECT COUNT(*), COUNT(DISTINCT ticker) FROM wnba_markets").fetchone()
print(f"  WNBA markets: {count[0]} rows, {count[1]} unique tickers")

print("Building first trades (WNBA only, 2024+)...")
# Step 1: Get set of WNBA tickers
db.execute("""
    CREATE OR REPLACE TABLE wnba_tickers AS
    SELECT DISTINCT ticker FROM wnba_markets
""")

# Step 2: For each WNBA ticker, find the first trade
# Do this in chunks via a semi-join
db.execute("""
    CREATE OR REPLACE TABLE first_trades AS
    SELECT t.ticker, t.yes_price, t.no_price, t.created_time
    FROM (
        SELECT
            ticker,
            yes_price,
            no_price,
            created_time,
            ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY created_time) AS rn
        FROM read_parquet('data/trevorjs/trades-*.parquet')
        WHERE ticker IN (SELECT ticker FROM wnba_tickers)
          AND created_time >= '2024-01-01'
    ) t
    WHERE t.rn = 1
""")
ft_count = db.execute("SELECT COUNT(*) FROM first_trades").fetchone()
print(f"  First trades found: {ft_count[0]}")

print("Building combined analysis table...")
db.execute("""
    CREATE OR REPLACE TABLE combined AS
    SELECT
        m.ticker,
        m.event_ticker,
        m.result,
        m.volume,
        ft.yes_price,
        ft.no_price,
        ft.created_time,
        (100.0 - ft.yes_price) / 100.0 AS no_cost,
        CASE WHEN m.result = 'no' THEN 1 ELSE 0 END AS won,
        CASE
            WHEN m.result = 'no'
            THEN (ft.yes_price::DOUBLE) / (100.0 - ft.yes_price)
            ELSE -1.0
        END AS bet_roi,
        (ft.yes_price / 5) * 5 AS price_bucket,
        EXTRACT(YEAR FROM ft.created_time) AS yr,
        EXTRACT(MONTH FROM ft.created_time) AS mo,
        EXTRACT(HOUR FROM ft.created_time) AS hr
    FROM wnba_markets m
    JOIN first_trades ft ON ft.ticker = m.ticker
    -- Only the FAVORITE side (yes_price > 50 means YES is the fav)
    WHERE ft.yes_price > 50
""")
comb_count = db.execute("SELECT COUNT(*) FROM combined").fetchone()
print(f"  Combined rows: {comb_count[0]}")

# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("WNBA GAME WINNER — FADE FAVORITES (BUY NO) ANALYSIS")
print("=" * 70)

overall = db.execute("""
SELECT
    COUNT(*) AS n_markets,
    ROUND(AVG(won) * 100, 1) AS no_win_pct,
    ROUND(AVG(bet_roi) * 100, 2) AS avg_roi_pct,
    ROUND(AVG(yes_price), 1) AS avg_yes_price,
    MIN(created_time)::DATE AS earliest,
    MAX(created_time)::DATE AS latest
FROM combined
""").fetchone()

print(f"\nOverall: {overall[0]} markets | NO win rate: {overall[1]}% | Avg ROI/bet: {overall[2]}%")
print(f"Avg yes_price (fav price): {overall[3]}c | Date range: {overall[4]} to {overall[5]}")

# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("1. PER-PRICE BUCKET ANALYSIS (5c buckets, YES favorite price)")
print("   Cost to buy NO = (100 - yes_price) cents per $1")
print("   ROI if win = yes_price / (100 - yes_price), if lose = -1")
print("=" * 70)
print(f"{'Bucket':>10} {'Count':>6} {'NO Win%':>9} {'Avg ROI%':>10} {'Breakeven%':>12} {'Edge':>8}")
print("-" * 65)

buckets = db.execute("""
SELECT
    price_bucket,
    COUNT(*) AS n,
    ROUND(AVG(won) * 100, 1) AS no_win_pct,
    ROUND(AVG(bet_roi) * 100, 2) AS avg_roi_pct,
    ROUND((100.0 - AVG(yes_price)) / 100.0 * 100, 1) AS breakeven_pct
FROM combined
WHERE price_bucket BETWEEN 55 AND 95
GROUP BY price_bucket
ORDER BY price_bucket
""").fetchall()

for row in buckets:
    bucket, n, win_pct, avg_roi, breakeven = row
    edge = round(win_pct - breakeven, 1)
    indicator = " <<" if avg_roi > 0 else ""
    print(f"  {bucket}-{bucket+4}c  {n:>6}  {win_pct:>8}%  {avg_roi:>+9.2f}%  {breakeven:>11}%  {edge:>+7.1f}%{indicator}")

# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("2. MONTHLY BREAKDOWN")
print("=" * 70)
print(f"{'Year-Mo':>9} {'Count':>6} {'NO Win%':>9} {'Avg ROI%':>10} {'Profitable?':>12}")
print("-" * 55)

monthly = db.execute("""
SELECT
    yr,
    mo,
    COUNT(*) AS n,
    ROUND(AVG(won) * 100, 1) AS no_win_pct,
    ROUND(AVG(bet_roi) * 100, 2) AS avg_roi_pct
FROM combined
GROUP BY yr, mo
ORDER BY yr, mo
""").fetchall()

month_names = {1:'Jan',2:'Feb',3:'Mar',4:'Apr',5:'May',6:'Jun',
               7:'Jul',8:'Aug',9:'Sep',10:'Oct',11:'Nov',12:'Dec'}
for row in monthly:
    yr, mo, n, win_pct, avg_roi = row
    label = f"{int(yr)}-{month_names[int(mo)]}"
    profitable = "PROFIT +" if avg_roi > 0 else "LOSS   -"
    print(f"  {label:>9}  {n:>6}  {win_pct:>8}%  {avg_roi:>+9.2f}%  {profitable:>12}")

# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("3. VOLUME FILTER TEST")
print("=" * 70)
print(f"{'Filter':>18} {'Count':>6} {'NO Win%':>9} {'Avg ROI%':>10} {'Pct of All':>11}")
print("-" * 60)

total_n = db.execute("SELECT COUNT(*) FROM combined").fetchone()[0]
vol_filters = [
    ("All volumes",   "1=1"),
    ("vol > 1M",      "volume > 1000000"),
    ("vol > 500K",    "volume > 500000"),
    ("vol 100K-500K", "volume BETWEEN 100000 AND 500000"),
    ("vol < 100K",    "volume < 100000"),
]

for label, filt in vol_filters:
    row = db.execute(f"""
    SELECT
        COUNT(*) AS n,
        ROUND(AVG(won) * 100, 1) AS no_win_pct,
        ROUND(AVG(bet_roi) * 100, 2) AS avg_roi_pct
    FROM combined
    WHERE {filt}
    """).fetchone()
    n, win_pct, avg_roi = row
    pct = round(n / total_n * 100, 1)
    indicator = " +" if avg_roi > 0 else ""
    print(f"  {label:>18}  {n:>6}  {win_pct:>8}%  {avg_roi:>+9.2f}%  {pct:>10}%{indicator}")

# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("4. TIME-OF-DAY ANALYSIS (first trade created_time, raw timezone)")
print("=" * 70)
print(f"{'Time Period':>20} {'Hours':>7} {'Count':>6} {'NO Win%':>9} {'Avg ROI%':>10}")
print("-" * 60)

tod = db.execute("""
SELECT
    CASE
        WHEN hr BETWEEN 0 AND 5   THEN '1_overnight (0-5)'
        WHEN hr BETWEEN 6 AND 11  THEN '2_morning (6-11)'
        WHEN hr BETWEEN 12 AND 16 THEN '3_afternoon (12-16)'
        WHEN hr BETWEEN 17 AND 20 THEN '4_evening (17-20)'
        ELSE                           '5_late_night (21-23)'
    END AS period,
    COUNT(*) AS n,
    ROUND(AVG(won) * 100, 1) AS no_win_pct,
    ROUND(AVG(bet_roi) * 100, 2) AS avg_roi_pct
FROM combined
GROUP BY period
ORDER BY period
""").fetchall()

for row in tod:
    period, n, win_pct, avg_roi = row
    indicator = " +" if avg_roi > 0 else ""
    print(f"  {period:>35}  {n:>6}  {win_pct:>8}%  {avg_roi:>+9.2f}%{indicator}")

# Hourly detail (only hours with >= 5 markets)
print("\n  Hourly detail (>=5 markets):")
print(f"  {'Hour':>6} {'Count':>6} {'NO Win%':>9} {'Avg ROI%':>10}")
hourly = db.execute("""
SELECT
    hr,
    COUNT(*) AS n,
    ROUND(AVG(won) * 100, 1) AS no_win_pct,
    ROUND(AVG(bet_roi) * 100, 2) AS avg_roi_pct
FROM combined
GROUP BY hr
HAVING COUNT(*) >= 5
ORDER BY hr
""").fetchall()
for row in hourly:
    hr, n, win_pct, avg_roi = row
    indicator = " +" if avg_roi > 0 else ""
    print(f"  {int(hr):>6}:00  {n:>6}  {win_pct:>8}%  {avg_roi:>+9.2f}%{indicator}")

# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("5. SEASON ANALYSIS: 2024 vs 2025")
print("=" * 70)

seasons = db.execute("""
SELECT
    yr,
    COUNT(*) AS n,
    ROUND(AVG(won) * 100, 1) AS no_win_pct,
    ROUND(AVG(bet_roi) * 100, 2) AS avg_roi_pct,
    ROUND(AVG(yes_price), 1) AS avg_yes_price
FROM combined
GROUP BY yr
ORDER BY yr
""").fetchall()

print(f"{'Season':>8} {'Count':>6} {'NO Win%':>9} {'Avg ROI%':>10} {'Avg FavPrice':>14}")
print("-" * 55)
for row in seasons:
    yr, n, win_pct, avg_roi, avg_price = row
    indicator = " +" if avg_roi > 0 else ""
    print(f"  {int(yr):>8}  {n:>6}  {win_pct:>8}%  {avg_roi:>+9.2f}%  {avg_price:>13}c{indicator}")

for season_yr in [2024, 2025]:
    print(f"\n  {season_yr} bucket detail:")
    print(f"  {'Bucket':>10} {'Count':>6} {'NO Win%':>9} {'Avg ROI%':>10}")
    sbuckets = db.execute(f"""
    SELECT
        price_bucket,
        COUNT(*) AS n,
        ROUND(AVG(won) * 100, 1) AS no_win_pct,
        ROUND(AVG(bet_roi) * 100, 2) AS avg_roi_pct
    FROM combined
    WHERE yr = {season_yr}
      AND price_bucket BETWEEN 55 AND 95
    GROUP BY price_bucket
    ORDER BY price_bucket
    """).fetchall()
    for row in sbuckets:
        bucket, n, win_pct, avg_roi = row
        indicator = " <<" if avg_roi > 0 else ""
        print(f"  {bucket}-{bucket+4}c  {n:>6}  {win_pct:>8}%  {avg_roi:>+9.2f}%{indicator}")

# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("6. BEST COMBINATIONS: Bucket x Season (vol > 500K, n >= 3)")
print("=" * 70)
print(f"{'Bucket':>10} {'Season':>8} {'Count':>6} {'NO Win%':>9} {'Avg ROI%':>10}")
print("-" * 50)

best = db.execute("""
SELECT
    price_bucket,
    yr,
    COUNT(*) AS n,
    ROUND(AVG(won) * 100, 1) AS no_win_pct,
    ROUND(AVG(bet_roi) * 100, 2) AS avg_roi_pct
FROM combined
WHERE volume > 500000
  AND price_bucket BETWEEN 55 AND 95
GROUP BY price_bucket, yr
HAVING COUNT(*) >= 3
ORDER BY avg_roi_pct DESC
LIMIT 15
""").fetchall()

for row in best:
    bucket, yr, n, win_pct, avg_roi = row
    indicator = " <<" if avg_roi > 0 else ""
    print(f"  {bucket}-{bucket+4}c  {int(yr):>8}  {n:>6}  {win_pct:>8}%  {avg_roi:>+9.2f}%{indicator}")

# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("DATA NOTES")
print("=" * 70)
data_check = db.execute("""
SELECT
    COUNT(*) AS total_fav_markets,
    SUM(CASE WHEN result='no' THEN 1 ELSE 0 END) AS upsets,
    ROUND(MIN(yes_price), 0) AS min_yes_price,
    ROUND(MAX(yes_price), 0) AS max_yes_price,
    ROUND(AVG(yes_price), 1) AS avg_yes_price
FROM combined
""").fetchone()
total, upsets, min_p, max_p, avg_p = data_check
print(f"  Favorite-side markets (yes_price > 50): {total}")
print(f"  Upsets (result=no, fav loses): {upsets} ({round(upsets/total*100,1)}%)")
print(f"  Yes-price range: {int(min_p)}c - {int(max_p)}c (avg {avg_p}c)")
print(f"  Prices are in cents (1-99 scale)")
print(f"  ROI formula: win → yes_price / (100 - yes_price), lose → -1")
print(f"  Only markets where first trade yes_price > 50 (clear favorite)")

db.close()
import os
if os.path.exists("wnba_analysis.duckdb"):
    os.remove("wnba_analysis.duckdb")
