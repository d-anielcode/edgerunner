"""
Research: How has the Kalshi market evolved over time?
Which data is still relevant for calibrating our models?
"""
import duckdb
con = duckdb.connect()
mp = "data/trevorjs/markets-*.parquet"
tp = "data/trevorjs/trades-*.parquet"

print("=" * 85)
print("RESEARCH: KALSHI MARKET EVOLUTION 2021-2026")
print("Question: Is old data still relevant? When did each market mature?")
print("=" * 85)

# 1. Volume growth over time
print("\n1. KALSHI VOLUME GROWTH BY QUARTER")
rows = con.sql(f"""
    SELECT EXTRACT(YEAR FROM created_time) as yr,
           CASE WHEN EXTRACT(MONTH FROM created_time) <= 3 THEN 'Q1'
                WHEN EXTRACT(MONTH FROM created_time) <= 6 THEN 'Q2'
                WHEN EXTRACT(MONTH FROM created_time) <= 9 THEN 'Q3'
                ELSE 'Q4' END as qtr,
           COUNT(*) as trades,
           COUNT(DISTINCT ticker) as markets,
           SUM(count) as contracts
    FROM '{tp}'
    GROUP BY yr, qtr
    ORDER BY yr, qtr
""").fetchall()
print(f"   {'Period':>8s} | {'Trades':>12s} | {'Markets':>8s} | {'Contracts':>14s}")
print("   " + "-" * 55)
for r in rows:
    period = f"{int(r[0])}-{r[1]}"
    print(f"   {period:>8s} | {r[2]:>12,} | {r[3]:>8,} | {r[4]:>14,}")

# 2. Per-sport: When did each sport reach maturity?
print("\n2. WHEN DID EACH SPORT REACH MEANINGFUL VOLUME?")
sports = [
    ("NBA", "KXNBAGAME%"), ("NHL", "KXNHLGAME%"), ("NFL", "KXNFLGAME%"),
    ("EPL", "KXEPLGAME%"), ("UCL", "KXUCLGAME%"), ("WNBA", "KXWNBAGAME%"),
    ("UFC", "KXUFCFIGHT%"), ("NCAA-BB", "KXNCAAMBGAME%"),
    ("WTA", "KXWTAMATCH%"), ("Weather", "KXHIGH%"),
    ("NFL TD", "KXNFLANYTD%"), ("CPI", "CPI%"),
]

print(f"   {'Sport':>10s} | {'First Trade':>12s} | {'Last Trade':>12s} | {'Total Mkts':>10s} | {'Avg Vol':>10s}")
print("   " + "-" * 70)
for name, pat in sports:
    r = con.sql(f"""
        SELECT MIN(open_time) as first, MAX(close_time) as last,
               COUNT(*) as mkts, ROUND(AVG(volume)) as avg_vol
        FROM '{mp}' WHERE event_ticker LIKE '{pat}' AND status='finalized'
    """).fetchone()
    first = str(r[0])[:10] if r[0] else "N/A"
    last = str(r[1])[:10] if r[1] else "N/A"
    print(f"   {name:>10s} | {first:>12s} | {last:>12s} | {r[2]:>10,} | {int(r[3] or 0):>10,}")

# 3. KEY QUESTION: Does the favorite-longshot bias change over time?
print("\n3. FAVORITE-LONGSHOT BIAS EVOLUTION (NBA)")
print("   Does the edge fade as markets mature?")
rows = con.sql(f"""
    WITH gm AS (
        SELECT ticker, result FROM '{mp}'
        WHERE event_ticker LIKE 'KXNBAGAME%' AND status='finalized' AND result IN ('yes','no')
    ),
    ft AS (
        SELECT t.ticker, t.yes_price, t.created_time,
               EXTRACT(YEAR FROM t.created_time) as yr,
               EXTRACT(MONTH FROM t.created_time) as mo,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
        FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM gm)
    )
    SELECT yr, mo,
           COUNT(*) as games,
           ROUND(SUM(CASE WHEN gm.result='no' THEN 1.0 ELSE 0.0 END)/COUNT(*)*100, 1) as no_win,
           ROUND(SUM(
               CASE WHEN gm.result='no' THEN 1.0 - (100-ft.yes_price)/100.0
                    ELSE -(100-ft.yes_price)/100.0 END
           ) / NULLIF(SUM((100-ft.yes_price)/100.0), 0) * 100, 1) as no_roi
    FROM ft JOIN gm ON ft.ticker = gm.ticker
    WHERE ft.rn = 1 AND ft.yes_price BETWEEN 61 AND 90
    GROUP BY yr, mo ORDER BY yr, mo
""").fetchall()
print(f"   {'Period':>8s} | {'Games':>6s} | {'NO Win%':>8s} | {'NO ROI':>8s} | Trend")
print("   " + "-" * 55)
for r in rows:
    period = f"{int(r[0])}-{int(r[1]):02d}"
    trend = "STRONG" if r[4] and r[4] > 20 else ("ok" if r[4] and r[4] > 0 else "WEAK/NEG")
    print(f"   {period:>8s} | {r[2]:>6d} | {r[3]:>7.1f}% | {r[4]:>+7.1f}% | {trend}")

# Same for NHL
print("\n   NHL bias evolution:")
rows = con.sql(f"""
    WITH gm AS (
        SELECT ticker, result FROM '{mp}'
        WHERE event_ticker LIKE 'KXNHLGAME%' AND status='finalized' AND result IN ('yes','no')
    ),
    ft AS (
        SELECT t.ticker, t.yes_price, t.created_time,
               EXTRACT(YEAR FROM t.created_time) as yr,
               EXTRACT(MONTH FROM t.created_time) as mo,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
        FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM gm)
    )
    SELECT yr, mo,
           COUNT(*) as games,
           ROUND(SUM(CASE WHEN gm.result='no' THEN 1.0 ELSE 0.0 END)/COUNT(*)*100, 1) as no_win,
           ROUND(SUM(
               CASE WHEN gm.result='no' THEN 1.0 - (100-ft.yes_price)/100.0
                    ELSE -(100-ft.yes_price)/100.0 END
           ) / NULLIF(SUM((100-ft.yes_price)/100.0), 0) * 100, 1) as no_roi
    FROM ft JOIN gm ON ft.ticker = gm.ticker
    WHERE ft.rn = 1 AND ft.yes_price BETWEEN 61 AND 90
    GROUP BY yr, mo ORDER BY yr, mo
""").fetchall()
for r in rows:
    period = f"{int(r[0])}-{int(r[1]):02d}"
    trend = "STRONG" if r[4] and r[4] > 20 else ("ok" if r[4] and r[4] > 0 else "WEAK/NEG")
    print(f"   {period:>8s} | {r[2]:>6d} | {r[3]:>7.1f}% | {r[4]:>+7.1f}% | {trend}")

# 4. Weather edge over time
print("\n4. WEATHER BIAS EVOLUTION")
rows = con.sql(f"""
    WITH gm AS (
        SELECT ticker, result FROM '{mp}'
        WHERE event_ticker LIKE 'KXHIGH%' AND status='finalized' AND result IN ('yes','no')
    ),
    ft AS (
        SELECT t.ticker, t.yes_price, t.created_time,
               EXTRACT(YEAR FROM t.created_time) as yr,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
        FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM gm)
    )
    SELECT yr,
           COUNT(*) as bets,
           ROUND(SUM(CASE WHEN gm.result='no' THEN 1.0 ELSE 0.0 END)/COUNT(*)*100, 1) as no_win,
           ROUND(SUM(
               CASE WHEN gm.result='no' THEN 1.0 - (100-ft.yes_price)/100.0
                    ELSE -(100-ft.yes_price)/100.0 END
           ) / NULLIF(SUM((100-ft.yes_price)/100.0), 0) * 100, 1) as no_roi
    FROM ft JOIN gm ON ft.ticker = gm.ticker
    WHERE ft.rn = 1 AND ft.yes_price BETWEEN 55 AND 95
    GROUP BY yr ORDER BY yr
""").fetchall()
for r in rows:
    trend = "STRONG" if r[3] and r[3] > 20 else ("ok" if r[3] and r[3] > 0 else "WEAK")
    print(f"   {int(r[0])}: {r[1]:>5d} bets | NO wins {r[2]:>5.1f}% | ROI {r[3]:>+7.1f}% | {trend}")

# 5. Maker-Taker ratio evolution
print("\n5. MAKER vs TAKER EVOLUTION (who is taking more?)")
rows = con.sql(f"""
    SELECT EXTRACT(YEAR FROM created_time) as yr,
           taker_side,
           COUNT(*) as trades,
           SUM(count) as contracts
    FROM '{tp}'
    WHERE created_time >= '2024-01-01'
    GROUP BY yr, taker_side
    ORDER BY yr, taker_side
""").fetchall()
for r in rows:
    print(f"   {int(r[0])}: taker={r[1]:>3s} | {r[2]:>12,} trades | {r[3]:>14,} contracts")

# 6. Average trade size evolution
print("\n6. AVERAGE TRADE SIZE EVOLUTION (are bettors getting bigger?)")
rows = con.sql(f"""
    SELECT EXTRACT(YEAR FROM created_time) as yr,
           EXTRACT(QUARTER FROM created_time) as qtr,
           ROUND(AVG(count), 1) as avg_size,
           ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY count), 1) as median_size,
           ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY count), 1) as p95_size
    FROM '{tp}'
    GROUP BY yr, qtr
    ORDER BY yr, qtr
""").fetchall()
print(f"   {'Period':>8s} | {'Avg Size':>9s} | {'Median':>7s} | {'P95':>7s}")
print("   " + "-" * 40)
for r in rows:
    print(f"   {int(r[0])}-Q{int(r[1])} | {r[2]:>9.1f} | {r[3]:>7.1f} | {r[4]:>7.1f}")

# 7. Price accuracy: How close is the opening price to the actual result?
print("\n7. PRICE CALIBRATION: Are markets getting more accurate?")
print("   (Perfect calibration: 70c YES should win exactly 70% of the time)")
for sport, pat in [("NBA", "KXNBAGAME%"), ("NHL", "KXNHLGAME%")]:
    rows = con.sql(f"""
        WITH gm AS (
            SELECT ticker, result FROM '{mp}'
            WHERE event_ticker LIKE '{pat}' AND status='finalized' AND result IN ('yes','no')
        ),
        ft AS (
            SELECT t.ticker, t.yes_price, t.created_time,
                   EXTRACT(YEAR FROM t.created_time) as yr,
                   EXTRACT(MONTH FROM t.created_time) as mo,
                   ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
            FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM gm)
        )
        SELECT
            CASE WHEN mo <= 6 THEN yr || '-H1' ELSE yr || '-H2' END as half,
            COUNT(*) as games,
            ROUND(AVG(ft.yes_price), 1) as avg_implied_yes,
            ROUND(SUM(CASE WHEN gm.result='yes' THEN 1.0 ELSE 0.0 END)/COUNT(*)*100, 1) as actual_yes_pct,
            ROUND(AVG(ft.yes_price) - SUM(CASE WHEN gm.result='yes' THEN 1.0 ELSE 0.0 END)/COUNT(*)*100, 1) as overpricing
        FROM ft JOIN gm ON ft.ticker = gm.ticker
        WHERE ft.rn = 1 AND ft.yes_price BETWEEN 61 AND 90
        GROUP BY half ORDER BY half
    """).fetchall()
    print(f"\n   {sport}:")
    print(f"   {'Period':>8s} | {'Games':>6s} | {'Implied YES':>11s} | {'Actual YES':>10s} | {'Overpricing':>11s}")
    print("   " + "-" * 60)
    for r in rows:
        print(f"   {r[0]:>8s} | {r[1]:>6d} | {r[2]:>10.1f}% | {r[3]:>9.1f}% | {r[4]:>+10.1f}%")

print("\n" + "=" * 85)
print("SUMMARY: WHICH DATA IS RELEVANT?")
print("=" * 85)
