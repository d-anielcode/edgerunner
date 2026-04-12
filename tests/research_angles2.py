"""
Research Round 2: NFL by week, late season effects, per-cent analysis for other sports.
"""
import duckdb
con = duckdb.connect()
mp = "data/trevorjs/markets-*.parquet"
tp = "data/trevorjs/trades-*.parquet"

# ================================================================
# TEST 1: NFL TD BY WEEK NUMBER
# Gemini says Week 1-3 lines overpredict by 1 point
# ================================================================
print("=" * 85)
print("TEST 1: NFL ANYTIME TD BY WEEK/MONTH")
print("Gemini: Early season (Sep) should have more mispricing than late season")
print("=" * 85)

rows = con.sql(f"""
    WITH gm AS (
        SELECT ticker, result FROM '{mp}'
        WHERE event_ticker LIKE 'KXNFLANYTD%' AND status='finalized' AND result IN ('yes','no')
    ),
    ft AS (
        SELECT t.ticker, t.yes_price,
               EXTRACT(MONTH FROM t.created_time) as mo,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
        FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM gm) AND t.created_time >= '2025-01-01'
    )
    SELECT mo, COUNT(*) as bets,
           ROUND(SUM(CASE WHEN gm.result='no' THEN 1.0 ELSE 0.0 END)/COUNT(*)*100, 1) as no_win,
           ROUND(SUM(
               CASE WHEN gm.result='no' THEN 1.0 - (100-ft.yes_price)/100.0
                    ELSE -(100-ft.yes_price)/100.0 END
           ) / NULLIF(SUM((100-ft.yes_price)/100.0), 0) * 100, 1) as roi
    FROM ft JOIN gm ON ft.ticker = gm.ticker
    WHERE ft.rn = 1 AND ft.yes_price BETWEEN 55 AND 95
    GROUP BY mo ORDER BY mo
""").fetchall()

mo_names = {1:"Jan",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}
print("\n  NFL TD by month:")
for r in rows:
    m = int(r[0])
    name = mo_names.get(m, f"M{m}")
    tag = " <-- EARLY SEASON" if m == 9 else (" <-- PLAYOFFS" if m == 1 else "")
    print(f"    {name:3s}: {r[1]:>5d} bets | NO wins {r[2]:>5.1f}% | ROI {r[3]:>+7.1f}%{tag}")

# NFL TD by price bucket per month
print("\n  NFL TD: Sep (early) vs Dec (late) by price:")
for month, label in [(9, "Sep (early season)"), (12, "Dec (late season)")]:
    rows2 = con.sql(f"""
        WITH gm AS (
            SELECT ticker, result FROM '{mp}'
            WHERE event_ticker LIKE 'KXNFLANYTD%' AND status='finalized' AND result IN ('yes','no')
        ),
        ft AS (
            SELECT t.ticker, t.yes_price,
                   EXTRACT(MONTH FROM t.created_time) as mo,
                   ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
            FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM gm) AND t.created_time >= '2025-01-01'
        )
        SELECT
            CASE WHEN ft.yes_price BETWEEN 55 AND 65 THEN '55-65c'
                 WHEN ft.yes_price BETWEEN 66 AND 75 THEN '66-75c'
                 WHEN ft.yes_price BETWEEN 76 AND 90 THEN '76-90c' END as bucket,
            COUNT(*) as bets,
            ROUND(SUM(CASE WHEN gm.result='no' THEN 1.0 ELSE 0.0 END)/COUNT(*)*100, 1) as no_win,
            ROUND(SUM(
                CASE WHEN gm.result='no' THEN 1.0 - (100-ft.yes_price)/100.0
                     ELSE -(100-ft.yes_price)/100.0 END
            ) / NULLIF(SUM((100-ft.yes_price)/100.0), 0) * 100, 1) as roi
        FROM ft JOIN gm ON ft.ticker = gm.ticker
        WHERE ft.rn = 1 AND ft.yes_price BETWEEN 55 AND 95 AND ft.mo = {month}
        GROUP BY bucket ORDER BY bucket
    """).fetchall()
    print(f"\n    {label}:")
    for r in rows2:
        if r[0]:
            print(f"      {r[0]:>6s}: {r[1]:>4d} bets | NO wins {r[2]:>5.1f}% | ROI {r[3]:>+7.1f}%")

# ================================================================
# TEST 2: LATE SEASON EFFECTS (final 2 weeks)
# Tanking, resting, clinched teams
# ================================================================
print("\n\n" + "=" * 85)
print("TEST 2: LATE SEASON vs MID SEASON")
print("Final 2 weeks of regular season: tanking + resting starters")
print("=" * 85)

# NBA: Compare Oct-Feb (mid season) vs March (late) vs Apr 1-12 (final stretch)
rows = con.sql(f"""
    WITH gm AS (
        SELECT ticker, result FROM '{mp}'
        WHERE event_ticker LIKE 'KXNBAGAME%' AND status='finalized' AND result IN ('yes','no')
    ),
    ft AS (
        SELECT t.ticker, t.yes_price,
               EXTRACT(MONTH FROM t.created_time) as mo,
               EXTRACT(DAY FROM t.created_time) as dy,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
        FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM gm) AND t.created_time >= '2025-01-01'
    )
    SELECT
        CASE
            WHEN mo BETWEEN 10 AND 12 THEN '1. Early season (Oct-Dec)'
            WHEN mo = 1 THEN '2. Mid season (Jan)'
            WHEN mo = 4 AND dy <= 12 THEN '3. Late season (Apr 1-12)'
        END as period,
        COUNT(*) as games,
        ROUND(AVG(ft.yes_price), 1) as avg_price,
        ROUND(SUM(CASE WHEN gm.result='no' THEN 1.0 ELSE 0.0 END)/COUNT(*)*100, 1) as no_win,
        ROUND(SUM(
            CASE WHEN gm.result='no' THEN 1.0 - (100-ft.yes_price)/100.0
                 ELSE -(100-ft.yes_price)/100.0 END
        ) / NULLIF(SUM((100-ft.yes_price)/100.0), 0) * 100, 1) as roi
    FROM ft JOIN gm ON ft.ticker = gm.ticker
    WHERE ft.rn = 1 AND ft.yes_price BETWEEN 55 AND 95
          AND ((mo BETWEEN 10 AND 12) OR mo = 1 OR (mo = 4 AND dy <= 12))
    GROUP BY period ORDER BY period
""").fetchall()

print("\n  NBA season phases:")
for r in rows:
    if r[0]:
        print(f"    {r[0]:35s}: {r[1]:>4d} games | avg price {r[2]}c | NO wins {r[3]:>5.1f}% | ROI {r[4]:>+7.1f}%")

# Same for NHL
rows2 = con.sql(f"""
    WITH gm AS (
        SELECT ticker, result FROM '{mp}'
        WHERE event_ticker LIKE 'KXNHLGAME%' AND status='finalized' AND result IN ('yes','no')
    ),
    ft AS (
        SELECT t.ticker, t.yes_price,
               EXTRACT(MONTH FROM t.created_time) as mo,
               EXTRACT(DAY FROM t.created_time) as dy,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
        FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM gm) AND t.created_time >= '2025-01-01'
    )
    SELECT
        CASE
            WHEN mo BETWEEN 10 AND 12 THEN '1. Early season (Oct-Dec)'
            WHEN mo = 1 THEN '2. Mid season (Jan)'
            WHEN mo = 4 AND dy <= 16 THEN '3. Late season (Apr 1-16)'
        END as period,
        COUNT(*) as games,
        ROUND(SUM(CASE WHEN gm.result='no' THEN 1.0 ELSE 0.0 END)/COUNT(*)*100, 1) as no_win,
        ROUND(SUM(
            CASE WHEN gm.result='no' THEN 1.0 - (100-ft.yes_price)/100.0
                 ELSE -(100-ft.yes_price)/100.0 END
        ) / NULLIF(SUM((100-ft.yes_price)/100.0), 0) * 100, 1) as roi
    FROM ft JOIN gm ON ft.ticker = gm.ticker
    WHERE ft.rn = 1 AND ft.yes_price BETWEEN 55 AND 95
          AND ((mo BETWEEN 10 AND 12) OR mo = 1 OR (mo = 4 AND dy <= 16))
    GROUP BY period ORDER BY period
""").fetchall()

print("\n  NHL season phases:")
for r in rows2:
    if r[0]:
        print(f"    {r[0]:35s}: {r[1]:>4d} games | NO wins {r[2]:>5.1f}% | ROI {r[3]:>+7.1f}%")

# ================================================================
# TEST 3: PER-CENT ANALYSIS FOR OTHER SPORTS
# We did NBA/NHL already. Now check EPL, NFLTD, Weather, WNBA
# ================================================================
print("\n\n" + "=" * 85)
print("TEST 3: PER-CENT PRICE ANALYSIS FOR OTHER SPORTS")
print("Finding optimal price points beyond NBA/NHL")
print("=" * 85)

for sport, pat, lo, hi in [
    ("EPL", "KXEPLGAME%", 61, 90),
    ("NFL TD", "KXNFLANYTD%", 55, 95),
    ("Weather", "KXHIGH%", 55, 95),
    ("WNBA", "KXWNBAGAME%", 55, 95),
    ("UFC", "KXUFCFIGHT%", 61, 90),
    ("NCAAMB", "KXNCAAMBGAME%", 61, 90),
]:
    rows = con.sql(f"""
        WITH gm AS (
            SELECT ticker, result FROM '{mp}'
            WHERE event_ticker LIKE '{pat}' AND status='finalized' AND result IN ('yes','no')
        ),
        ft AS (
            SELECT t.ticker, t.yes_price,
                   ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
            FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM gm) AND t.created_time >= '2025-01-01'
        )
        SELECT ft.yes_price,
               COUNT(*) as bets,
               ROUND(SUM(CASE WHEN gm.result='no' THEN 1.0 ELSE 0.0 END)/COUNT(*)*100, 1) as no_win,
               ROUND(SUM(
                   CASE WHEN gm.result='no' THEN 1.0 - (100-ft.yes_price)/100.0
                        ELSE -(100-ft.yes_price)/100.0 END
               ) / NULLIF(SUM((100-ft.yes_price)/100.0), 0) * 100, 1) as roi
        FROM ft JOIN gm ON ft.ticker = gm.ticker
        WHERE ft.rn = 1 AND ft.yes_price BETWEEN {lo} AND {hi}
        GROUP BY ft.yes_price
        HAVING COUNT(*) >= 3
        ORDER BY ft.yes_price
    """).fetchall()

    if rows:
        print(f"\n  {sport}:")
        for r in rows:
            bar = "#" * max(0, int((r[3] or 0) / 10)) if r[3] and r[3] > 0 else "-" * max(0, int(-(r[3] or 0) / 10))
            print(f"    {r[0]:>3d}c: {r[1]:>4d} bets | NO wins {r[2]:>5.1f}% | ROI {r[3]:>+7.1f}% | {bar[:30]}")

print("\n" + "=" * 85)
