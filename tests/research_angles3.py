"""
Research Round 3: Trade size signal, open interest, divisional matchups, conference strength.
"""
import duckdb
con = duckdb.connect()
mp = "data/trevorjs/markets-*.parquet"
tp = "data/trevorjs/trades-*.parquet"

# ================================================================
# TEST 1: TRADE SIZE AS SIGNAL
# Do large early trades (whale bets) predict outcomes differently?
# ================================================================
print("=" * 85)
print("TEST 1: TRADE SIZE — Do whales predict outcomes better?")
print("=" * 85)

for sport, pat in [("NBA", "KXNBAGAME%"), ("NHL", "KXNHLGAME%")]:
    rows = con.sql(f"""
        WITH gm AS (
            SELECT ticker, result FROM '{mp}'
            WHERE event_ticker LIKE '{pat}' AND status='finalized' AND result IN ('yes','no')
        ),
        early_trades AS (
            SELECT t.ticker, t.yes_price, t.taker_side, t.count as sz,
                   ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
            FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM gm)
                AND t.created_time >= '2025-01-01'
        )
        SELECT
            CASE WHEN sz <= 10 THEN '1. Small (1-10)'
                 WHEN sz <= 50 THEN '2. Medium (11-50)'
                 WHEN sz <= 200 THEN '3. Large (51-200)'
                 ELSE '4. Whale (200+)' END as size_bucket,
            taker_side,
            COUNT(*) as trades,
            ROUND(SUM(CASE WHEN gm.result='no' THEN 1.0 ELSE 0.0 END)/COUNT(*)*100, 1) as no_win_pct
        FROM early_trades JOIN gm ON early_trades.ticker = gm.ticker
        WHERE early_trades.rn <= 5 AND early_trades.yes_price BETWEEN 61 AND 90
        GROUP BY size_bucket, taker_side
        HAVING COUNT(*) >= 10
        ORDER BY size_bucket, taker_side
    """).fetchall()

    print(f"\n  {sport} — First 5 trades per market:")
    print(f"    {'Size':>20s} | {'Taker':>5s} | {'Trades':>6s} | {'NO wins':>8s} | Signal")
    print("    " + "-" * 60)
    for r in rows:
        # If taker buys YES and NO wins a lot, the taker is the sucker
        # If taker buys NO and NO wins a lot, the taker is smart money
        signal = ""
        if r[1] == "no" and r[3] > 50: signal = " <-- SMART NO MONEY"
        if r[1] == "yes" and r[3] > 45: signal = " <-- YES TAKERS LOSING"
        print(f"    {r[0]:>20s} | {r[1]:>5s} | {r[2]:>6d} | {r[3]:>7.1f}% | {signal}")

# ================================================================
# TEST 2: OPEN INTEREST — High OI = more informed positioning?
# ================================================================
print("\n\n" + "=" * 85)
print("TEST 2: OPEN INTEREST — Does high OI predict anything?")
print("=" * 85)

for sport, pat in [("NBA", "KXNBAGAME%"), ("NHL", "KXNHLGAME%")]:
    rows = con.sql(f"""
        WITH gm AS (
            SELECT ticker, result, open_interest FROM '{mp}'
            WHERE event_ticker LIKE '{pat}' AND status='finalized' AND result IN ('yes','no')
        ),
        ft AS (
            SELECT t.ticker, t.yes_price,
                   ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
            FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM gm)
                AND t.created_time >= '2025-01-01'
        )
        SELECT
            CASE WHEN gm.open_interest < 500 THEN '1. Low OI (<500)'
                 WHEN gm.open_interest < 5000 THEN '2. Med OI (500-5K)'
                 WHEN gm.open_interest < 20000 THEN '3. High OI (5K-20K)'
                 ELSE '4. Very High OI (20K+)' END as oi_bucket,
            COUNT(*) as games,
            ROUND(SUM(CASE WHEN gm.result='no' THEN 1.0 ELSE 0.0 END)/COUNT(*)*100, 1) as no_win,
            ROUND(SUM(
                CASE WHEN gm.result='no' THEN 1.0 - (100-ft.yes_price)/100.0
                     ELSE -(100-ft.yes_price)/100.0 END
            ) / NULLIF(SUM((100-ft.yes_price)/100.0), 0) * 100, 1) as roi
        FROM ft JOIN gm ON ft.ticker = gm.ticker
        WHERE ft.rn = 1 AND ft.yes_price BETWEEN 61 AND 95
        GROUP BY oi_bucket
        HAVING COUNT(*) >= 5
        ORDER BY oi_bucket
    """).fetchall()

    print(f"\n  {sport}:")
    for r in rows:
        print(f"    {r[0]:25s}: {r[1]:>5d} games | NO wins {r[2]:>5.1f}% | ROI {r[3]:>+7.1f}%")

# ================================================================
# TEST 3: SPREAD AS CONFIRMATION SIGNAL
# Compare moneyline price vs spread market for same game
# If spread implies closer game than moneyline, divergence = signal
# ================================================================
print("\n\n" + "=" * 85)
print("TEST 3: MONEYLINE vs SPREAD DIVERGENCE")
print("Do spread markets give us a second opinion on the game?")
print("=" * 85)

# Check if we can match KXNBAGAME and KXNBASPREAD for same game
rows = con.sql(f"""
    WITH ml AS (
        SELECT ticker, event_ticker, result,
               SPLIT_PART(event_ticker, '-', 2) as game_key
        FROM '{mp}'
        WHERE event_ticker LIKE 'KXNBAGAME%' AND status='finalized' AND result IN ('yes','no')
    ),
    sp AS (
        SELECT ticker, event_ticker,
               SPLIT_PART(event_ticker, '-', 2) as game_key
        FROM '{mp}'
        WHERE event_ticker LIKE 'KXNBASPREAD%' AND status='finalized'
    ),
    matched AS (
        SELECT ml.game_key, COUNT(DISTINCT ml.ticker) as ml_markets, COUNT(DISTINCT sp.ticker) as sp_markets
        FROM ml JOIN sp ON ml.game_key = sp.game_key
        GROUP BY ml.game_key
    )
    SELECT COUNT(*) as matched_games, AVG(ml_markets) as avg_ml, AVG(sp_markets) as avg_sp
    FROM matched
""").fetchone()
print(f"\n  Matched games (moneyline + spread): {rows[0]}")
print(f"  Avg moneyline markets per game: {rows[1]:.1f}")
print(f"  Avg spread markets per game: {rows[2]:.1f}")

if rows[0] > 0:
    # For matched games, compare: did the spread market "agree" with moneyline?
    print("  (Would need to compare ML price vs spread implied probability)")
    print("  This requires cross-market logic — logging for future implementation.")

# ================================================================
# TEST 4: SAME-DIVISION MATCHUPS
# Do divisional rivals produce more upsets?
# ================================================================
print("\n\n" + "=" * 85)
print("TEST 4: PRICE RANGE WHERE EDGE IS STRONGEST")
print("Instead of divisions (no data), check the optimal price range per sport")
print("=" * 85)

# For each sport, find the price range with the best risk-adjusted ROI
for sport, pat, lo, hi in [
    ("NBA", "KXNBAGAME%", 55, 95),
    ("NHL", "KXNHLGAME%", 55, 95),
    ("EPL", "KXEPLGAME%", 61, 90),
    ("NFLTD", "KXNFLANYTD%", 55, 95),
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
        SELECT
            CASE WHEN ft.yes_price BETWEEN 55 AND 60 THEN '55-60c'
                 WHEN ft.yes_price BETWEEN 61 AND 65 THEN '61-65c'
                 WHEN ft.yes_price BETWEEN 66 AND 70 THEN '66-70c'
                 WHEN ft.yes_price BETWEEN 71 AND 75 THEN '71-75c'
                 WHEN ft.yes_price BETWEEN 76 AND 80 THEN '76-80c'
                 WHEN ft.yes_price BETWEEN 81 AND 85 THEN '81-85c'
                 WHEN ft.yes_price BETWEEN 86 AND 90 THEN '86-90c'
                 WHEN ft.yes_price BETWEEN 91 AND 95 THEN '91-95c' END as bucket,
            COUNT(*) as bets,
            ROUND(SUM(CASE WHEN gm.result='no' THEN 1.0 ELSE 0.0 END)/COUNT(*)*100, 1) as no_win,
            ROUND(SUM(
                CASE WHEN gm.result='no' THEN 1.0 - (100-ft.yes_price)/100.0
                     ELSE -(100-ft.yes_price)/100.0 END
            ) / NULLIF(SUM((100-ft.yes_price)/100.0), 0) * 100, 1) as roi,
            -- EV per dollar risked
            ROUND(SUM(
                CASE WHEN gm.result='no' THEN 1.0 - (100-ft.yes_price)/100.0
                     ELSE -(100-ft.yes_price)/100.0 END
            ) / NULLIF(COUNT(*), 0), 3) as ev_per_bet
        FROM ft JOIN gm ON ft.ticker = gm.ticker
        WHERE ft.rn = 1 AND ft.yes_price BETWEEN {lo} AND {hi}
        GROUP BY bucket
        HAVING COUNT(*) >= 5
        ORDER BY bucket
    """).fetchall()

    print(f"\n  {sport} — optimal price ranges:")
    for r in rows:
        if r[0]:
            bar = "#" * max(0, int((r[3] or 0) / 10))
            print(f"    {r[0]:>7s}: {r[1]:>4d} bets | NO wins {r[2]:>5.1f}% | ROI {r[3]:>+7.1f}% | EV/bet ${r[4]:>+.3f} | {bar[:20]}")

# ================================================================
# TEST 5: NHL EARLY SEASON BOOST (Oct-Nov)
# Gemini says early season = information vacuum = more mispricing
# Our data showed Oct-Dec at +36.5% vs Jan at +16.4%
# ================================================================
print("\n\n" + "=" * 85)
print("TEST 5: NHL EARLY vs LATE REGULAR SEASON")
print("=" * 85)

rows = con.sql(f"""
    WITH gm AS (
        SELECT ticker, result FROM '{mp}'
        WHERE event_ticker LIKE 'KXNHLGAME%' AND status='finalized' AND result IN ('yes','no')
    ),
    ft AS (
        SELECT t.ticker, t.yes_price,
               EXTRACT(MONTH FROM t.created_time) as mo,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
        FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM gm) AND t.created_time >= '2025-01-01'
    )
    SELECT mo, COUNT(*) as games,
           ROUND(SUM(CASE WHEN gm.result='no' THEN 1.0 ELSE 0.0 END)/COUNT(*)*100, 1) as no_win,
           ROUND(SUM(
               CASE WHEN gm.result='no' THEN 1.0 - (100-ft.yes_price)/100.0
                    ELSE -(100-ft.yes_price)/100.0 END
           ) / NULLIF(SUM((100-ft.yes_price)/100.0), 0) * 100, 1) as roi
    FROM ft JOIN gm ON ft.ticker = gm.ticker
    WHERE ft.rn = 1 AND ft.yes_price BETWEEN 61 AND 95
    GROUP BY mo ORDER BY mo
""").fetchall()

mo_names = {1:"Jan",4:"Apr",5:"May",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}
for r in rows:
    m = int(r[0])
    name = mo_names.get(m, f"M{m}")
    tag = ""
    if m in (4,5): tag = " <-- PLAYOFFS"
    if m in (9,10): tag = " <-- EARLY SEASON"
    print(f"  {name:3s}: {r[1]:>4d} games | NO wins {r[2]:>5.1f}% | ROI {r[3]:>+7.1f}%{tag}")

print("\n" + "=" * 85)
