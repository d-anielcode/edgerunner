"""Research: 7 untapped data angles we're not using."""
import duckdb

con = duckdb.connect()
tp = "data/trevorjs/trades-*.parquet"
mp = "data/trevorjs/markets-*.parquet"

print("=" * 80)
print("RESEARCH: UNTAPPED ANGLES IN 154M TRADES")
print("We currently use 0.02% of our data (first trade per market only)")
print("=" * 80)

# 1. TAKER SIDE
print("\n1. TAKER SIDE: Who is the sucker?")
print("   (taker = person crossing the spread, maker = person with resting order)")
for sport, pat in [("NBA", "KXNBAGAME%"), ("NHL", "KXNHLGAME%")]:
    rows = con.sql(f"""
        WITH gm AS (
            SELECT ticker, result FROM '{mp}'
            WHERE event_ticker LIKE '{pat}' AND status='finalized' AND result IN ('yes','no')
        ),
        ft AS (
            SELECT t.ticker, t.yes_price, t.taker_side,
                   ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
            FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM gm)
        )
        SELECT taker_side,
               COUNT(*) as trades,
               ROUND(SUM(CASE WHEN gm.result=ft.taker_side THEN 1.0 ELSE 0.0 END)/COUNT(*)*100, 1) as taker_wins,
               ROUND(AVG(ft.yes_price), 1) as avg_price
        FROM ft JOIN gm ON ft.ticker = gm.ticker
        WHERE ft.rn = 1 AND ft.yes_price BETWEEN 61 AND 90
        GROUP BY taker_side
    """).fetchall()
    print(f"   {sport}:")
    for r in rows:
        print(f"     Taker buys {r[0]:>3s}: {r[1]:>5d} | taker wins {r[2]:>5.1f}% | avg price {r[3]}c")

# 2. TRADE SIZE AS SIGNAL
print("\n2. TRADE SIZE: Do big bets predict the winner?")
rows = con.sql(f"""
    WITH gm AS (
        SELECT ticker, result FROM '{mp}'
        WHERE event_ticker LIKE 'KXNBAGAME%' AND status='finalized' AND result IN ('yes','no')
    ),
    trades AS (
        SELECT t.ticker, t.yes_price, t.taker_side, t.count as sz,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
        FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM gm)
    )
    SELECT
        CASE WHEN sz <= 10 THEN 'small (1-10)'
             WHEN sz <= 50 THEN 'medium (11-50)'
             WHEN sz <= 200 THEN 'large (51-200)'
             ELSE 'whale (200+)' END as bucket,
        taker_side,
        COUNT(*) as cnt,
        ROUND(SUM(CASE WHEN gm.result=trades.taker_side THEN 1.0 ELSE 0.0 END)/COUNT(*)*100, 1) as taker_win
    FROM trades JOIN gm ON trades.ticker = gm.ticker
    WHERE trades.rn <= 10 AND trades.yes_price BETWEEN 61 AND 90
    GROUP BY bucket, taker_side
    HAVING COUNT(*) >= 20
    ORDER BY bucket, taker_side
""").fetchall()
for r in rows:
    smart = " <-- SMART MONEY?" if r[3] > 55 else ""
    print(f"   {r[0]:>18s} | taker={r[1]:>3s} | {r[2]:>5d} | wins {r[3]:>5.1f}%{smart}")

# 3. LINE MOVEMENT
print("\n3. LINE MOVEMENT: Does the market get smarter over time?")
rows = con.sql(f"""
    WITH gm AS (
        SELECT ticker, result, last_price FROM '{mp}'
        WHERE event_ticker LIKE 'KXNBAGAME%' AND status='finalized' AND result IN ('yes','no')
    ),
    ft AS (
        SELECT t.ticker, t.yes_price as open_price,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
        FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM gm)
    )
    SELECT
        CASE WHEN gm.last_price > ft.open_price + 5 THEN 'price moved UP 5+'
             WHEN gm.last_price < ft.open_price - 5 THEN 'price moved DOWN 5+'
             ELSE 'price stayed within 5c' END as movement,
        COUNT(*) as total,
        ROUND(SUM(CASE WHEN gm.result='yes' THEN 1.0 ELSE 0.0 END)/COUNT(*)*100, 1) as yes_pct,
        ROUND(SUM(CASE WHEN gm.result='no' THEN 1.0 ELSE 0.0 END)/COUNT(*)*100, 1) as no_pct,
        ROUND(AVG(ft.open_price), 1) as avg_open,
        ROUND(AVG(gm.last_price), 1) as avg_close
    FROM ft JOIN gm ON ft.ticker = gm.ticker
    WHERE ft.rn = 1 AND ft.open_price BETWEEN 61 AND 90
    GROUP BY movement ORDER BY movement
""").fetchall()
for r in rows:
    print(f"   {r[0]:>25s}: {r[1]:>5d} | YES={r[2]:>5.1f}% NO={r[3]:>5.1f}% | open={r[4]}c close={r[5]}c")

# 4. TIME BEFORE GAME: How far before the game was the trade placed?
print("\n4. TIMING: How far before settlement are the best bets?")
rows = con.sql(f"""
    WITH gm AS (
        SELECT ticker, result, close_time FROM '{mp}'
        WHERE event_ticker LIKE 'KXNBAGAME%' AND status='finalized' AND result IN ('yes','no')
    ),
    ft AS (
        SELECT t.ticker, t.yes_price, t.created_time,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
        FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM gm)
    )
    SELECT
        CASE WHEN EXTRACT(EPOCH FROM gm.close_time - ft.created_time)/3600 > 12 THEN '12+ hours before'
             WHEN EXTRACT(EPOCH FROM gm.close_time - ft.created_time)/3600 > 6 THEN '6-12 hours before'
             WHEN EXTRACT(EPOCH FROM gm.close_time - ft.created_time)/3600 > 3 THEN '3-6 hours before'
             WHEN EXTRACT(EPOCH FROM gm.close_time - ft.created_time)/3600 > 1 THEN '1-3 hours before'
             ELSE 'within 1 hour' END as timing,
        COUNT(*) as trades,
        ROUND(SUM(CASE WHEN gm.result='no' THEN 1.0 ELSE 0.0 END)/COUNT(*)*100, 1) as no_win,
        ROUND(SUM(
            CASE WHEN gm.result='no' THEN 1.0 - (100-ft.yes_price)/100.0
                 ELSE -(100-ft.yes_price)/100.0 END
        ) / NULLIF(SUM((100-ft.yes_price)/100.0), 0) * 100, 1) as no_roi
    FROM ft JOIN gm ON ft.ticker = gm.ticker
    WHERE ft.rn = 1 AND ft.yes_price BETWEEN 61 AND 90
    GROUP BY timing ORDER BY timing
""").fetchall()
for r in rows:
    print(f"   {r[0]:>20s}: {r[1]:>5d} | NO wins {r[2]:>5.1f}% | NO ROI {r[3]:>+6.1f}%")

# 5. VOLUME vs EDGE
print("\n5. VOLUME: Does high volume kill the edge?")
rows = con.sql(f"""
    WITH gm AS (
        SELECT ticker, result, volume FROM '{mp}'
        WHERE event_ticker LIKE 'KXNBAGAME%' AND status='finalized' AND result IN ('yes','no')
    ),
    ft AS (
        SELECT t.ticker, t.yes_price,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
        FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM gm)
    )
    SELECT
        CASE WHEN gm.volume < 100000 THEN 'low (<100K)'
             WHEN gm.volume < 1000000 THEN 'med (100K-1M)'
             WHEN gm.volume < 5000000 THEN 'high (1M-5M)'
             ELSE 'very high (5M+)' END as vol,
        COUNT(*) as trades,
        ROUND(SUM(CASE WHEN gm.result='no' THEN 1.0 ELSE 0.0 END)/COUNT(*)*100, 1) as no_win,
        ROUND(SUM(
            CASE WHEN gm.result='no' THEN 1.0 - (100-ft.yes_price)/100.0
                 ELSE -(100-ft.yes_price)/100.0 END
        ) / NULLIF(SUM((100-ft.yes_price)/100.0), 0) * 100, 1) as no_roi
    FROM ft JOIN gm ON ft.ticker = gm.ticker
    WHERE ft.rn = 1 AND ft.yes_price BETWEEN 61 AND 90
    GROUP BY vol ORDER BY vol
""").fetchall()
for r in rows:
    print(f"   {r[0]:>20s}: {r[1]:>5d} | NO wins {r[2]:>5.1f}% | NO ROI {r[3]:>+6.1f}%")

# 6. PRICE GRANULARITY: Per-cent ROI (not bucket)
print("\n6. PER-CENT PRICE ANALYSIS (NBA): ROI at EVERY price point")
rows = con.sql(f"""
    WITH gm AS (
        SELECT ticker, result FROM '{mp}'
        WHERE event_ticker LIKE 'KXNBAGAME%' AND status='finalized' AND result IN ('yes','no')
    ),
    ft AS (
        SELECT t.ticker, t.yes_price,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
        FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM gm)
    )
    SELECT ft.yes_price,
           COUNT(*) as trades,
           ROUND(SUM(CASE WHEN gm.result='no' THEN 1.0 ELSE 0.0 END)/COUNT(*)*100, 1) as no_win,
           ROUND(SUM(
               CASE WHEN gm.result='no' THEN 1.0 - (100-ft.yes_price)/100.0
                    ELSE -(100-ft.yes_price)/100.0 END
           ) / NULLIF(SUM((100-ft.yes_price)/100.0), 0) * 100, 1) as no_roi
    FROM ft JOIN gm ON ft.ticker = gm.ticker
    WHERE ft.rn = 1 AND ft.yes_price BETWEEN 55 AND 95
    GROUP BY ft.yes_price
    HAVING COUNT(*) >= 5
    ORDER BY ft.yes_price
""").fetchall()
for r in rows:
    bar = "#" * max(0, int(r[3] / 5)) if r[3] > 0 else "-" * max(0, int(-r[3] / 5))
    print(f"   {r[0]:>3d}c: {r[1]:>4d} trades | NO wins {r[2]:>5.1f}% | ROI {r[3]:>+7.1f}% | {bar}")

# 7. MULTI-TRADE FLOW: What do the SECOND and THIRD trades tell us?
print("\n7. TRADE FLOW: Does early buying pressure predict the outcome?")
rows = con.sql(f"""
    WITH gm AS (
        SELECT ticker, result FROM '{mp}'
        WHERE event_ticker LIKE 'KXNBAGAME%' AND status='finalized' AND result IN ('yes','no')
    ),
    early_trades AS (
        SELECT t.ticker, t.taker_side, t.count as sz,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
        FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM gm)
    ),
    flow AS (
        SELECT ticker,
               SUM(CASE WHEN taker_side='yes' THEN sz ELSE 0 END) as yes_vol,
               SUM(CASE WHEN taker_side='no' THEN sz ELSE 0 END) as no_vol
        FROM early_trades
        WHERE rn <= 20
        GROUP BY ticker
    )
    SELECT
        CASE WHEN yes_vol > no_vol * 2 THEN 'heavy YES buying (2x+)'
             WHEN yes_vol > no_vol * 1.3 THEN 'moderate YES buying'
             WHEN no_vol > yes_vol * 2 THEN 'heavy NO buying (2x+)'
             WHEN no_vol > yes_vol * 1.3 THEN 'moderate NO buying'
             ELSE 'balanced flow' END as flow_type,
        COUNT(*) as games,
        ROUND(SUM(CASE WHEN gm.result='yes' THEN 1.0 ELSE 0.0 END)/COUNT(*)*100, 1) as yes_win,
        ROUND(SUM(CASE WHEN gm.result='no' THEN 1.0 ELSE 0.0 END)/COUNT(*)*100, 1) as no_win
    FROM flow JOIN gm ON flow.ticker = gm.ticker
    GROUP BY flow_type
    HAVING COUNT(*) >= 10
    ORDER BY flow_type
""").fetchall()
for r in rows:
    print(f"   {r[0]:>30s}: {r[1]:>5d} games | YES wins {r[2]:>5.1f}% | NO wins {r[3]:>5.1f}%")

print("\n" + "=" * 80)
