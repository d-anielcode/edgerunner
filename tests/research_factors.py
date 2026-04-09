"""
Research: Test 3 new factors against our dataset.
1. Home vs Away underdog
2. Day of week (weekend vs weekday)
3. Back-to-back (would need schedule data, so we proxy with game density)
"""
import duckdb
con = duckdb.connect()
mp = "data/trevorjs/markets-*.parquet"
tp = "data/trevorjs/trades-*.parquet"

print("=" * 85)
print("FACTOR RESEARCH: Home/Away, Day of Week, Game Density")
print("=" * 85)

# ================================================================
# FACTOR 1: HOME vs AWAY UNDERDOG
# Kalshi ticker format: KXNBAGAME-26APR07SACGSW-GSW
# The 6-letter game ID is AWYHOM (away team first, home team last)
# The final segment after the last dash is the team this market is for
# So SACGSW-GSW means GSW is the market, and GSW is the HOME team (last 3 of 6)
# ================================================================
print("\n1. HOME vs AWAY UNDERDOG")
print("   Kalshi ticker: KXNBAGAME-26APR07SACGSW-GSW")
print("   Game ID SACGSW: SAC=away, GSW=home. Market is for GSW (home).")
print("   We buy NO on the favorite. Is fading a HOME favorite better than AWAY?")

for sport, pat in [("NBA", "KXNBAGAME%"), ("NHL", "KXNHLGAME%")]:
    rows = con.sql(f"""
        WITH gm AS (
            SELECT ticker, result, event_ticker FROM '{mp}'
            WHERE event_ticker LIKE '{pat}' AND status='finalized' AND result IN ('yes','no')
        ),
        ft AS (
            SELECT t.ticker, t.yes_price,
                   ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
            FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM gm)
                AND t.created_time >= '2025-01-01'
        ),
        parsed AS (
            SELECT ft.ticker, ft.yes_price, gm.result,
                   -- Split ticker by dash, get parts
                   SPLIT_PART(ft.ticker, '-', 2) as date_game,
                   SPLIT_PART(ft.ticker, '-', 3) as market_team
            FROM ft JOIN gm ON ft.ticker = gm.ticker
            WHERE ft.rn = 1 AND ft.yes_price BETWEEN 61 AND 95
        ),
        with_loc AS (
            SELECT *,
                   -- Game ID is last 6 chars of date_game (e.g., 26APR07SACGSW -> SACGSW)
                   RIGHT(date_game, 6) as game_id
            FROM parsed
        )
        SELECT
            CASE
                WHEN LENGTH(game_id) = 6 AND market_team = SUBSTRING(game_id, 4, 3) THEN 'HOME favorite'
                WHEN LENGTH(game_id) = 6 AND market_team = SUBSTRING(game_id, 1, 3) THEN 'AWAY favorite'
                ELSE 'unknown'
            END as location,
            COUNT(*) as games,
            ROUND(SUM(CASE WHEN result='no' THEN 1.0 ELSE 0.0 END)/COUNT(*)*100, 1) as no_win,
            ROUND(SUM(
                CASE WHEN result='no' THEN 1.0 - (100-yes_price)/100.0
                     ELSE -(100-yes_price)/100.0 END
            ) / NULLIF(SUM((100-yes_price)/100.0), 0) * 100, 1) as roi
        FROM with_loc
        WHERE game_id IS NOT NULL AND LENGTH(game_id) = 6
        GROUP BY location
        ORDER BY location
    """).fetchall()

    print(f"\n  {sport}:")
    for r in rows:
        marker = " <-- BETTER" if r[3] and r[3] > 30 else ""
        print(f"    {r[0]:18s}: {r[1]:>5d} games | NO wins {r[2]:>5.1f}% | ROI {r[3]:>+7.1f}%{marker}")

# ================================================================
# FACTOR 2: DAY OF WEEK
# ================================================================
print("\n\n2. DAY OF WEEK: Weekend vs Weekday")
print("   Gemini says weekends have more casual bettors = more mispricing")

for sport, pat in [("NBA", "KXNBAGAME%"), ("NHL", "KXNHLGAME%"), ("ALL SPORTS", "KX%")]:
    if sport == "ALL SPORTS":
        where = "(event_ticker LIKE 'KXNBAGAME%' OR event_ticker LIKE 'KXNHLGAME%' OR event_ticker LIKE 'KXEPLGAME%' OR event_ticker LIKE 'KXNFLANYTD%' OR event_ticker LIKE 'KXUFCFIGHT%')"
    else:
        where = f"event_ticker LIKE '{pat}'"

    rows = con.sql(f"""
        WITH gm AS (
            SELECT ticker, result FROM '{mp}'
            WHERE {where} AND status='finalized' AND result IN ('yes','no')
        ),
        ft AS (
            SELECT t.ticker, t.yes_price,
                   EXTRACT(DOW FROM t.created_time) as dow,
                   ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
            FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM gm)
                AND t.created_time >= '2025-01-01'
        )
        SELECT
            CASE WHEN dow IN (0, 6) THEN 'Weekend (Sat/Sun)'
                 ELSE 'Weekday (Mon-Fri)' END as day_type,
            COUNT(*) as games,
            ROUND(SUM(CASE WHEN gm.result='no' THEN 1.0 ELSE 0.0 END)/COUNT(*)*100, 1) as no_win,
            ROUND(SUM(
                CASE WHEN gm.result='no' THEN 1.0 - (100-ft.yes_price)/100.0
                     ELSE -(100-ft.yes_price)/100.0 END
            ) / NULLIF(SUM((100-ft.yes_price)/100.0), 0) * 100, 1) as roi
        FROM ft JOIN gm ON ft.ticker = gm.ticker
        WHERE ft.rn = 1 AND ft.yes_price BETWEEN 55 AND 95
        GROUP BY day_type
        ORDER BY day_type
    """).fetchall()

    print(f"\n  {sport}:")
    for r in rows:
        marker = " <-- BETTER" if r[3] and r[3] > 30 else ""
        print(f"    {r[0]:20s}: {r[1]:>5d} games | NO wins {r[2]:>5.1f}% | ROI {r[3]:>+7.1f}%{marker}")

# Per day breakdown for NBA
print("\n  NBA by specific day:")
rows = con.sql(f"""
    WITH gm AS (
        SELECT ticker, result FROM '{mp}'
        WHERE event_ticker LIKE 'KXNBAGAME%' AND status='finalized' AND result IN ('yes','no')
    ),
    ft AS (
        SELECT t.ticker, t.yes_price,
               EXTRACT(DOW FROM t.created_time) as dow,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
        FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM gm)
            AND t.created_time >= '2025-01-01'
    )
    SELECT dow,
           COUNT(*) as games,
           ROUND(SUM(CASE WHEN gm.result='no' THEN 1.0 ELSE 0.0 END)/COUNT(*)*100, 1) as no_win,
           ROUND(SUM(
               CASE WHEN gm.result='no' THEN 1.0 - (100-ft.yes_price)/100.0
                    ELSE -(100-ft.yes_price)/100.0 END
           ) / NULLIF(SUM((100-ft.yes_price)/100.0), 0) * 100, 1) as roi
    FROM ft JOIN gm ON ft.ticker = gm.ticker
    WHERE ft.rn = 1 AND ft.yes_price BETWEEN 55 AND 95
    GROUP BY dow ORDER BY dow
""").fetchall()
day_names = {0: "Sun", 1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat"}
for r in rows:
    d = day_names.get(int(r[0]), "?")
    print(f"    {d:3s}: {r[1]:>5d} games | NO wins {r[2]:>5.1f}% | ROI {r[3]:>+7.1f}%")

# ================================================================
# FACTOR 3: GAME DENSITY (proxy for back-to-back)
# If the same team appears in games on consecutive days, the 2nd game
# is a B2B. We can detect this from ticker dates.
# ================================================================
print("\n\n3. BACK-TO-BACK PROXY")
print("   If we see the same team in games on consecutive dates,")
print("   the 2nd game is likely a B2B situation.")

# Extract team + date from tickers, find consecutive games
rows = con.sql(f"""
    WITH gm AS (
        SELECT ticker, result, event_ticker FROM '{mp}'
        WHERE event_ticker LIKE 'KXNBAGAME%' AND status='finalized' AND result IN ('yes','no')
    ),
    ft AS (
        SELECT t.ticker, t.yes_price, t.created_time,
               CAST(t.created_time AS DATE) as trade_date,
               -- Extract date portion from ticker (e.g., 26APR07 from KXNBAGAME-26APR07SACGSW-GSW)
               SPLIT_PART(t.ticker, '-', 2) as ticker_mid,
               SPLIT_PART(t.ticker, '-', 3) as team,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
        FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM gm)
            AND t.created_time >= '2025-01-01'
    ),
    team_games AS (
        SELECT ft.ticker, ft.yes_price, ft.trade_date, ft.team, gm.result,
               LAG(ft.trade_date) OVER (PARTITION BY ft.team ORDER BY ft.trade_date) as prev_game_date
        FROM ft JOIN gm ON ft.ticker = gm.ticker
        WHERE ft.rn = 1 AND ft.yes_price BETWEEN 61 AND 95
    )
    SELECT
        CASE WHEN trade_date - prev_game_date <= 1 THEN 'B2B (2nd game)'
             WHEN trade_date - prev_game_date <= 2 THEN 'Short rest (2 days)'
             ELSE 'Normal rest (3+ days)' END as rest,
        COUNT(*) as games,
        ROUND(SUM(CASE WHEN result='no' THEN 1.0 ELSE 0.0 END)/COUNT(*)*100, 1) as no_win,
        ROUND(SUM(
            CASE WHEN result='no' THEN 1.0 - (100-yes_price)/100.0
                 ELSE -(100-yes_price)/100.0 END
        ) / NULLIF(SUM((100-yes_price)/100.0), 0) * 100, 1) as roi
    FROM team_games
    WHERE prev_game_date IS NOT NULL
    GROUP BY rest
    ORDER BY rest
""").fetchall()

print("\n  NBA favorites by rest situation:")
for r in rows:
    marker = " <-- MORE UPSETS" if r[2] and r[2] > 40 else ""
    print(f"    {r[0]:25s}: {r[1]:>5d} games | NO wins {r[2]:>5.1f}% | ROI {r[3]:>+7.1f}%{marker}")

# Same for NHL
print("\n  NHL favorites by rest:")
rows2 = con.sql(f"""
    WITH gm AS (
        SELECT ticker, result FROM '{mp}'
        WHERE event_ticker LIKE 'KXNHLGAME%' AND status='finalized' AND result IN ('yes','no')
    ),
    ft AS (
        SELECT t.ticker, t.yes_price,
               CAST(t.created_time AS DATE) as trade_date,
               SPLIT_PART(t.ticker, '-', 3) as team,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
        FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM gm)
            AND t.created_time >= '2025-01-01'
    ),
    tg AS (
        SELECT ft.ticker, ft.yes_price, ft.trade_date, ft.team, gm.result,
               LAG(ft.trade_date) OVER (PARTITION BY ft.team ORDER BY ft.trade_date) as prev
        FROM ft JOIN gm ON ft.ticker = gm.ticker
        WHERE ft.rn = 1 AND ft.yes_price BETWEEN 61 AND 95
    )
    SELECT
        CASE WHEN trade_date - prev <= 1 THEN 'B2B'
             WHEN trade_date - prev <= 2 THEN 'Short rest'
             ELSE 'Normal rest' END as rest,
        COUNT(*) as g,
        ROUND(SUM(CASE WHEN result='no' THEN 1.0 ELSE 0.0 END)/COUNT(*)*100, 1) as nw,
        ROUND(SUM(
            CASE WHEN result='no' THEN 1.0 - (100-yes_price)/100.0
                 ELSE -(100-yes_price)/100.0 END
        ) / NULLIF(SUM((100-yes_price)/100.0), 0) * 100, 1) as roi
    FROM tg WHERE prev IS NOT NULL
    GROUP BY rest ORDER BY rest
""").fetchall()
for r in rows2:
    print(f"    {r[0]:25s}: {r[1]:>5d} games | NO wins {r[2]:>5.1f}% | ROI {r[3]:>+7.1f}%")

print("\n" + "=" * 85)
print("SUMMARY: Which factors have a real edge?")
print("=" * 85)
