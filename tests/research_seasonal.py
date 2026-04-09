"""Test NBA playoff reduction and weather seasonality."""
import duckdb
con = duckdb.connect()
mp = "data/trevorjs/markets-*.parquet"
tp = "data/trevorjs/trades-*.parquet"

print("=" * 85)
print("TEST 1: NBA PLAYOFF REDUCTION")
print("=" * 85)

# NBA by playoff period
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
            WHEN mo = 4 AND dy <= 12 THEN '1. Apr 1-12 (reg season end)'
            WHEN mo = 4 AND dy <= 17 THEN '2. Apr 13-17 (play-in)'
            WHEN mo = 4 AND dy > 17 THEN '3. Apr 18-30 (R1 playoffs)'
            WHEN mo = 5 THEN '4. May (R1-R2 playoffs)'
            WHEN mo = 6 THEN '5. Jun (Finals)'
            ELSE '0. Oct-Jan (regular season)'
        END as period,
        COUNT(*) as games,
        ROUND(SUM(CASE WHEN gm.result='no' THEN 1.0 ELSE 0.0 END)/COUNT(*)*100, 1) as no_win,
        ROUND(SUM(
            CASE WHEN gm.result='no' THEN 1.0 - (100-ft.yes_price)/100.0
                 ELSE -(100-ft.yes_price)/100.0 END
        ) / NULLIF(SUM((100-ft.yes_price)/100.0), 0) * 100, 1) as roi
    FROM ft JOIN gm ON ft.ticker = gm.ticker
    WHERE ft.rn = 1 AND ft.yes_price BETWEEN 55 AND 95
    GROUP BY period ORDER BY period
""").fetchall()

for r in rows:
    tag = " <-- WEAK" if r[3] is not None and r[3] < 10 else (" <-- STRONG" if r[3] is not None and r[3] > 30 else "")
    print(f"  {r[0]:35s}: {r[1]:>4d} games | NO wins {r[2]:>5.1f}% | ROI {r[3]:>+7.1f}%{tag}")

# Simulate playoff reductions
print("\n  Backtest: NBA with different playoff reductions")
nba_data = con.sql(f"""
    WITH gm AS (
        SELECT ticker, result FROM '{mp}'
        WHERE event_ticker LIKE 'KXNBAGAME%' AND status='finalized' AND result IN ('yes','no')
    ),
    ft AS (
        SELECT t.ticker, t.yes_price, t.no_price,
               EXTRACT(MONTH FROM t.created_time) as mo,
               EXTRACT(DAY FROM t.created_time) as dy,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
        FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM gm) AND t.created_time >= '2025-01-01'
    )
    SELECT ft.ticker, ft.yes_price, ft.no_price, ft.mo, ft.dy, ft.rn, gm.result
    FROM ft JOIN gm ON ft.ticker = gm.ticker
    WHERE ft.rn = 1 AND ft.yes_price BETWEEN 55 AND 95
    ORDER BY ft.ticker
""").fetchdf()

for label, mult in [("No reduction", 1.0), ("50% reduction", 0.50), ("75% reduction", 0.25), ("Full veto", 0.0)]:
    br = 100.0
    t = 0; w = 0; traded = set()
    for _, row in nba_data.iterrows():
        tk = row["ticker"]
        if tk in traded or br < 2: continue
        yp = int(row["yes_price"]); np_ = int(row["no_price"])
        mo = int(row["mo"]); dy = int(row["dy"])

        ay = max(0.20, 0.50 - (yp - 60) * 0.004)
        edge = (yp / 100.0) - ay
        if edge < 0.08: continue
        nc = np_ / 100.0
        if edge - (0.07 * nc * (1-nc) + 0.005) / nc < 0.03: continue

        b = (yp/100) / nc if nc > 0 else 0
        kr = (b * (1-ay) - ay) / b if b > 0 else 0
        ka = max(0, min(kr * 0.25, 0.12))

        is_playoff = (mo == 4 and dy > 17) or mo in (5, 6)
        if is_playoff: ka *= mult
        if ka <= 0: continue

        bet = min(br * ka, 100)
        contracts = max(1, int(bet / nc))
        cost = contracts * nc
        if cost > br: continue
        tf = 0.07 * nc * (1-nc) * contracts
        if row["result"] == "no": br += contracts * (1.0 - nc) - tf; w += 1
        else: br -= cost + tf
        t += 1; traded.add(tk)

    wr = w / max(t, 1) * 100
    print(f"    {label:20s}: $100 -> ${br:>10,.2f} | {t} trades | {wr:.1f}% WR")

print("\n" + "=" * 85)
print("TEST 2: WEATHER SEASONAL SIZING")
print("=" * 85)

# Weather by season
rows = con.sql(f"""
    WITH gm AS (
        SELECT ticker, result FROM '{mp}'
        WHERE event_ticker LIKE 'KXHIGH%' AND status='finalized' AND result IN ('yes','no')
    ),
    ft AS (
        SELECT t.ticker, t.yes_price,
               EXTRACT(MONTH FROM t.created_time) as mo,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
        FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM gm)
    )
    SELECT
        CASE
            WHEN mo IN (3,4,5) THEN '1. Spring (Mar-May)'
            WHEN mo IN (6,7,8) THEN '2. Summer (Jun-Aug)'
            WHEN mo IN (9,10,11) THEN '3. Fall (Sep-Nov)'
            WHEN mo IN (12,1,2) THEN '4. Winter (Dec-Feb)'
        END as season,
        COUNT(*) as bets,
        ROUND(SUM(CASE WHEN gm.result='no' THEN 1.0 ELSE 0.0 END)/COUNT(*)*100, 1) as no_win,
        ROUND(SUM(
            CASE WHEN gm.result='no' THEN 1.0 - (100-ft.yes_price)/100.0
                 ELSE -(100-ft.yes_price)/100.0 END
        ) / NULLIF(SUM((100-ft.yes_price)/100.0), 0) * 100, 1) as roi
    FROM ft JOIN gm ON ft.ticker = gm.ticker
    WHERE ft.rn = 1 AND ft.yes_price BETWEEN 55 AND 95
    GROUP BY season ORDER BY season
""").fetchall()

print("\n  Weather edge by season:")
for r in rows:
    gemini = ""
    if "Spring" in r[0]: gemini = " | Gemini: HIGH edge expected"
    if "Summer" in r[0]: gemini = " | Gemini: LOW edge expected"
    if "Fall" in r[0]: gemini = " | Gemini: HIGH edge expected"
    if "Winter" in r[0]: gemini = " | Gemini: LOW edge expected"
    print(f"  {r[0]:25s}: {r[1]:>4d} bets | NO wins {r[2]:>5.1f}% | ROI {r[3]:>+7.1f}%{gemini}")

# Per-month
print("\n  Weather edge by month:")
rows2 = con.sql(f"""
    WITH gm AS (
        SELECT ticker, result FROM '{mp}'
        WHERE event_ticker LIKE 'KXHIGH%' AND status='finalized' AND result IN ('yes','no')
    ),
    ft AS (
        SELECT t.ticker, t.yes_price,
               EXTRACT(MONTH FROM t.created_time) as mo,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
        FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM gm)
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
mo_names = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",
            7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}
for r in rows2:
    print(f"    {mo_names[int(r[0])]:3s}: {r[1]:>4d} bets | NO wins {r[2]:>5.1f}% | ROI {r[3]:>+7.1f}%")

print("\n" + "=" * 85)
