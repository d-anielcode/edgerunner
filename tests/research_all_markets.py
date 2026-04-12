"""
Comprehensive scan: What OTHER Kalshi markets have exploitable edges?
We currently only trade game winners. What about player props, totals,
spreads, weather brackets, and everything else?
"""
import duckdb
con = duckdb.connect()
mp = "data/trevorjs/markets-*.parquet"
tp = "data/trevorjs/trades-*.parquet"

print("=" * 90)
print("UNTAPPED MARKET SCAN: Every Kalshi market type with 50+ settled bets")
print("Looking for fade-the-favorite edge (buy NO when YES > 55c)")
print("=" * 90)

# Get ALL prefixes with enough data
rows = con.sql(f"""
    WITH ft AS (
        SELECT t.ticker, t.yes_price,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
        FROM '{tp}' t WHERE t.created_time >= '2025-01-01'
    ),
    combined AS (
        SELECT SPLIT_PART(m.event_ticker, '-', 1) as prefix,
               m.ticker, m.result, ft.yes_price, m.title
        FROM '{mp}' m JOIN ft ON m.ticker = ft.ticker
        WHERE m.status = 'finalized' AND m.result IN ('yes','no')
              AND ft.rn = 1 AND ft.yes_price BETWEEN 55 AND 95
    )
    SELECT prefix,
           COUNT(*) as bets,
           ROUND(SUM(CASE WHEN result='no' THEN 1.0 ELSE 0.0 END)/COUNT(*)*100, 1) as no_win,
           ROUND(SUM(
               CASE WHEN result='no' THEN 1.0 - (100-yes_price)/100.0
                    ELSE -(100-yes_price)/100.0 END
           ) / NULLIF(SUM((100-yes_price)/100.0), 0) * 100, 1) as no_roi,
           MIN(title) as sample_title
    FROM combined
    GROUP BY prefix
    HAVING COUNT(*) >= 50
    ORDER BY no_roi DESC
""").fetchall()

print(f"\n{'Prefix':25s} | {'Bets':>6s} | {'NO Win%':>8s} | {'NO ROI':>8s} | Sample")
print("-" * 100)

already_trading = {
    "KXNBAGAME", "KXNHLGAME", "KXEPLGAME", "KXUCLGAME", "KXLALIGAGAME",
    "KXWNBAGAME", "KXUFCFIGHT", "KXNCAAMBGAME", "KXNCAAWBGAME",
    "KXWTAMATCH", "KXNFLANYTD",
    "KXHIGHNY", "KXHIGHCHI", "KXHIGHMIA", "KXHIGHLA", "KXHIGHSF",
    "KXHIGHHOU", "KXHIGHDEN", "KXHIGHTDC",
}

for r in rows:
    prefix = r[0]
    tag = " [TRADING]" if prefix in already_trading else ""
    roi = r[3] if r[3] else 0
    if roi > 15:
        tag += " ** STRONG"
    elif roi > 0:
        tag += " decent"
    elif roi < -10:
        tag += " LOSING"
    print(f"{prefix:25s} | {r[1]:>6d} | {r[2]:>7.1f}% | {roi:>+7.1f}% | {str(r[4])[:35]}{tag}")

# Deep dive into the most promising ones we're NOT trading
print("\n\n" + "=" * 90)
print("DEEP DIVE: Most promising untapped markets")
print("=" * 90)

interesting = [
    # Player props
    ("NBA Points", "KXNBAPTS%"),
    ("NBA Rebounds", "KXNBAREB%"),
    ("NBA Assists", "KXNBAAST%"),
    ("NBA 3-Pointers", "KXNBA3PT%"),
    ("NBA Double-Double", "KXNBA2D%"),
    # NBA other
    ("NBA Spreads", "KXNBASPREAD%"),
    ("NBA Totals", "KXNBATOTAL%"),
    # NHL props
    ("NHL Goals", "KXNHLGOAL%"),
    ("NHL First Goal", "KXNHLFIRSTGOAL%"),
    ("NHL Points", "KXNHLPTS%"),
    ("NHL Spreads", "KXNHLSPREAD%"),
    ("NHL Totals", "KXNHLTOTAL%"),
    # NFL
    ("NFL Spreads", "KXNFLSPREAD%"),
    ("NFL Totals", "KXNFLTOTAL%"),
    ("NFL First TD", "KXNFLFIRSTTD%"),
    ("NFL 2+ TDs", "KXNFL2TD%"),
    ("NFL Rush Yards", "KXNFLRSHYDS%"),
    ("NFL Rec Yards", "KXNFLRECYDS%"),
    ("NFL Pass Yards", "KXNFLPASSYDS%"),
    # NCAA
    ("NCAAMB Spreads", "KXNCAAMBSPREAD%"),
    ("NCAAMB Totals", "KXNCAAMBTOTAL%"),
    ("NCAAWB Game", "KXNCAAWBGAME%"),
    # Soccer
    ("EPL Spreads", "KXEPLSPREAD%"),
    ("EPL Totals", "KXEPLTOTAL%"),
    ("EPL Both Score", "KXEPLBTTS%"),
    ("UCL Spreads", "KXUCLSPREAD%"),
    ("UCL Totals", "KXUCLTOTAL%"),
    # Weather (different brackets)
    ("Weather Austin", "KXHIGHAUS%"),
    ("Weather Philly", "KXHIGHPHIL%"),
    # Other
    ("March Madness", "KXMARMAD%"),
    ("W March Madness", "KXWMARMAD%"),
    ("ATP Tennis", "KXATPMATCH%"),
    ("ATP Challenger", "KXATPCHALLENGERMATCH%"),
    ("Boxing", "KXBOXING%"),
    # Non-sports
    ("Bitcoin Daily", "KXBTCD%"),
    ("Ethereum Daily", "KXETHD%"),
    ("S&P 500 Daily", "KXINXU%"),
    ("Nasdaq Daily", "KXNASDAQ100U%"),
    ("Fed Decision", "KXFEDDECISION%"),
]

print(f"\n{'Market':25s} | {'Bets':>6s} | {'NO Win%':>8s} | {'NO ROI':>8s} | Action")
print("-" * 70)

for name, pattern in interesting:
    r = con.sql(f"""
        WITH ft AS (
            SELECT t.ticker, t.yes_price,
                   ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
            FROM '{tp}' t WHERE t.ticker IN (
                SELECT ticker FROM '{mp}' WHERE event_ticker LIKE '{pattern}' AND status='finalized' AND result IN ('yes','no')
            ) AND t.created_time >= '2025-01-01'
        )
        SELECT COUNT(*) as bets,
               ROUND(SUM(CASE WHEN m.result='no' THEN 1.0 ELSE 0.0 END)/NULLIF(COUNT(*),0)*100, 1) as no_win,
               ROUND(SUM(
                   CASE WHEN m.result='no' THEN 1.0 - (100-ft.yes_price)/100.0
                        ELSE -(100-ft.yes_price)/100.0 END
               ) / NULLIF(SUM((100-ft.yes_price)/100.0), 0) * 100, 1) as no_roi
        FROM ft JOIN '{mp}' m ON ft.ticker = m.ticker
        WHERE ft.rn = 1 AND ft.yes_price BETWEEN 55 AND 95
    """).fetchone()

    if r and r[0] and r[0] >= 20:
        roi = r[2] if r[2] else 0
        action = "ADD" if roi > 20 else ("consider" if roi > 5 else ("skip" if roi > -5 else "AVOID"))
        print(f"{name:25s} | {r[0]:>6d} | {r[1]:>7.1f}% | {roi:>+7.1f}% | {action}")

# Per-price for the top candidates
print("\n\n" + "=" * 90)
print("PER-PRICE ANALYSIS: Top untapped markets")
print("=" * 90)

for name, pattern in [
    ("NBA Points (Over X)", "KXNBAPTS%"),
    ("NBA Rebounds", "KXNBAREB%"),
    ("NBA Spreads", "KXNBASPREAD%"),
    ("NFL First TD", "KXNFLFIRSTTD%"),
    ("NFL 2+ TDs", "KXNFL2TD%"),
    ("NHL Goals", "KXNHLGOAL%"),
    ("EPL Both Score", "KXEPLBTTS%"),
    ("Bitcoin Daily", "KXBTCD%"),
    ("S&P 500 Daily", "KXINXU%"),
]:
    rows2 = con.sql(f"""
        WITH ft AS (
            SELECT t.ticker, t.yes_price,
                   ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
            FROM '{tp}' t WHERE t.ticker IN (
                SELECT ticker FROM '{mp}' WHERE event_ticker LIKE '{pattern}' AND status='finalized' AND result IN ('yes','no')
            ) AND t.created_time >= '2025-01-01'
        )
        SELECT
            CASE WHEN ft.yes_price BETWEEN 55 AND 65 THEN '55-65c'
                 WHEN ft.yes_price BETWEEN 66 AND 75 THEN '66-75c'
                 WHEN ft.yes_price BETWEEN 76 AND 85 THEN '76-85c'
                 WHEN ft.yes_price BETWEEN 86 AND 95 THEN '86-95c' END as bucket,
            COUNT(*) as bets,
            ROUND(SUM(CASE WHEN m.result='no' THEN 1.0 ELSE 0.0 END)/NULLIF(COUNT(*),0)*100, 1) as no_win,
            ROUND(SUM(
                CASE WHEN m.result='no' THEN 1.0 - (100-ft.yes_price)/100.0
                     ELSE -(100-ft.yes_price)/100.0 END
            ) / NULLIF(SUM((100-ft.yes_price)/100.0), 0) * 100, 1) as no_roi
        FROM ft JOIN '{mp}' m ON ft.ticker = m.ticker
        WHERE ft.rn = 1 AND ft.yes_price BETWEEN 55 AND 95
        GROUP BY bucket
        HAVING COUNT(*) >= 10
        ORDER BY bucket
    """).fetchall()

    if rows2:
        total_bets = sum(r[1] for r in rows2 if r[0])
        if total_bets < 20:
            continue
        print(f"\n  {name} ({total_bets} bets):")
        for r in rows2:
            if r[0]:
                bar = "#" * max(0, int((r[3] or 0) / 10)) if r[3] and r[3] > 0 else ""
                print(f"    {r[0]:>7s}: {r[1]:>5d} bets | NO wins {r[2]:>5.1f}% | ROI {r[3]:>+7.1f}% {bar}")

print("\n" + "=" * 90)
