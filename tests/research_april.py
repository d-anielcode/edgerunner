"""Research: Why is April bad? Is it a structural problem or one-time noise?"""
import duckdb
con = duckdb.connect()
mp = "data/trevorjs/markets-*.parquet"
tp = "data/trevorjs/trades-*.parquet"

print("=" * 80)
print("APRIL ANALYSIS: Is it structurally bad or one-time noise?")
print("=" * 80)

# 1. April 2025 breakdown by sport
print("\n1. APRIL 2025: What caused the -71% loss?")
for name, pat in [("NBA", "KXNBAGAME%"), ("NHL", "KXNHLGAME%"),
                   ("UCL", "KXUCLGAME%"), ("Weather", "KXHIGH%"),
                   ("EPL", "KXEPLGAME%"), ("WTA", "KXWTAMATCH%")]:
    r = con.sql(f"""
        WITH gm AS (
            SELECT ticker, result FROM '{mp}'
            WHERE event_ticker LIKE '{pat}' AND status='finalized' AND result IN ('yes','no')
        ),
        ft AS (
            SELECT t.ticker, t.yes_price,
                   ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
            FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM gm)
                AND EXTRACT(YEAR FROM t.created_time) = 2025
                AND EXTRACT(MONTH FROM t.created_time) = 4
        )
        SELECT COUNT(*) as g,
               SUM(CASE WHEN gm.result='no' THEN 1 ELSE 0 END) as nw,
               ROUND(SUM(CASE WHEN gm.result='no' THEN 1.0 ELSE 0.0 END)/NULLIF(COUNT(*),0)*100, 1) as np,
               ROUND(SUM(
                   CASE WHEN gm.result='no' THEN 1.0 - (100-ft.yes_price)/100.0
                        ELSE -(100-ft.yes_price)/100.0 END
               ) / NULLIF(SUM((100-ft.yes_price)/100.0), 0) * 100, 1) as roi
        FROM ft JOIN gm ON ft.ticker = gm.ticker
        WHERE ft.rn = 1 AND ft.yes_price BETWEEN 55 AND 95
    """).fetchone()
    if r and r[0] > 0:
        tag = " <-- PLAYOFFS (edge disappears)" if name == "NHL" else ""
        print(f"  {name:10s}: {r[0]:>3d} games | {r[1]} NO wins ({r[2]}%) | ROI {r[3]:>+.1f}%{tag}")

# 2. NBA month-by-month (is April specifically bad for NBA?)
print("\n2. NBA BY MONTH (is April NBA-specific?)")
rows = con.sql(f"""
    WITH gm AS (
        SELECT ticker, result FROM '{mp}'
        WHERE event_ticker LIKE 'KXNBAGAME%' AND status='finalized' AND result IN ('yes','no')
    ),
    ft AS (
        SELECT t.ticker, t.yes_price,
               EXTRACT(MONTH FROM t.created_time) as mo,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
        FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM gm)
    )
    SELECT mo, COUNT(*) as g,
           ROUND(SUM(CASE WHEN gm.result='no' THEN 1.0 ELSE 0.0 END)/NULLIF(COUNT(*),0)*100, 1) as np,
           ROUND(SUM(
               CASE WHEN gm.result='no' THEN 1.0 - (100-ft.yes_price)/100.0
                    ELSE -(100-ft.yes_price)/100.0 END
           ) / NULLIF(SUM((100-ft.yes_price)/100.0), 0) * 100, 1) as roi
    FROM ft JOIN gm ON ft.ticker = gm.ticker
    WHERE ft.rn = 1 AND ft.yes_price BETWEEN 55 AND 95
    GROUP BY mo ORDER BY mo
""").fetchall()
mo_names = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",
            10:"Oct",11:"Nov",12:"Dec"}
for r in rows:
    m = int(r[0])
    name = mo_names.get(m, f"M{m}")
    playoff = " <-- NBA playoffs start" if m == 4 else (" <-- NBA Finals" if m == 6 else "")
    print(f"  {name:3s}: {r[1]:>4d} games | NO wins {r[2]:>5.1f}% | ROI {r[3]:>+7.1f}%{playoff}")

# 3. NHL month-by-month
print("\n3. NHL BY MONTH")
rows = con.sql(f"""
    WITH gm AS (
        SELECT ticker, result FROM '{mp}'
        WHERE event_ticker LIKE 'KXNHLGAME%' AND status='finalized' AND result IN ('yes','no')
    ),
    ft AS (
        SELECT t.ticker, t.yes_price,
               EXTRACT(MONTH FROM t.created_time) as mo,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
        FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM gm)
    )
    SELECT mo, COUNT(*) as g,
           ROUND(SUM(CASE WHEN gm.result='no' THEN 1.0 ELSE 0.0 END)/NULLIF(COUNT(*),0)*100, 1) as np,
           ROUND(SUM(
               CASE WHEN gm.result='no' THEN 1.0 - (100-ft.yes_price)/100.0
                    ELSE -(100-ft.yes_price)/100.0 END
           ) / NULLIF(SUM((100-ft.yes_price)/100.0), 0) * 100, 1) as roi
    FROM ft JOIN gm ON ft.ticker = gm.ticker
    WHERE ft.rn = 1 AND ft.yes_price BETWEEN 55 AND 95
    GROUP BY mo ORDER BY mo
""").fetchall()
for r in rows:
    m = int(r[0])
    name = mo_names.get(m, f"M{m}")
    playoff = " <-- PLAYOFFS (veto active)" if m in (4,5,6) else ""
    print(f"  {name:3s}: {r[1]:>4d} games | NO wins {r[2]:>5.1f}% | ROI {r[3]:>+7.1f}%{playoff}")

# 4. Weather in April (multi-year)
print("\n4. WEATHER IN APRIL (multi-year - is it consistent?)")
rows = con.sql(f"""
    WITH gm AS (
        SELECT ticker, result FROM '{mp}'
        WHERE event_ticker LIKE 'KXHIGH%' AND status='finalized' AND result IN ('yes','no')
    ),
    ft AS (
        SELECT t.ticker, t.yes_price,
               EXTRACT(YEAR FROM t.created_time) as yr,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
        FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM gm)
                AND EXTRACT(MONTH FROM t.created_time) = 4
    )
    SELECT yr, COUNT(*) as g,
           ROUND(SUM(CASE WHEN gm.result='no' THEN 1.0 ELSE 0.0 END)/NULLIF(COUNT(*),0)*100, 1) as np,
           ROUND(SUM(
               CASE WHEN gm.result='no' THEN 1.0 - (100-ft.yes_price)/100.0
                    ELSE -(100-ft.yes_price)/100.0 END
           ) / NULLIF(SUM((100-ft.yes_price)/100.0), 0) * 100, 1) as roi
    FROM ft JOIN gm ON ft.ticker = gm.ticker
    WHERE ft.rn = 1 AND ft.yes_price BETWEEN 55 AND 95
    GROUP BY yr ORDER BY yr
""").fetchall()
for r in rows:
    print(f"  {int(r[0])}: {r[1]:>4d} bets | NO wins {r[2]:>5.1f}% | ROI {r[3]:>+7.1f}%")

# 5. Per-year backtest starting $100 each Jan 1
print("\n" + "=" * 80)
print("5. PER-YEAR BACKTEST: $100 fresh start each year")
print("   Using current strategy (per-price Kelly, volume filter, etc.)")
print("=" * 80)

# We need all data, not just 2025
all_trades = con.sql(f"""
    WITH sport_patterns AS (
        SELECT ticker, result, event_ticker, volume,
               CASE
                   WHEN event_ticker LIKE 'KXNBAGAME%' THEN 'NBA'
                   WHEN event_ticker LIKE 'KXNHLGAME%' THEN 'NHL'
                   WHEN event_ticker LIKE 'KXHIGH%' THEN 'WEATHER'
               END as sport
        FROM '{mp}'
        WHERE (event_ticker LIKE 'KXNBAGAME%' OR event_ticker LIKE 'KXNHLGAME%'
               OR event_ticker LIKE 'KXHIGH%')
              AND status='finalized' AND result IN ('yes','no')
    ),
    ft AS (
        SELECT t.ticker, t.yes_price, t.no_price, t.created_time,
               CAST(t.created_time AS DATE) as trade_date,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
        FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM sport_patterns)
    )
    SELECT ft.*, sport_patterns.result, sport_patterns.sport, sport_patterns.volume
    FROM ft JOIN sport_patterns ON ft.ticker = sport_patterns.ticker
    WHERE ft.rn = 1 AND sport_patterns.sport IS NOT NULL
    ORDER BY ft.created_time
""").fetchdf()

def per_price(sport, yp):
    if sport == "NBA": return max(0.20, 0.50 - (yp - 60) * 0.004)
    if sport == "NHL": return max(0.30, 0.55 - (yp - 60) * 0.003)
    if sport == "WEATHER":
        for (lo, hi), rate in {(55,65): 0.404, (66,75): 0.417, (76,85): 0.417, (86,95): 0.419}.items():
            if lo <= yp <= hi: return rate
    return None

years = sorted(all_trades["trade_date"].apply(lambda x: str(x)[:4]).unique())

for year in years:
    yr_data = all_trades[all_trades["trade_date"].apply(lambda x: str(x)[:4]) == year]
    bankroll = 100.0
    peak = 100.0
    max_dd = 0.0
    trades = 0
    wins = 0
    monthly = {}
    traded = set()

    for _, row in yr_data.iterrows():
        d = str(row["trade_date"])
        mo = d[:7]
        t = row["ticker"]
        yp = int(row["yes_price"])
        np_ = int(row["no_price"])
        sp = row["sport"]
        res = row["result"]
        vol = float(row["volume"])

        if t in traded or bankroll < 2:
            continue

        # NHL playoff veto
        m_num = int(d[5:7]); d_num = int(d[8:10])
        if sp == "NHL" and ((m_num > 4 or (m_num == 4 and d_num > 16)) and m_num < 10):
            continue

        # NBA volume filter
        if sp == "NBA" and 500_000 <= vol <= 2_000_000:
            continue

        min_p = 55 if sp == "WEATHER" else 61
        max_p = 95 if sp in ("NBA", "NHL", "WEATHER") else 90
        if yp < min_p or yp > max_p:
            continue

        ay = per_price(sp, yp)
        if ay is None:
            continue

        edge = (yp / 100.0) - ay
        me = 0.10 if sp == "WEATHER" else (0.05 if sp == "NHL" else 0.08)
        if edge < me:
            continue

        nc = np_ / 100.0
        fee = 0.07 * nc * (1 - nc)
        if edge - (fee + 0.005) / nc < 0.03:
            continue

        yc = yp / 100.0
        b = yc / nc if nc > 0 else 0
        pw = 1 - ay
        kr = (b * pw - ay) / b if b > 0 else 0
        km = 0.25 if sp in ("NBA", "NHL") else 0.25
        mp_ = 0.12
        ka = max(0, min(kr * km, mp_))
        if ka <= 0:
            continue

        bet = min(bankroll * ka, 100)
        contracts = max(1, int(bet / nc))
        cost = contracts * nc
        if cost > bankroll:
            continue

        tf = 0.07 * nc * (1 - nc) * contracts
        if res == "no":
            bankroll += contracts * (1.0 - nc) - tf
            wins += 1
        else:
            bankroll -= cost + tf

        trades += 1
        traded.add(t)
        monthly[mo] = bankroll
        if bankroll > peak: peak = bankroll
        dd = (peak - bankroll) / peak if peak > 0 else 0
        if dd > max_dd: max_dd = dd

    wr = wins / max(trades, 1) * 100
    print(f"\n  {year}: $100 -> ${bankroll:>10,.2f} | {trades} trades | {wr:.1f}% WR | {max_dd*100:.1f}% MaxDD")

    # Show monthly for this year
    for mo in sorted(monthly.keys()):
        m_num = int(mo[5:7])
        m_name = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",
                  7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}[m_num]
        marker = " <-- APRIL" if m_num == 4 else ""
        print(f"    {m_name}: ${monthly[mo]:>10,.2f}{marker}")

print("\n" + "=" * 80)
