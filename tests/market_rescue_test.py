"""
Test if profit-taking rescues previously-dropped markets.
Also test if new Kalshi markets (MLB, MLS, etc.) become viable with PT.

For EVERY market type in the dataset, simulate:
- Hold to settlement
- 50%, 100%, 150%, 200%, 300% profit-take

Report which markets flip from losing to profitable with PT.
"""
import math, time, duckdb
from collections import defaultdict

t0 = time.time()

con = duckdb.connect()
mp = "data/trevorjs/markets-*.parquet"
tp = "data/trevorjs/trades-*.parquet"

# ALL possible market prefixes (including disabled/untested)
ALL_PREFIXES = {
    "KXNBAGAME": "NBA", "KXNHLGAME": "NHL", "KXEPLGAME": "EPL",
    "KXUCLGAME": "UCL", "KXLALIGAGAME": "LALIGA", "KXWNBAGAME": "WNBA",
    "KXUFCFIGHT": "UFC", "KXNCAAMBGAME": "NCAAMB", "KXNCAAWBGAME": "NCAAWB",
    "KXATPMATCH": "ATP", "KXWTAMATCH": "WTA",  # Currently disabled
    "KXNFLANYTD": "NFLTD",
    "KXNHLSPREAD": "NHLSPREAD", "KXNHLFIRSTGOAL": "NHLFG",  # NHLFG disabled
    "KXNBASPREAD": "NBASPREAD", "KXNBA2D": "NBA2D",  # NBA2D disabled
    "KXNFLSPREAD": "NFLSPREAD",
    "KXMLBGAME": "MLB",  # Never tried
    "KXMLSGAME": "MLS",  # Never tried
    "KXCFBGAME": "CFB",  # New, no data yet
}

case_parts = [f"WHEN event_ticker LIKE '{p}%' THEN '{s}'" for p, s in ALL_PREFIXES.items()]
case_stmt = " ".join(case_parts)
like_clauses = " OR ".join(f"event_ticker LIKE '{p}%'" for p in ALL_PREFIXES.keys())

print("Loading ALL markets across every sport type...")

mdf = con.sql(f"""
    WITH gm AS (
        SELECT ticker, result, event_ticker, volume,
               CASE {case_stmt} END as sport
        FROM '{mp}'
        WHERE ({like_clauses}) AND status='finalized' AND result IN ('yes','no')
    ),
    ts AS (
        SELECT ticker, MIN(created_time) as ft, COUNT(*) as tc
        FROM '{tp}'
        WHERE ticker IN (SELECT ticker FROM gm) AND created_time >= '2025-01-01'
        GROUP BY ticker HAVING COUNT(*) >= 20
    ),
    ft AS (
        SELECT t.ticker, t.yes_price, t.no_price, CAST(t.created_time AS DATE) as td
        FROM '{tp}' t JOIN ts ON t.ticker = ts.ticker AND t.created_time = ts.ft
    )
    SELECT ft.ticker, ft.yes_price, ft.td, gm.result, gm.sport, gm.volume
    FROM ft JOIN gm ON ft.ticker = gm.ticker JOIN ts ON ft.ticker = ts.ticker
    WHERE gm.sport IS NOT NULL
    ORDER BY ft.td
""").fetchdf()

print(f"  {len(mdf)} total markets loaded ({time.time()-t0:.1f}s)")

# Count per sport
sport_counts = mdf.groupby("sport").size().to_dict()
print("\n  Per sport:")
for s in sorted(sport_counts.keys()):
    print(f"    {s:<12} {sport_counts[s]:>5} markets")

# Filter to YES >= 55 (our widest possible range)
markets = []
for _, row in mdf.iterrows():
    yp = int(row["yes_price"])
    if yp < 55 or yp > 95:
        continue
    markets.append({
        "ticker": row["ticker"], "yp": yp, "sport": row["sport"],
        "d": str(row["td"]), "res": row["result"], "nc": 100 - yp,
    })

print(f"\n  {len(markets)} markets in 55-95c range")

# Load trajectories
tickers = [m["ticker"] for m in markets]
con.execute("CREATE TEMP TABLE tks (ticker VARCHAR)")
for t in tickers:
    con.execute("INSERT INTO tks VALUES (?)", [t])

print("\nLoading min YES prices...")
min_df = con.sql(f"""
    WITH ft AS (
        SELECT ticker, MIN(created_time) as ft FROM '{tp}'
        WHERE ticker IN (SELECT ticker FROM tks) AND created_time >= '2025-01-01'
        GROUP BY ticker
    )
    SELECT t.ticker, MIN(t.yes_price) as min_yes
    FROM '{tp}' t JOIN ft ON t.ticker = ft.ticker
    WHERE t.created_time > ft.ft AND t.created_time >= '2025-01-01'
    GROUP BY t.ticker
""").fetchdf()

min_yes_map = {}
for _, row in min_df.iterrows():
    min_yes_map[row["ticker"]] = int(row["min_yes"])

# Find crossing points for all PT levels
ptx = {}
for pt_pct in [50, 100, 150, 200, 300]:
    mult = 1 + pt_pct / 100.0
    cands = []
    for m in markets:
        ym = int(100 - mult * m["nc"])
        mv = min_yes_map.get(m["ticker"], 999)
        if ym > 0 and mv <= ym:
            cands.append((m["ticker"], ym))

    if not cands:
        continue

    con.execute(f"DROP TABLE IF EXISTS ptc{pt_pct}")
    con.execute(f"CREATE TEMP TABLE ptc{pt_pct} (ticker VARCHAR, ym INTEGER)")
    for tk, ym in cands:
        con.execute(f"INSERT INTO ptc{pt_pct} VALUES (?, ?)", [tk, ym])

    cdf = con.sql(f"""
        WITH ft AS (
            SELECT ticker, MIN(created_time) as ft FROM '{tp}'
            WHERE ticker IN (SELECT ticker FROM ptc{pt_pct}) AND created_time >= '2025-01-01'
            GROUP BY ticker
        ),
        cr AS (
            SELECT t.ticker, t.yes_price,
                   ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
            FROM '{tp}' t JOIN ft ON t.ticker = ft.ticker
            JOIN ptc{pt_pct} c ON t.ticker = c.ticker
            WHERE t.created_time > ft.ft AND t.created_time >= '2025-01-01'
            AND t.yes_price <= c.ym
        )
        SELECT ticker, yes_price FROM cr WHERE rn = 1
    """).fetchdf()

    for _, r in cdf.iterrows():
        ptx.setdefault(r["ticker"], {})[pt_pct] = int(r["yes_price"])

print(f"  Done ({time.time()-t0:.1f}s)")


def fee(c):
    p = c / 100.0
    return math.ceil(0.07 * p * (1 - p) * 100) / 100


# Simulate per sport × PT level (flat $1 per trade, no compounding)
print("\n" + "=" * 95)
print("MARKET RESCUE TEST — Which markets become profitable with profit-taking?")
print("=" * 95)
print(f"\n{'Sport':<12} {'N':>5} {'HOLD':>10} {'50%PT':>10} {'100%PT':>10} {'150%PT':>10} {'200%PT':>10} {'300%PT':>10} {'Best':>6} {'Rescue?':>8}")
print("-" * 95)

results = {}
for sport in sorted(set(m["sport"] for m in markets)):
    sm = [m for m in markets if m["sport"] == sport]
    if len(sm) < 10:
        continue

    row = {}
    for pt in [None, 50, 100, 150, 200, 300]:
        total_pnl = 0.0
        wins = 0
        trades = 0
        for m in sm:
            nc = m["nc"]
            noc = nc / 100.0
            ef = fee(nc)
            trades += 1

            pt_trig = False
            if pt is not None:
                cross = ptx.get(m["ticker"], {}).get(pt)
                if cross is not None:
                    enoc = (100 - cross) / 100.0
                    xf = fee(100 - cross)
                    pnl = (enoc - noc) - ef - xf
                    pt_trig = True
                    if pnl > 0:
                        wins += 1

            if not pt_trig:
                if m["res"] == "no":
                    pnl = (1.0 - noc) - ef
                    wins += 1
                else:
                    pnl = -(noc + ef)

            total_pnl += pnl

        roi = total_pnl / (trades * 0.30) * 100 if trades > 0 else 0  # Approximate ROI
        row[pt] = {"pnl": total_pnl, "roi": roi, "trades": trades, "wins": wins}

    # Find best PT by P&L
    best_pt = max(row.keys(), key=lambda k: row[k]["pnl"])
    hold_pnl = row[None]["pnl"]
    best_pnl = row[best_pt]["pnl"]
    rescue = "YES!" if hold_pnl < 0 and best_pnl > 0 else ("BETTER" if best_pnl > hold_pnl * 1.2 else "")

    vals = []
    for pt in [None, 50, 100, 150, 200, 300]:
        p = row[pt]["pnl"]
        marker = "*" if p > 0 else " "
        vals.append(f"${p:>7.1f}{marker}")

    best_str = f"{best_pt}%" if best_pt else "HOLD"
    print(f"{sport:<12} {row[None]['trades']:>5} {vals[0]:>10} {vals[1]:>10} {vals[2]:>10} {vals[3]:>10} {vals[4]:>10} {vals[5]:>10} {best_str:>6} {rescue:>8}")

    results[sport] = row

# Summary of rescued markets
print("\n" + "=" * 95)
print("RESCUE SUMMARY — Markets that flip from LOSING to PROFITABLE with profit-taking")
print("=" * 95)
for sport, row in sorted(results.items()):
    hold = row[None]["pnl"]
    if hold >= 0:
        continue
    best_pt = max(row.keys(), key=lambda k: row[k]["pnl"])
    best_pnl = row[best_pt]["pnl"]
    if best_pnl > 0:
        best_str = f"{best_pt}%" if best_pt else "HOLD"
        print(f"  {sport}: HOLD=${hold:.1f} -> {best_str}=${best_pnl:.1f}  (RESCUED)")
    else:
        print(f"  {sport}: HOLD=${hold:.1f}, best=${best_pnl:.1f} at {best_pt}%  (still losing)")

print(f"\n  Total time: {time.time()-t0:.1f}s")
