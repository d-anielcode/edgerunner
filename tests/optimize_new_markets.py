"""
Optimize parameters for WTA, MLB, MLS, NBA2D, and La Liga.
Find the best (profit_take, entry_range, kelly_mult) per sport using trajectory data.
"""
import math, time, duckdb
from collections import defaultdict

t0 = time.time()
con = duckdb.connect()
mp = "data/trevorjs/markets-*.parquet"
tp = "data/trevorjs/trades-*.parquet"

TARGET_SPORTS = {
    "KXWTAMATCH": "WTA",
    "KXMLBGAME": "MLB",
    "KXMLSGAME": "MLS",
    "KXNBA2D": "NBA2D",
    "KXLALIGAGAME": "LALIGA",
}

case_parts = [f"WHEN event_ticker LIKE '{p}%' THEN '{s}'" for p, s in TARGET_SPORTS.items()]
case_stmt = " ".join(case_parts)
like_clauses = " OR ".join(f"event_ticker LIKE '{p}%'" for p in TARGET_SPORTS.keys())

print("Loading target markets...")
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
        SELECT t.ticker, t.yes_price, CAST(t.created_time AS DATE) as td
        FROM '{tp}' t JOIN ts ON t.ticker = ts.ticker AND t.created_time = ts.ft
    )
    SELECT ft.ticker, ft.yes_price, ft.td, gm.result, gm.sport, gm.volume
    FROM ft JOIN gm ON ft.ticker = gm.ticker JOIN ts ON ft.ticker = ts.ticker
    WHERE gm.sport IS NOT NULL ORDER BY ft.td
""").fetchdf()

print(f"  {len(mdf)} markets loaded")
for s, n in mdf.groupby("sport").size().items():
    print(f"    {s}: {n}")

# Build markets list
markets = []
for _, row in mdf.iterrows():
    yp = int(row["yes_price"])
    if yp < 55 or yp > 95:
        continue
    markets.append({
        "tk": row["ticker"], "yp": yp, "sp": row["sport"],
        "d": str(row["td"]), "res": row["result"],
        "nc": 100 - yp, "noc": (100 - yp) / 100.0,
    })

print(f"  {len(markets)} in 55-95c range")

# Load trajectories
tickers = list(set(m["tk"] for m in markets))
con.execute("CREATE TEMP TABLE tks (ticker VARCHAR)")
for t in tickers:
    con.execute("INSERT INTO tks VALUES (?)", [t])

print("\nLoading trajectories...")
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

min_yes_map = {row["ticker"]: int(row["min_yes"]) for _, row in min_df.iterrows()}

ptx = {}
for pt_pct in [50, 100, 150, 200, 300]:
    mult = 1 + pt_pct / 100.0
    cands = [(m["tk"], int(100 - mult * m["nc"])) for m in markets if int(100 - mult * m["nc"]) > 0 and min_yes_map.get(m["tk"], 999) <= int(100 - mult * m["nc"])]
    if not cands:
        continue
    con.execute(f"DROP TABLE IF EXISTS ptc{pt_pct}")
    con.execute(f"CREATE TEMP TABLE ptc{pt_pct} (ticker VARCHAR, ym INTEGER)")
    for tk, ym in cands:
        con.execute(f"INSERT INTO ptc{pt_pct} VALUES (?, ?)", [tk, ym])
    cdf = con.sql(f"""
        WITH ft AS (SELECT ticker, MIN(created_time) as ft FROM '{tp}'
            WHERE ticker IN (SELECT ticker FROM ptc{pt_pct}) AND created_time >= '2025-01-01' GROUP BY ticker),
        cr AS (SELECT t.ticker, t.yes_price,
            ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
            FROM '{tp}' t JOIN ft ON t.ticker = ft.ticker JOIN ptc{pt_pct} c ON t.ticker = c.ticker
            WHERE t.created_time > ft.ft AND t.created_time >= '2025-01-01' AND t.yes_price <= c.ym)
        SELECT ticker, yes_price FROM cr WHERE rn = 1
    """).fetchdf()
    for _, r in cdf.iterrows():
        ptx.setdefault(r["ticker"], {})[pt_pct] = int(r["yes_price"])

print(f"  Done ({time.time()-t0:.1f}s)")


def fee(c):
    p = c / 100.0
    return math.ceil(0.07 * p * (1 - p) * 100) / 100


# Per-sport YES rate analysis first
print("\n" + "=" * 80)
print("STEP 1: YES WIN RATE BY PRICE BUCKET (for edge table calibration)")
print("=" * 80)

for sport in sorted(set(m["sp"] for m in markets)):
    sm = [m for m in markets if m["sp"] == sport]
    print(f"\n  {sport} ({len(sm)} markets):")
    print(f"    {'Bucket':>8} {'N':>5} {'YES%':>6} {'Breakeven':>10} {'Edge':>6} {'NO ROI':>8}")
    print(f"    {'-'*50}")
    for lo in range(55, 95, 5):
        hi = lo + 4
        bm = [m for m in sm if lo <= m["yp"] <= hi]
        if len(bm) < 5:
            continue
        yes_rate = sum(1 for m in bm if m["res"] == "yes") / len(bm)
        avg_yp = sum(m["yp"] for m in bm) / len(bm)
        breakeven = (100 - avg_yp) / 100
        no_rate = 1 - yes_rate
        avg_noc = sum(m["noc"] for m in bm) / len(bm)
        roi = (no_rate * (1 - avg_noc) - yes_rate * avg_noc) / avg_noc if avg_noc > 0 else 0
        edge = avg_yp / 100 - yes_rate
        print(f"    {lo}-{hi}c {len(bm):>5} {yes_rate:>5.1%} {breakeven:>9.1%} {edge:>+5.1%} {roi:>+7.1%}")


# Grid optimization
print("\n" + "=" * 80)
print("STEP 2: OPTIMIZATION GRID (PT x Entry Range)")
print("=" * 80)

PT_LEVELS = [None, 50, 100, 150, 200, 300]
RANGES = [(55, 95), (61, 90), (66, 85), (71, 85), (76, 90)]

for sport in sorted(set(m["sp"] for m in markets)):
    sm = [m for m in markets if m["sp"] == sport]
    if len(sm) < 20:
        print(f"\n  {sport}: Too few markets ({len(sm)}), skipping grid")
        continue

    print(f"\n  {sport} ({len(sm)} markets):")
    print(f"    {'PT':>6} {'Range':>8} {'N':>5} {'P&L':>8} {'WinRate':>8} {'PTtrig':>7} {'Sharpe':>7}")
    print(f"    {'-'*55}")

    best_sharpe = -999
    best_config = None

    for pt in PT_LEVELS:
        for (lo, hi) in RANGES:
            fm = [m for m in sm if lo <= m["yp"] <= hi]
            if len(fm) < 10:
                continue

            pnls = []
            wins = 0
            pt_trigs = 0
            for m in fm:
                nc = m["nc"]; noc = m["noc"]
                ef = fee(nc)
                pt_trig = False
                if pt is not None:
                    cross = ptx.get(m["tk"], {}).get(pt)
                    if cross is not None:
                        enoc = (100 - cross) / 100.0
                        xf = fee(100 - cross)
                        pnl = (enoc - noc) - ef - xf
                        pt_trig = True
                        pt_trigs += 1
                        if pnl > 0:
                            wins += 1
                if not pt_trig:
                    if m["res"] == "no":
                        pnl = (1.0 - noc) - ef
                        wins += 1
                    else:
                        pnl = -(noc + ef)
                pnls.append(pnl)

            total = sum(pnls)
            n = len(pnls)
            wr = wins / n if n > 0 else 0
            ptr = pt_trigs / n if n > 0 else 0
            avg = total / n if n > 0 else 0
            std = (sum((p - avg) ** 2 for p in pnls) / n) ** 0.5 if n > 1 else 1
            sharpe = avg / std if std > 0 else 0

            pt_str = f"{pt}%" if pt else "HOLD"
            marker = " <--" if sharpe > best_sharpe else ""
            if sharpe > best_sharpe:
                best_sharpe = sharpe
                best_config = {"pt": pt, "lo": lo, "hi": hi, "n": n, "pnl": total,
                               "wr": wr, "ptr": ptr, "sharpe": sharpe}

            if total > 0 or pt is None:  # Show all HOLD rows + profitable combos
                print(f"    {pt_str:>6} {lo}-{hi}c {n:>5} ${total:>6.1f} {wr:>7.1%} {ptr:>6.0%} {sharpe:>7.3f}{marker}")

    if best_config:
        bc = best_config
        pt_str = f"{bc['pt']}%" if bc['pt'] else "HOLD"
        print(f"\n    BEST: {pt_str} PT, {bc['lo']}-{bc['hi']}c, N={bc['n']}, "
              f"P&L=${bc['pnl']:.1f}, WR={bc['wr']:.0%}, Sharpe={bc['sharpe']:.3f}")


# Final recommendations
print("\n" + "=" * 80)
print("STEP 3: RECOMMENDED EDGE TABLES + SPORT_PARAMS")
print("=" * 80)
print("""
Based on the YES win rate analysis and optimization grid above,
here are the recommended parameters for each market.
Use the YES rates from Step 1 to build edge tables,
and the best config from Step 2 for PT/range/Kelly.
""")

print(f"  Total time: {time.time()-t0:.1f}s")
