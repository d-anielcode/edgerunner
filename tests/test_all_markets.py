"""Test ALL untested market types for profit-take viability."""
import math, time, duckdb

t0 = time.time()
con = duckdb.connect()
mp = "data/trevorjs/markets-*.parquet"
tp = "data/trevorjs/trades-*.parquet"

# Markets we haven't tested yet that could have binary game-winner style outcomes
UNTESTED = {
    "KXNFLGAME": "NFLGW",          # NFL game winners (596 markets!)
    "KXSERIEAGAME": "SERIEA",       # Serie A (Italian soccer)
    "KXBUNDESLIGAGAME": "BUNDESLIGA",# Bundesliga (German soccer)
    "KXLIGUE": "LIGUE1",           # Ligue 1 (French soccer)
    "KXEFLCHAMPIONSHIPGAME": "EFL", # English Championship
    "KXCS": "CSGO",                # Counter-Strike esports
    "KXDOTA": "DOTA2",             # Dota 2 esports
    "KXLOLMAP": "LOL",             # League of Legends esports
    "KXCBAGAME": "CBA",            # Chinese Basketball
    "KXEPLFIRSTGOAL": "EPLFG",     # EPL first goal scorer
    "KXUCLFIRSTGOAL": "UCLFG",     # UCL first goal scorer
    "KXEPLSPREAD": "EPLSPREAD",    # EPL spreads
    "KXSERIEASPREAD": "SERIEASPREAD",# Serie A spreads
    "KXNFLPASSTDS": "NFLPASS",     # NFL passing TDs
    "KXATPCHALLENGERMATCH": "ATPCH",# ATP Challenger matches
    "KXNFLTEAMTOTAL": "NFLTT",     # NFL team totals
    "KXMLBTOTAL": "MLBTOTAL",      # MLB totals
    "KXEPLGOAL": "EPLGOAL",        # EPL total goals
    "KXUCLGOAL": "UCLGOAL",        # UCL total goals
}

case_parts = [f"WHEN event_ticker LIKE '{p}%' THEN '{s}'" for p, s in UNTESTED.items()]
case_stmt = " ".join(case_parts)
like_clauses = " OR ".join(f"event_ticker LIKE '{p}%'" for p in UNTESTED.keys())

print("Loading ALL untested markets...")
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
        GROUP BY ticker HAVING COUNT(*) >= 15
    ),
    ft AS (
        SELECT t.ticker, t.yes_price, CAST(t.created_time AS DATE) as td
        FROM '{tp}' t JOIN ts ON t.ticker = ts.ticker AND t.created_time = ts.ft
    )
    SELECT ft.ticker, ft.yes_price, ft.td, gm.result, gm.sport
    FROM ft JOIN gm ON ft.ticker = gm.ticker JOIN ts ON ft.ticker = ts.ticker
    WHERE gm.sport IS NOT NULL ORDER BY ft.td
""").fetchdf()

print(f"  {len(mdf)} markets loaded")
for s, n in sorted(mdf.groupby("sport").size().items(), key=lambda x: -x[1]):
    print(f"    {s:<15} {n:>5}")

markets = []
for _, row in mdf.iterrows():
    yp = int(row["yes_price"])
    if yp < 55 or yp > 95:
        continue
    markets.append({"tk": row["ticker"], "yp": yp, "sp": row["sport"],
        "res": row["result"], "nc": 100 - yp, "noc": (100 - yp) / 100.0})

print(f"\n  {len(markets)} in 55-95c range")

tickers = list(set(m["tk"] for m in markets))
if not tickers:
    print("No markets found!")
    exit()

con.execute("CREATE TEMP TABLE tks (ticker VARCHAR)")
for t in tickers: con.execute("INSERT INTO tks VALUES (?)", [t])

print("\nLoading trajectories...")
min_df = con.sql(f"""
    WITH ft AS (SELECT ticker, MIN(created_time) as ft FROM '{tp}'
        WHERE ticker IN (SELECT ticker FROM tks) AND created_time >= '2025-01-01' GROUP BY ticker)
    SELECT t.ticker, MIN(t.yes_price) as min_yes
    FROM '{tp}' t JOIN ft ON t.ticker = ft.ticker
    WHERE t.created_time > ft.ft AND t.created_time >= '2025-01-01'
    GROUP BY t.ticker
""").fetchdf()
min_yes_map = {row["ticker"]: int(row["min_yes"]) for _, row in min_df.iterrows()}

ptx = {}
for pt_pct in [50, 100, 150, 200]:
    mult = 1 + pt_pct / 100.0
    cands = [(m["tk"], int(100 - mult * m["nc"])) for m in markets
             if int(100 - mult * m["nc"]) > 0 and min_yes_map.get(m["tk"], 999) <= int(100 - mult * m["nc"])]
    if not cands: continue
    con.execute(f"DROP TABLE IF EXISTS ptc{pt_pct}")
    con.execute(f"CREATE TEMP TABLE ptc{pt_pct} (ticker VARCHAR, ym INTEGER)")
    for tk, ym in cands: con.execute(f"INSERT INTO ptc{pt_pct} VALUES (?, ?)", [tk, ym])
    cdf = con.sql(f"""
        WITH ft AS (SELECT ticker, MIN(created_time) as ft FROM '{tp}'
            WHERE ticker IN (SELECT ticker FROM ptc{pt_pct}) AND created_time >= '2025-01-01' GROUP BY ticker),
        cr AS (SELECT t.ticker, t.yes_price, ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
            FROM '{tp}' t JOIN ft ON t.ticker = ft.ticker JOIN ptc{pt_pct} c ON t.ticker = c.ticker
            WHERE t.created_time > ft.ft AND t.created_time >= '2025-01-01' AND t.yes_price <= c.ym)
        SELECT ticker, yes_price FROM cr WHERE rn = 1
    """).fetchdf()
    for _, r in cdf.iterrows():
        ptx.setdefault(r["ticker"], {})[pt_pct] = int(r["yes_price"])

def fee(c):
    p = c / 100.0
    return math.ceil(0.07 * p * (1 - p) * 100) / 100

print(f"  Done ({time.time()-t0:.1f}s)")
print()
print("=" * 90)
print("UNTESTED MARKETS — PROFIT-TAKE ANALYSIS")
print("=" * 90)
print(f"\n{'Sport':<15} {'N':>5} {'HOLD':>9} {'50%PT':>9} {'100%PT':>9} {'150%PT':>9} {'200%PT':>9} {'Best':>6} {'Add?':>6}")
print("-" * 80)

for sport in sorted(set(m["sp"] for m in markets)):
    sm = [m for m in markets if m["sp"] == sport]
    if len(sm) < 10: continue
    row = {}
    for pt in [None, 50, 100, 150, 200]:
        pnl = 0.0; wins = 0
        for m in sm:
            nc = m["nc"]; noc = m["noc"]; ef = fee(nc)
            pt_trig = False
            if pt is not None:
                cross = ptx.get(m["tk"], {}).get(pt)
                if cross is not None:
                    enoc = (100 - cross) / 100.0
                    xf = fee(100 - cross)
                    p = (enoc - noc) - ef - xf; pt_trig = True
                    if p > 0: wins += 1
            if not pt_trig:
                if m["res"] == "no": p = (1.0 - noc) - ef; wins += 1
                else: p = -(noc + ef)
            pnl += p
        row[pt] = pnl

    best_pt = max(row.keys(), key=lambda k: row[k])
    best_pnl = row[best_pt]
    hold_pnl = row[None]
    add = "YES" if best_pnl > 5 else ("maybe" if best_pnl > 0 else "no")

    vals = [f"${v:>7.1f}" for v in [row[None], row[50], row[100], row[150], row[200]]]
    pt_str = f"{best_pt}%" if best_pt else "HOLD"
    print(f"{sport:<15} {len(sm):>5} {vals[0]} {vals[1]} {vals[2]} {vals[3]} {vals[4]} {pt_str:>6} {add:>6}")

print(f"\n  Time: {time.time()-t0:.1f}s")
