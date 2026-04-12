"""
Risk-adjusted analysis of new markets.
For each market, compute: ROI, max drawdown, Sharpe, max consecutive losses, win rate.
Using best PT level from previous test.
"""
import math, time, duckdb

t0 = time.time()
con = duckdb.connect()
mp = "data/trevorjs/markets-*.parquet"
tp = "data/trevorjs/trades-*.parquet"

TARGETS = {
    "KXMLBTOTAL": ("MLBTOTAL", 100),
    "KXATPCHALLENGERMATCH": ("ATPCH", 50),
    "KXNFLGAME": ("NFLGW", 100),
    "KXCS": ("CSGO", 200),
    "KXLOLMAP": ("LOL", 100),
    "KXCBAGAME": ("CBA", 100),
    "KXLIGUE": ("LIGUE1", 100),
    "KXNFLTEAMTOTAL": ("NFLTT", 150),
    "KXDOTA": ("DOTA2", 200),
    "KXSERIEAGAME": ("SERIEA", 150),
}

case_parts = [f"WHEN event_ticker LIKE '{p}%' THEN '{s}'" for p, (s, _) in TARGETS.items()]
case_stmt = " ".join(case_parts)
like_clauses = " OR ".join(f"event_ticker LIKE '{p}%'" for p in TARGETS.keys())

print("Loading markets...")
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

markets = []
for _, row in mdf.iterrows():
    yp = int(row["yes_price"])
    if yp < 55 or yp > 95: continue
    markets.append({"tk": row["ticker"], "yp": yp, "sp": row["sport"],
        "d": str(row["td"]), "res": row["result"], "nc": 100 - yp, "noc": (100 - yp) / 100.0})

tickers = list(set(m["tk"] for m in markets))
con.execute("CREATE TEMP TABLE tks (ticker VARCHAR)")
for t in tickers: con.execute("INSERT INTO tks VALUES (?)", [t])

print("Loading trajectories...")
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

print(f"Done ({time.time()-t0:.1f}s)\n")

# Detailed per-sport analysis
print("=" * 95)
print("RISK-ADJUSTED ANALYSIS — New Markets (flat $1/trade, sequential)")
print("=" * 95)
print(f"\n{'Sport':<10} {'PT':>4} {'N':>5} {'WR':>6} {'ROI':>7} {'P&L':>8} {'MaxDD':>7} {'MCL':>4} {'Sharpe':>7} {'Season':>12} {'Verdict'}")
print("-" * 90)

sport_results = {}
for prefix, (sport, best_pt) in sorted(TARGETS.items(), key=lambda x: x[1][0]):
    sm = sorted([m for m in markets if m["sp"] == sport], key=lambda x: x["d"])
    if len(sm) < 10: continue

    pt_mult = 1 + best_pt / 100.0
    pnls = []
    wins = 0
    cum_pnl = 0.0
    peak = 0.0
    max_dd = 0.0
    consec_loss = 0
    max_consec = 0
    total_wagered = 0.0

    for m in sm:
        nc = m["nc"]; noc = m["noc"]; ef = fee(nc)
        total_wagered += noc

        pt_trig = False
        cross = ptx.get(m["tk"], {}).get(best_pt)
        if cross is not None:
            enoc = (100 - cross) / 100.0
            xf = fee(100 - cross)
            pnl = (enoc - noc) - ef - xf
            pt_trig = True
            if pnl > 0: wins += 1; consec_loss = 0
            else: consec_loss += 1
        if not pt_trig:
            if m["res"] == "no":
                pnl = (1.0 - noc) - ef; wins += 1; consec_loss = 0
            else:
                pnl = -(noc + ef); consec_loss += 1

        if consec_loss > max_consec: max_consec = consec_loss
        pnls.append(pnl)
        cum_pnl += pnl
        if cum_pnl > peak: peak = cum_pnl
        dd = peak - cum_pnl
        if dd > max_dd: max_dd = dd

    n = len(pnls)
    wr = wins / n if n > 0 else 0
    roi = cum_pnl / total_wagered * 100 if total_wagered > 0 else 0
    avg = cum_pnl / n if n > 0 else 0
    std = (sum((p - avg) ** 2 for p in pnls) / n) ** 0.5 if n > 1 else 1
    sharpe = avg / std if std > 0 else 0

    # Determine season
    dates = sorted(set(m["d"] for m in sm))
    months = sorted(set(int(d[5:7]) for d in dates))
    if len(months) >= 10:
        season = "Year-round"
    else:
        season = f"{min(months)}-{max(months)}mo"

    # Verdict
    if sharpe >= 0.15 and max_dd < 5 and roi > 5:
        verdict = "STRONG ADD"
    elif sharpe >= 0.10 and max_dd < 8 and roi > 3:
        verdict = "ADD"
    elif sharpe >= 0.05 and roi > 0:
        verdict = "CAUTIOUS"
    else:
        verdict = "SKIP"

    pt_str = f"{best_pt}%"
    print(f"{sport:<10} {pt_str:>4} {n:>5} {wr:>5.0%} {roi:>+6.1f}% ${cum_pnl:>6.1f} ${max_dd:>5.1f} {max_consec:>4} {sharpe:>7.3f} {season:>12} {verdict}")

    sport_results[sport] = {
        "n": n, "wr": wr, "roi": roi, "pnl": cum_pnl, "max_dd": max_dd,
        "mcl": max_consec, "sharpe": sharpe, "season": season, "verdict": verdict,
        "pt": best_pt,
    }

# Ranked summary
print("\n" + "=" * 60)
print("RANKED BY CONSISTENCY (Sharpe / MaxDD ratio)")
print("=" * 60)
ranked = sorted(sport_results.items(), key=lambda x: x[1]["sharpe"] / max(x[1]["max_dd"], 0.1), reverse=True)
print(f"\n{'Rank':>4} {'Sport':<10} {'Sharpe':>7} {'MaxDD':>7} {'ROI':>7} {'MCL':>4} {'Verdict'}")
print("-" * 50)
for i, (sport, r) in enumerate(ranked, 1):
    print(f"{i:>4} {sport:<10} {r['sharpe']:>7.3f} ${r['max_dd']:>5.1f} {r['roi']:>+6.1f}% {r['mcl']:>4} {r['verdict']}")

print(f"\n  Time: {time.time()-t0:.1f}s")
