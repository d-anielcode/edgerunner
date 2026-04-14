"""
NEW MARKETS BACKTEST — Test every untapped market in TrevorJS dataset.

For each new market:
1. Compute NO win rate by price bucket (does FLB exist?)
2. Find optimal PT threshold
3. Compute Sharpe ratio (risk-adjusted edge)
4. Simulate flat $1 bet P&L

Then re-run the FULL portfolio sim with the best new markets added.
"""
import duckdb
import math
import time

t0 = time.time()
con = duckdb.connect()
mp = 'data/trevorjs/markets-*.parquet'
tp = 'data/trevorjs/trades-*.parquet'

# ALL new markets to test (not currently traded)
NEW_PREFIXES = {
    # College sports
    "KXNCAAMBTOTAL": "NCAAMB_TOTAL",
    "KXNCAAMBSPREAD": "NCAAMB_SPREAD",
    "KXNCAAFSPREAD": "NCAAF_SPREAD",
    "KXNCAAFTOTAL": "NCAAF_TOTAL",
    "KXNCAAFGAME": "NCAAF_GW",
    # NHL props
    "KXNHLPTS": "NHL_PTS",
    "KXNHLAST": "NHL_AST",
    "KXNHLGOAL": "NHL_GOAL",
    "KXNHLTOTAL": "NHL_TOTAL",
    # NBA/NFL totals
    "KXNBATOTAL": "NBA_TOTAL",
    "KXNFLTOTAL": "NFL_TOTAL",
    "KXNFLFIRSTTD": "NFL_1ST_TD",
    # European soccer
    "KXSERIEAGAME": "SERIE_A",
    "KXBUNDESLIGAGAME": "BUNDESLIGA",
    "KXMLSGAME": "MLS",
    "KXSAUDIPLGAME": "SAUDI_PL",
    "KXEFLCHAMPIONSHIPGAME": "EFL_CHAMP",
    "KXLIGAPORTUGALGAME": "PORTUGAL",
    "KXEREDIVISIEGAME": "EREDIVISIE",
    "KXBRASILEIROGAME": "BRASILEIRO",
    "KXSUPERLIGGAME": "SUPER_LIG",
    "KXSCOTTISHPREMGAME": "SCOTTISH",
    # European basketball
    "KXEUROLEAGUEGAME": "EUROLEAGUE",
    "KXNBLGAME": "NBL_AUS",
    "KXFIBACHAMPLEAGUEGAME": "FIBA_CL",
    "KXARGLNBGAME": "ARG_BBALL",
    "KXKBLGAME": "KBL_KOREA",
    # International hockey
    "KXKHLGAME": "KHL",
    "KXSHLGAME": "SHL",
    "KXAHLGAME": "AHL",
    "KXNCAAHOCKEYGAME": "NCAA_HOCKEY",
    # Soccer cup competitions
    "KXFACUPGAME": "FA_CUP",
    "KXUELGAME": "EUROPA_LG",
    "KXUECLGAME": "CONF_LG",
    "KXCLUBWCGAME": "CLUB_WC",
    "KXCOPADELREYGAME": "COPA_REY",
    "KXFIFAGAME": "FIFA",
    # Soccer spreads/totals
    "KXEPLSPREAD": "EPL_SPREAD",
    "KXSERIEASPREAD": "SERIE_A_SP",
    "KXLALIGASPREAD": "LALIGA_SP",
    "KXBUNDESLIGASPREAD": "BUND_SP",
    "KXUCLSPREAD": "UCL_SPREAD",
    "KXEPLTOTAL": "EPL_TOTAL",
    "KXSERIEATOTAL": "SERIEA_TOT",
    "KXLALIGATOTAL": "LALIGA_TOT",
    "KXBUNDESLIGATOTAL": "BUND_TOT",
    "KXUCLTOTAL": "UCL_TOTAL",
    # Goal scorer markets
    "KXUCLGOAL": "UCL_GOAL",
    "KXEPLGOAL": "EPL_GOAL",
    "KXUCLFIRSTGOAL": "UCL_1STGOAL",
    "KXEPLFIRSTGOAL": "EPL_1STGOAL",
    # Esports
    "KXCS": "CS2",
    "KXDOTA": "DOTA2",
    "KXVALORANTGAME": "VALORANT",
    "KXCSGOGAME": "CSGO",
    "KXCODGAME": "COD",
    "KXLOLGAME": "LOL_GAME",
    # Other
    "KXDARTSMATCH": "DARTS",
    "KXMLBSPREAD": "MLB_SPREAD",
    "KXNFLPASSTDS": "NFL_PASS_TD",
    "KXNFLRECYDS": "NFL_REC_YDS",
    "KXNBASTL": "NBA_STL",
    "KXLIGAMXGAME": "LIGA_MX",
    "KXJLEAGUEGAME": "J_LEAGUE",
    "KXALEGAGUEGAME": "A_LEAGUE",
    "KXAFCONGAME": "AFC_CONF",
}

# Build SQL
case_parts = [f"WHEN event_ticker LIKE '{p}%' THEN '{s}'" for p, s in NEW_PREFIXES.items()]
case_stmt = ' '.join(case_parts)
like_clauses = ' OR '.join(f"event_ticker LIKE '{p}%'" for p in NEW_PREFIXES.keys())

print("Loading new market data...")
new_df = con.sql(f"""
    WITH gm AS (
        SELECT ticker, result, event_ticker, close_time,
               CASE {case_stmt} END as sport
        FROM '{mp}'
        WHERE ({like_clauses}) AND status='finalized' AND result IN ('yes','no')
    ),
    pregame AS (
        SELECT t.ticker, t.yes_price as yp,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time DESC) as rn
        FROM '{tp}' t
        JOIN gm ON t.ticker = gm.ticker
        WHERE t.created_time < gm.close_time - INTERVAL 2 HOURS
    ),
    max_no AS (
        SELECT t.ticker, MAX(100 - t.yes_price) as max_no_cents
        FROM '{tp}' t
        WHERE t.ticker IN (SELECT ticker FROM gm)
        GROUP BY t.ticker
    )
    SELECT gm.ticker, gm.result, gm.sport, gm.close_time,
           pg.yp, mn.max_no_cents
    FROM gm
    JOIN pregame pg ON gm.ticker = pg.ticker AND pg.rn = 1
    JOIN max_no mn ON gm.ticker = mn.ticker
    WHERE gm.sport IS NOT NULL AND pg.yp BETWEEN 55 AND 95
    ORDER BY gm.close_time
""").fetchdf()

import pandas as pd
for col in ['yp', 'max_no_cents']:
    new_df[col] = pd.to_numeric(new_df[col], errors='coerce')
new_df = new_df.dropna(subset=['yp', 'max_no_cents'])
new_df['yp'] = new_df['yp'].astype(int)
new_df['max_no_cents'] = new_df['max_no_cents'].astype(int)
new_df['entry_no'] = 100 - new_df['yp']

print(f"  {len(new_df)} new market entries loaded ({time.time()-t0:.1f}s)")
print(f"  Sports found: {sorted(new_df['sport'].unique())}")
print()


def kalshi_fee(yp_cents):
    p = yp_cents / 100.0
    return math.ceil(0.07 * p * (1 - p) * 100) / 100.0


def analyze_sport(sport_df, sport_name):
    """Full analysis of a sport: NO win rate, optimal PT, Sharpe."""
    n = len(sport_df)
    if n < 20:
        return None

    # NO win rate by price bucket
    buckets = {}
    for lo, hi in [(55, 64), (65, 74), (75, 84), (85, 95)]:
        bdf = sport_df[(sport_df['yp'] >= lo) & (sport_df['yp'] <= hi)]
        if len(bdf) >= 5:
            no_wins = len(bdf[bdf['result'] == 'no'])
            wr = no_wins / len(bdf) * 100
            implied = 100 - (lo + hi) / 2
            edge = wr - implied
            buckets[f"{lo}-{hi}c"] = {"n": len(bdf), "wr": round(wr, 1), "edge": round(edge, 1)}

    # Optimal PT (grid search)
    pt_levels = [None, 50, 100, 150, 200, 300, 500]
    best_pnl = -999
    best_pt = None
    pt_results = {}

    for pt in pt_levels:
        total_pnl = 0
        wins = 0
        for _, row in sport_df.iterrows():
            entry_no = row['entry_no'] / 100.0
            max_no = row['max_no_cents'] / 100.0
            fee = kalshi_fee(row['yp'])
            cost = entry_no + fee

            if pt is not None:
                pt_price = entry_no * (1 + pt / 100.0)
                if max_no >= pt_price:
                    exit_fee = kalshi_fee(int((1 - pt_price) * 100))
                    revenue = pt_price - exit_fee
                    total_pnl += revenue - cost
                    if revenue > cost: wins += 1
                    continue

            if row['result'] == 'no':
                total_pnl += 1.0 - cost
                wins += 1
            else:
                total_pnl += -cost

        pt_results[pt] = round(total_pnl, 1)
        if total_pnl > best_pnl:
            best_pnl = total_pnl
            best_pt = pt

    # Sharpe ratio (hold to settlement)
    pnls = []
    no_wins = 0
    for _, row in sport_df.iterrows():
        entry_no = row['entry_no'] / 100.0
        fee = kalshi_fee(row['yp'])
        if row['result'] == 'no':
            pnls.append(1.0 - entry_no - fee)
            no_wins += 1
        else:
            pnls.append(-(entry_no + fee))

    avg_pnl = sum(pnls) / len(pnls) if pnls else 0
    std_pnl = (sum((p - avg_pnl)**2 for p in pnls) / len(pnls)) ** 0.5 if pnls else 1
    sharpe = avg_pnl / std_pnl if std_pnl > 0 else 0
    overall_wr = no_wins / n * 100

    return {
        "sport": sport_name,
        "n": n,
        "no_wr": round(overall_wr, 1),
        "sharpe": round(sharpe, 3),
        "avg_pnl": round(avg_pnl, 4),
        "best_pt": best_pt,
        "best_pnl": round(best_pnl, 1),
        "hold_pnl": pt_results[None],
        "pt_results": pt_results,
        "buckets": buckets,
    }


# ═══════════════════════════════════════════════════════════════
# ANALYZE ALL NEW MARKETS
# ═══════════════════════════════════════════════════════════════
print("=" * 90)
print("  NEW MARKET ANALYSIS (sorted by Sharpe ratio)")
print("=" * 90)
print()

results = []
for sport in sorted(new_df['sport'].unique()):
    sdf = new_df[new_df['sport'] == sport]
    r = analyze_sport(sdf, sport)
    if r:
        results.append(r)

# Sort by Sharpe
results.sort(key=lambda x: -x['sharpe'])

print(f"{'Sport':<15} {'N':>5} {'NO WR':>6} {'Sharpe':>7} {'AvgPnL':>8} {'Best PT':>8} {'Best P&L':>9} {'Hold P&L':>9} {'Verdict':>10}")
print("-" * 90)
for r in results:
    pt_label = "HOLD" if r['best_pt'] is None else f"{r['best_pt']}%"
    verdict = "STRONG" if r['sharpe'] > 0.05 and r['best_pnl'] > 5 else ("WEAK" if r['sharpe'] > 0 else "AVOID")
    print(f"{r['sport']:<15} {r['n']:>5} {r['no_wr']:>5.1f}% {r['sharpe']:>+6.3f} ${r['avg_pnl']:>7.4f} {pt_label:>8} ${r['best_pnl']:>8.1f} ${r['hold_pnl']:>8.1f} {verdict:>10}")

# ═══════════════════════════════════════════════════════════════
# DETAILED BREAKDOWN FOR TOP NEW MARKETS
# ═══════════════════════════════════════════════════════════════
print()
print("=" * 90)
print("  DETAILED BREAKDOWN: TOP NEW MARKETS")
print("=" * 90)
print()

strong_markets = [r for r in results if r['sharpe'] > 0.03 and r['best_pnl'] > 3]
for r in strong_markets[:15]:
    print(f"  {r['sport']} ({r['n']} markets, Sharpe={r['sharpe']:+.3f}):")
    print(f"    Overall NO win rate: {r['no_wr']:.1f}%")
    print(f"    Optimal PT: {'HOLD' if r['best_pt'] is None else str(r['best_pt'])+'%'} (P&L=${r['best_pnl']:.1f})")

    # Price bucket breakdown
    if r['buckets']:
        for bucket, info in sorted(r['buckets'].items()):
            edge_str = f"+{info['edge']:.1f}%" if info['edge'] > 0 else f"{info['edge']:.1f}%"
            print(f"    {bucket}: {info['n']:>4} markets, NO WR={info['wr']:.1f}% (edge {edge_str})")

    # PT comparison
    hold = r['pt_results'].get(None, 0)
    pt100 = r['pt_results'].get(100, 0)
    pt200 = r['pt_results'].get(200, 0)
    print(f"    PT comparison: HOLD=${hold:.0f}  100%=${pt100:.0f}  200%=${pt200:.0f}")
    print()

# ═══════════════════════════════════════════════════════════════
# SUMMARY: RECOMMENDED NEW MARKETS TO ADD
# ═══════════════════════════════════════════════════════════════
print("=" * 90)
print("  RECOMMENDED NEW MARKETS TO ADD")
print("=" * 90)
print()

add_candidates = [r for r in results if r['sharpe'] > 0.03 and r['best_pnl'] > 5 and r['n'] >= 50]
print(f"{'Sport':<15} {'N':>5} {'Sharpe':>7} {'Best PT':>8} {'Best P&L':>9}")
print("-" * 50)
for r in add_candidates:
    pt_label = "HOLD" if r['best_pt'] is None else f"{r['best_pt']}%"
    print(f"{r['sport']:<15} {r['n']:>5} {r['sharpe']:>+6.3f} {pt_label:>8} ${r['best_pnl']:>8.1f}")

print(f"\n  Total new markets to consider: {len(add_candidates)}")
print(f"  Total additional trades: {sum(r['n'] for r in add_candidates)}")

print(f"\n  Runtime: {time.time()-t0:.1f}s")
