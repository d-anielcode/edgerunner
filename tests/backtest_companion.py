"""
Final backtest with ALL improvements including companion market signals.
$100 start, recent data (2025+), hold to settlement.
Includes: per-price Kelly 1.5x, $200 cap, away boost, April reduction,
NBA playoff reduction, NFL TD Jan boost, NCAAMB 82-90c, NBA vol filter,
AND companion signals (spread + draw price).
"""
import duckdb
from collections import defaultdict

con = duckdb.connect()
mp = "data/trevorjs/markets-*.parquet"
tp = "data/trevorjs/trades-*.parquet"

EDGE_TABLES = {
    "EPL": {(71, 85): 0.485}, "UCL": {(66, 70): 0.400, (76, 85): 0.641},
    "LALIGA": {(81, 90): 0.588}, "WNBA": {(61, 65): 0.559, (71, 75): 0.596, (81, 90): 0.735},
    "UFC": {(76, 85): 0.622}, "NCAAMB": {(61, 70): 0.579, (71, 80): 0.656, (82, 90): 0.770},
    "NCAAWB": {(61, 70): 0.600, (71, 80): 0.680, (81, 90): 0.780},
    "WTA": {(61, 75): 0.650, (76, 85): 0.680},
    "WEATHER": {(55, 65): 0.404, (66, 75): 0.417, (76, 85): 0.417, (86, 95): 0.419},
    "NFLTD": {(55, 65): 0.492, (66, 75): 0.452, (76, 85): 0.545, (86, 95): 0.286},
}
SPORT_PARAMS = {
    "EPL": {"km": 0.25, "mp": 0.10, "me": 0.10},
    "UCL": {"km": 0.12, "mp": 0.06, "me": 0.08},
    "LALIGA": {"km": 0.08, "mp": 0.04, "me": 0.15},
    "WNBA": {"km": 0.12, "mp": 0.06, "me": 0.05},
    "UFC": {"km": 0.12, "mp": 0.06, "me": 0.08},
    "NCAAMB": {"km": 0.10, "mp": 0.05, "me": 0.08},
    "NCAAWB": {"km": 0.12, "mp": 0.06, "me": 0.08},
    "WTA": {"km": 0.08, "mp": 0.04, "me": 0.10},
    "WEATHER": {"km": 0.25, "mp": 0.10, "me": 0.10},
    "NFLTD": {"km": 0.20, "mp": 0.10, "me": 0.05},
}

sport_patterns = {
    "NBA": "KXNBAGAME%", "NHL": "KXNHLGAME%",
    "EPL": "KXEPLGAME%", "UCL": "KXUCLGAME%", "LALIGA": "KXLALIGAGAME%",
    "WNBA": "KXWNBAGAME%", "UFC": "KXUFCFIGHT%",
    "NCAAMB": "KXNCAAMBGAME%", "NCAAWB": "KXNCAAWBGAME%",
    "WTA": "KXWTAMATCH%",
    "W1": "KXHIGHNY%", "W2": "KXHIGHCHI%", "W3": "KXHIGHMIA%",
    "W4": "KXHIGHLA%", "W5": "KXHIGHSF%", "W6": "KXHIGHHOU%",
    "W7": "KXHIGHDEN%", "W8": "KXHIGHDC%", "W9": "KXHIGHDAL%",
    "NFLTD": "KXNFLANYTD%",
}
sport_map = {k: ("WEATHER" if k.startswith("W") and len(k) <= 2 else k) for k in sport_patterns}
case_stmts = " ".join(f"WHEN event_ticker LIKE '{p}' THEN '{k}'" for k, p in sport_patterns.items())
like_clauses = " OR ".join(f"event_ticker LIKE '{p}'" for p in sport_patterns.values())

print("Loading data...")
all_trades = con.sql(f"""
    WITH gm AS (
        SELECT ticker, result, event_ticker, volume,
               CASE {case_stmts} END as sk
        FROM '{mp}' WHERE ({like_clauses}) AND status='finalized' AND result IN ('yes','no')
    ),
    ft AS (
        SELECT t.ticker, t.yes_price, t.no_price, t.created_time,
               CAST(t.created_time AS DATE) as trade_date,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
        FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM gm) AND t.created_time >= '2025-01-01'
    )
    SELECT ft.*, gm.result, gm.sk, gm.volume
    FROM ft JOIN gm ON ft.ticker = gm.ticker WHERE ft.rn=1 AND gm.sk IS NOT NULL
    ORDER BY ft.created_time
""").fetchdf()
all_trades["sport"] = all_trades["sk"].map(sport_map)

# Also load spread + draw markets for companion signals
print("Loading companion markets (spreads + draws)...")
companions = con.sql(f"""
    WITH sp AS (
        SELECT ticker, event_ticker, yes_bid,
               REGEXP_EXTRACT(event_ticker, '[0-9]{{2}}[A-Z]{{3}}[0-9]{{2}}[A-Z]+') as game_key,
               CASE
                   WHEN event_ticker LIKE 'KXNBASPREAD%' THEN 'NBA_SPREAD'
                   WHEN event_ticker LIKE 'KXNHLSPREAD%' THEN 'NHL_SPREAD'
               END as comp_type,
               REGEXP_EXTRACT(SPLIT_PART(ticker, '-', 3), '^[A-Z]+') as team
        FROM '{mp}'
        WHERE (event_ticker LIKE 'KXNBASPREAD%' OR event_ticker LIKE 'KXNHLSPREAD%')
              AND status='finalized' AND yes_bid > 0
    ),
    draws AS (
        SELECT ticker, event_ticker, yes_bid,
               REGEXP_EXTRACT(event_ticker, '[0-9]{{2}}[A-Z]{{3}}[0-9]{{2}}[A-Z]+') as game_key,
               CASE
                   WHEN event_ticker LIKE 'KXEPLGAME%' THEN 'EPL_DRAW'
                   WHEN event_ticker LIKE 'KXUCLGAME%' THEN 'UCL_DRAW'
               END as comp_type,
               'TIE' as team
        FROM '{mp}'
        WHERE ((event_ticker LIKE 'KXEPLGAME%') OR (event_ticker LIKE 'KXUCLGAME%'))
              AND SPLIT_PART(ticker, '-', 3) = 'TIE'
              AND status='finalized' AND yes_bid > 0
    )
    SELECT * FROM sp WHERE game_key IS NOT NULL
    UNION ALL
    SELECT * FROM draws WHERE game_key IS NOT NULL
""").fetchdf()

# Build companion lookup: game_key+team -> {spread_prices: [...], draw_price: int}
import re
comp_lookup = {}  # game_key -> {spread_min: X, draw_price: Y}
for _, row in companions.iterrows():
    gk = row["game_key"]
    ct = row["comp_type"]
    price = int(row["yes_bid"])
    team = row["team"]

    if gk not in comp_lookup:
        comp_lookup[gk] = {"spread_prices": {}, "draw_price": None}

    if "SPREAD" in ct:
        key = f"{gk}_{team}"
        if key not in comp_lookup[gk]["spread_prices"] or price < comp_lookup[gk]["spread_prices"][key]:
            comp_lookup[gk]["spread_prices"][key] = price
    elif "DRAW" in ct:
        comp_lookup[gk]["draw_price"] = price

print(f"Companion lookup: {len(comp_lookup)} games with spread/draw data")


def pp(sp, yp):
    if sp == "NBA": return max(0.20, 0.50 - (yp - 60) * 0.004)
    if sp == "NHL": return max(0.30, 0.55 - (yp - 60) * 0.003)
    return None


def run_backtest(use_companions=True, label=""):
    bankroll = 100.0
    peak = 100.0
    max_dd = 0.0
    min_br = 100.0
    tt = tw = dc = 0
    ld = None
    traded = set()
    monthly = {}
    mt = defaultdict(lambda: {"t": 0, "w": 0, "pnl": 0.0, "sbr": 0})
    sport_stats = defaultdict(lambda: {"t": 0, "w": 0, "pnl": 0.0, "wag": 0.0})
    modifier_hits = defaultdict(int)

    for _, row in all_trades.iterrows():
        d = str(row["trade_date"]); mo = d[:7]; t = row["ticker"]
        yp = int(row["yes_price"]); np_ = int(row["no_price"])
        sp = row["sport"]; res = row["result"]; vol = float(row["volume"])
        m_num = int(d[5:7]); d_num = int(d[8:10])

        if d != ld:
            if ld: monthly[ld[:7]] = bankroll
            dc = 0; ld = d
        if mt[mo]["sbr"] == 0: mt[mo]["sbr"] = bankroll
        if dc >= 15 or bankroll < 2 or t in traded: continue
        if sp == "NHL" and ((m_num > 4 or (m_num == 4 and d_num > 16)) and m_num < 10): continue
        if sp == "NBA" and 500_000 <= vol <= 2_000_000: continue

        min_p = 55 if sp in ("WEATHER", "NFLTD") else 61
        max_p = 95 if sp in ("WEATHER", "NFLTD", "NBA", "NHL") else 90
        if yp < min_p or yp > max_p: continue

        p2 = pp(sp, yp)
        if p2 is not None:
            ay = p2; km = 0.375; mp_ = 0.18; me = 0.05 if sp == "NHL" else 0.08
        else:
            params = SPORT_PARAMS.get(sp)
            if not params: continue
            et = EDGE_TABLES.get(sp, {})
            ay = None
            for (lo, hi), rate in et.items():
                if lo <= yp <= hi: ay = rate; break
            if ay is None: continue
            km = params["km"] * 1.5; mp_ = params["mp"] * 1.5; me = params["me"]

        edge = (yp / 100.0) - ay
        if edge < me: continue
        nc = np_ / 100.0
        fee = 0.07 * nc * (1 - nc)
        if edge - (fee + 0.005) / nc < 0.03: continue

        b = (yp / 100) / nc if nc > 0 else 0
        kr = (b * (1 - ay) - ay) / b if b > 0 else 0
        ka = max(0, min(kr * km, mp_))
        if ka <= 0: continue

        # === KELLY MODIFIERS ===
        mods = []

        # Away favorite boost
        if sp in ("NBA", "NHL"):
            parts = t.split("-")
            if len(parts) >= 3:
                gp = parts[1]; tp_ = parts[2]
                if len(gp) >= 6 and len(tp_) >= 2:
                    gid = gp[-6:]
                    if tp_ == gid[:3]:
                        ka = min(ka * 1.5, mp_)
                        mods.append("away_fav")

        # April 50% reduction
        if m_num == 4:
            ka *= 0.50
            mods.append("april")

        # NBA early R1 playoff
        if sp == "NBA" and m_num == 4 and 13 <= d_num <= 30:
            ka *= 0.25
            mods.append("nba_playoff")

        # NFL TD January boost
        if sp == "NFLTD" and m_num == 1:
            ka = min(ka * 1.5, mp_)
            mods.append("nfltd_jan")

        # === COMPANION SIGNALS ===
        if use_companions:
            game_key_match = re.search(r"\d{2}[A-Z]{3}\d{2}[A-Z]{3,8}", t.upper())
            if game_key_match:
                gk = game_key_match.group(0)
                comp = comp_lookup.get(gk)
                if comp:
                    team_part = t.split("-")[2] if len(t.split("-")) >= 3 else ""
                    sp_key = f"{gk}_{team_part}"

                    if sp in ("NBA", "NHL"):
                        sp_price = comp["spread_prices"].get(sp_key)
                        if sp_price is not None:
                            if sp_price < 40:
                                ka = min(ka * 1.5, mp_)
                                mods.append(f"spread_close_{sp_price}")
                            elif sp_price > 60:
                                ka *= 0.5
                                mods.append(f"spread_blowout_{sp_price}")

                    if sp in ("EPL", "UCL"):
                        dp = comp["draw_price"]
                        if dp is not None:
                            if dp >= 25:
                                ka = min(ka * 1.5, mp_)
                                mods.append(f"draw_high_{dp}")
                            elif dp < 18:
                                ka *= 0.0
                                mods.append(f"draw_low_{dp}")

        if ka <= 0: continue
        for m in mods: modifier_hits[m] += 1

        bet = min(bankroll * ka, 200)  # $200 cap
        contracts = max(1, int(bet / nc))
        cost = contracts * nc
        if cost > bankroll: continue

        tf = 0.07 * nc * (1 - nc) * contracts
        if res == "no":
            pnl = contracts * (1.0 - nc) - tf; tw += 1; mt[mo]["w"] += 1; sport_stats[sp]["w"] += 1
        else:
            pnl = -(cost + tf)

        bankroll += pnl; tt += 1; dc += 1; traded.add(t)
        mt[mo]["t"] += 1; mt[mo]["pnl"] += pnl
        sport_stats[sp]["t"] += 1; sport_stats[sp]["pnl"] += pnl; sport_stats[sp]["wag"] += cost
        if bankroll > peak: peak = bankroll
        if bankroll < min_br: min_br = bankroll
        dd = (peak - bankroll) / peak if peak > 0 else 0
        if dd > max_dd: max_dd = dd

    if ld: monthly[ld[:7]] = bankroll

    return {
        "label": label, "final": bankroll, "trades": tt, "wins": tw,
        "wr": tw / max(tt, 1) * 100, "max_dd": max_dd * 100, "min_br": min_br,
        "monthly": monthly, "mt": dict(mt), "sports": dict(sport_stats),
        "mods": dict(modifier_hits),
    }


# Run both versions
print("\nRunning backtests...")
r_without = run_backtest(use_companions=False, label="Without companions")
r_with = run_backtest(use_companions=True, label="With companions")

print("=" * 95)
print("BACKTEST COMPARISON: With vs Without Companion Signals")
print("$100 start, 1.5x Kelly, $200 max bet, all 12 sports, Jan 2025 - Jan 2026")
print("=" * 95)

print(f"\n{'':40s} | {'Without':>12s} | {'With':>12s} | {'Diff':>10s}")
print("-" * 80)
print(f"{'Final bankroll':40s} | ${r_without['final']:>10,.2f} | ${r_with['final']:>10,.2f} | ${r_with['final']-r_without['final']:>+9,.2f}")
print(f"{'Total trades':40s} | {r_without['trades']:>12d} | {r_with['trades']:>12d} | {r_with['trades']-r_without['trades']:>+10d}")
print(f"{'Win rate':40s} | {r_without['wr']:>11.1f}% | {r_with['wr']:>11.1f}% | {r_with['wr']-r_without['wr']:>+9.1f}%")
print(f"{'Max drawdown':40s} | {r_without['max_dd']:>11.1f}% | {r_with['max_dd']:>11.1f}% | {r_with['max_dd']-r_without['max_dd']:>+9.1f}%")
print(f"{'Lowest balance':40s} | ${r_without['min_br']:>10.2f} | ${r_with['min_br']:>10.2f} |")

# Monthly comparison
print(f"\nMONTHLY PROGRESSION:")
print(f"  {'Month':>7s} | {'Without':>12s} | {'With':>12s} | {'W P&L':>10s} | {'W Trades':>8s} | {'W WR':>5s}")
print("  " + "-" * 65)

for mo in sorted(set(list(r_without["monthly"].keys()) + list(r_with["monthly"].keys()))):
    br_wo = r_without["monthly"].get(mo, 0)
    br_w = r_with["monthly"].get(mo, 0)
    m_w = r_with["mt"].get(mo, {"t": 0, "w": 0, "pnl": 0})
    mwr = m_w["w"] / max(m_w["t"], 1) * 100
    print(f"  {mo:>7s} | ${br_wo:>10,.2f} | ${br_w:>10,.2f} | ${m_w['pnl']:>+8,.2f} | {m_w['t']:>8d} | {mwr:>4.0f}%")

# Sport breakdown
print(f"\nSPORT BREAKDOWN (with companions):")
print(f"  {'Sport':10s} | {'Trades':>6s} | {'Wins':>5s} | {'WR':>6s} | {'P&L':>12s} | {'ROI':>7s}")
print("  " + "-" * 55)
for sp, s in sorted(r_with["sports"].items(), key=lambda x: -x[1]["pnl"]):
    if s["t"] > 0:
        swr = s["w"] / s["t"] * 100
        roi = s["pnl"] / s["wag"] * 100 if s["wag"] > 0 else 0
        print(f"  {sp:10s} | {s['t']:>6d} | {s['w']:>5d} | {swr:>5.1f}% | ${s['pnl']:>+10,.2f} | {roi:>+6.1f}%")

# Modifier stats
print(f"\nMODIFIER HIT COUNTS:")
for mod, count in sorted(r_with["mods"].items(), key=lambda x: -x[1]):
    print(f"  {mod:30s}: {count:>5d} times")

# Milestones
print(f"\nMILESTONES (with companions):")
for target in [200, 500, 1000, 5000, 10000, 25000, 50000, 100000, 200000]:
    for mo in sorted(r_with["monthly"].keys()):
        if r_with["monthly"][mo] >= target:
            print(f"  ${target:>7,} hit in {mo}")
            break
    else:
        print(f"  ${target:>7,} not reached")

print(f"\n{'=' * 95}")
