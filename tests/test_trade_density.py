"""
Test: Is more trade volume at smaller sizes better than fewer trades at bigger sizes?
Same total capital deployed, different distribution.
"""
import duckdb
from collections import defaultdict

con = duckdb.connect()
mp = "data/trevorjs/markets-*.parquet"
tp = "data/trevorjs/trades-*.parquet"

# Load all tradeable markets (current 12 + proposed new ones)
sport_patterns = {
    # Current
    "NBA": "KXNBAGAME%", "NHL": "KXNHLGAME%",
    "EPL": "KXEPLGAME%", "UCL": "KXUCLGAME%", "LALIGA": "KXLALIGAGAME%",
    "WNBA": "KXWNBAGAME%", "UFC": "KXUFCFIGHT%",
    "NCAAMB": "KXNCAAMBGAME%", "NCAAWB": "KXNCAAWBGAME%",
    "WTA": "KXWTAMATCH%", "NFLTD": "KXNFLANYTD%",
    "W1": "KXHIGHNY%", "W2": "KXHIGHCHI%", "W3": "KXHIGHMIA%",
    "W4": "KXHIGHLA%", "W5": "KXHIGHSF%", "W6": "KXHIGHHOU%",
    "W7": "KXHIGHDEN%", "W8": "KXHIGHDC%", "W9": "KXHIGHDAL%",
    # NEW proposed
    "NHLSPREAD": "KXNHLSPREAD%", "NBASPREAD": "KXNBASPREAD%",
    "NBA2D": "KXNBA2D%", "NHLFG": "KXNHLFIRSTGOAL%",
    "NFLSPREAD": "KXNFLSPREAD%",
    "WA": "KXHIGHAUS%", "WP": "KXHIGHPHIL%",
}

sport_map = {}
for k in sport_patterns:
    if k.startswith("W") and len(k) <= 2: sport_map[k] = "WEATHER"
    elif k == "WA": sport_map[k] = "WEATHER"
    elif k == "WP": sport_map[k] = "WEATHER"
    else: sport_map[k] = k

case_stmts = " ".join(f"WHEN event_ticker LIKE '{p}' THEN '{k}'" for k, p in sport_patterns.items())
like_clauses = " OR ".join(f"event_ticker LIKE '{p}'" for p in sport_patterns.values())

EDGE_TABLES = {
    "EPL": {(71, 85): 0.485}, "UCL": {(66, 70): 0.400, (76, 85): 0.641},
    "LALIGA": {(81, 90): 0.588}, "WNBA": {(61, 65): 0.559, (71, 75): 0.596, (81, 90): 0.735},
    "UFC": {(76, 85): 0.622}, "NCAAMB": {(61, 70): 0.579, (71, 80): 0.656, (82, 90): 0.770},
    "NCAAWB": {(61, 70): 0.600, (71, 80): 0.680, (81, 90): 0.780},
    "WTA": {(61, 75): 0.650, (76, 85): 0.680},
    "WEATHER": {(55, 65): 0.404, (66, 75): 0.417, (76, 85): 0.417, (86, 95): 0.419},
    "NFLTD": {(55, 65): 0.492, (66, 75): 0.452, (76, 85): 0.545, (86, 95): 0.286},
    # New markets — use conservative estimates
    "NHLSPREAD": {(55, 65): 0.500, (66, 75): 0.450, (76, 90): 0.400},
    "NBASPREAD": {(55, 65): 0.480, (66, 75): 0.440, (76, 90): 0.380},
    "NBA2D": {(55, 70): 0.550, (71, 90): 0.500},
    "NHLFG": {(55, 70): 0.550, (71, 90): 0.450},
    "NFLSPREAD": {(55, 65): 0.480, (66, 75): 0.440, (76, 90): 0.380},
}

SP = {
    "EPL": (0.25, 0.10, 0.10), "UCL": (0.12, 0.06, 0.08),
    "LALIGA": (0.08, 0.04, 0.15), "WNBA": (0.12, 0.06, 0.05),
    "UFC": (0.12, 0.06, 0.08), "NCAAMB": (0.10, 0.05, 0.08),
    "NCAAWB": (0.12, 0.06, 0.08), "WTA": (0.08, 0.04, 0.10),
    "WEATHER": (0.25, 0.10, 0.10), "NFLTD": (0.20, 0.10, 0.05),
    "NHLSPREAD": (0.15, 0.08, 0.05), "NBASPREAD": (0.12, 0.06, 0.05),
    "NBA2D": (0.10, 0.05, 0.08), "NHLFG": (0.15, 0.08, 0.05),
    "NFLSPREAD": (0.12, 0.06, 0.05),
}

print("Loading ALL markets (current + proposed)...")
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
print(f"Total markets: {len(all_trades)}")

# Current markets only
current_sports = {"NBA", "NHL", "EPL", "UCL", "LALIGA", "WNBA", "UFC",
                   "NCAAMB", "NCAAWB", "WTA", "NFLTD", "WEATHER"}

def pp(sp, yp):
    if sp == "NBA": return max(0.20, 0.50 - (yp - 60) * 0.004)
    if sp == "NHL": return max(0.30, 0.55 - (yp - 60) * 0.003)
    return None


def run(data, kelly_scale, max_bet, max_daily, label):
    br = 100.0; peak = 100.0; max_dd = 0.0; min_br = 100.0
    tt = tw = dc = 0; ld = None; traded = set()
    monthly = {}

    for _, row in data.iterrows():
        d = str(row["trade_date"]); t = row["ticker"]
        yp = int(row["yes_price"]); np_ = int(row["no_price"])
        sp = row["sport"]; res = row["result"]; vol = float(row["volume"])
        m_num = int(d[5:7]); d_num = int(d[8:10])

        if d != ld:
            if ld: monthly[ld[:7]] = br
            dc = 0; ld = d
        if dc >= max_daily or br < 2 or t in traded: continue
        if sp == "NHL" and ((m_num > 4 or (m_num == 4 and d_num > 16)) and m_num < 10): continue
        if sp == "NBA" and 500_000 <= vol <= 2_000_000: continue

        min_p = 55 if sp in ("WEATHER", "NFLTD") else 61
        max_p = 95 if sp in ("WEATHER", "NFLTD") else 90
        if yp < min_p or yp > max_p: continue

        p2 = pp(sp, yp)
        if p2 is not None:
            ay = p2; km = 0.375 * kelly_scale; mp_ = 0.18 * kelly_scale
            me = 0.05 if sp == "NHL" else 0.08
        else:
            params = SP.get(sp)
            if not params: continue
            et = EDGE_TABLES.get(sp, {})
            ay = None
            for (lo, hi), rate in et.items():
                if lo <= yp <= hi: ay = rate; break
            if ay is None: continue
            km = params[0] * 1.5 * kelly_scale; mp_ = params[1] * 1.5 * kelly_scale; me = params[2]

        edge = (yp / 100.0) - ay
        if edge < me: continue
        nc = np_ / 100.0
        fee = 0.07 * nc * (1 - nc)
        if edge - (fee + 0.005) / nc < 0.03: continue

        b = (yp / 100) / nc if nc > 0 else 0
        kr = (b * (1 - ay) - ay) / b if b > 0 else 0
        ka = max(0, min(kr * km, mp_))

        # April reduction
        if m_num == 4: ka *= 0.50
        if ka <= 0: continue

        bet = min(br * ka, max_bet)
        contracts = max(1, int(bet / nc))
        cost = contracts * nc
        if cost > br: continue

        tf = 0.07 * nc * (1 - nc) * contracts
        if res == "no": br += contracts * (1.0 - nc) - tf; tw += 1
        else: br -= cost + tf
        tt += 1; dc += 1; traded.add(t)
        if br > peak: peak = br
        if br < min_br: min_br = br
        dd = (peak - br) / peak if peak > 0 else 0
        if dd > max_dd: max_dd = dd

    if ld: monthly[ld[:7]] = br
    wr = tw / max(tt, 1) * 100
    return {"label": label, "final": br, "trades": tt, "wr": wr,
            "max_dd": max_dd * 100, "min_br": min_br, "monthly": monthly}


# ================================================================
# TEST 1: Current markets — vary Kelly and daily cap
# ================================================================
current_data = all_trades[all_trades["sport"].isin(current_sports)]

print("\n" + "=" * 95)
print("TEST 1: CURRENT 12 MARKETS — Vary bet size and daily cap")
print("=" * 95)
print(f"\n{'Config':55s} | {'Final':>10s} | {'Trades':>6s} | {'WR':>5s} | {'MaxDD':>6s}")
print("-" * 90)

configs = [
    (1.0, 200, 8, "Current Kelly, $200 cap, 8/day"),
    (1.0, 200, 15, "Current Kelly, $200 cap, 15/day"),
    (1.0, 200, 25, "Current Kelly, $200 cap, 25/day"),
    (0.67, 200, 15, "67% Kelly (smaller bets), $200 cap, 15/day"),
    (0.50, 200, 15, "50% Kelly (half bets), $200 cap, 15/day"),
    (0.50, 200, 25, "50% Kelly, $200 cap, 25/day"),
    (0.33, 200, 25, "33% Kelly (tiny bets), $200 cap, 25/day"),
    (0.50, 100, 15, "50% Kelly, $100 cap, 15/day"),
    (0.50, 100, 25, "50% Kelly, $100 cap, 25/day"),
]

for ks, mb, md, label in configs:
    r = run(current_data, ks, mb, md, label)
    print(f"{label:55s} | ${r['final']:>8,.2f} | {r['trades']:>6d} | {r['wr']:>4.1f}% | {r['max_dd']:>5.1f}%")

# ================================================================
# TEST 2: Current + New markets — does adding more help?
# ================================================================
print(f"\n{'=' * 95}")
print("TEST 2: CURRENT 12 vs EXPANDED 19 MARKETS")
print("Same Kelly, same cap — does more variety help?")
print("=" * 95)

expanded_data = all_trades.copy()

print(f"\n{'Config':55s} | {'Final':>10s} | {'Trades':>6s} | {'WR':>5s} | {'MaxDD':>6s}")
print("-" * 90)

for label, data, ks, mb, md in [
    ("Current 12, normal Kelly, 15/day", current_data, 1.0, 200, 15),
    ("Expanded 19, normal Kelly, 15/day", expanded_data, 1.0, 200, 15),
    ("Expanded 19, normal Kelly, 25/day", expanded_data, 1.0, 200, 25),
    ("Current 12, 50% Kelly, 25/day", current_data, 0.50, 200, 25),
    ("Expanded 19, 50% Kelly, 25/day", expanded_data, 0.50, 200, 25),
    ("Expanded 19, 33% Kelly, 30/day", expanded_data, 0.33, 200, 30),
]:
    r = run(data, ks, mb, md, label)
    print(f"{label:55s} | ${r['final']:>8,.2f} | {r['trades']:>6d} | {r['wr']:>4.1f}% | {r['max_dd']:>5.1f}%")

# ================================================================
# TEST 3: Same total risk, different distribution
# ================================================================
print(f"\n{'=' * 95}")
print("TEST 3: SAME TOTAL DAILY RISK (~$50), DIFFERENT DISTRIBUTION")
print("What's better: 5 trades x $10 or 10 trades x $5?")
print("=" * 95)

# At $117 bankroll, ~$50 daily deployment
# 5 trades x 8.5% = 42.5% deployed
# 10 trades x 4.25% = 42.5% deployed (same total!)
print(f"\n{'Config':55s} | {'Final':>10s} | {'Trades':>6s} | {'WR':>5s} | {'MaxDD':>6s}")
print("-" * 90)

for label, ks, mb, md in [
    ("Concentrated: big bets, few trades (5/day)", 1.0, 200, 5),
    ("Moderate: medium bets, moderate trades (10/day)", 0.67, 200, 10),
    ("Spread: small bets, many trades (15/day)", 0.50, 200, 15),
    ("Very spread: tiny bets, max trades (25/day)", 0.33, 200, 25),
    ("Ultra spread: micro bets, all trades (50/day)", 0.20, 200, 50),
]:
    r = run(current_data, ks, mb, md, label)
    daily_avg = r["trades"] / max(len(r["monthly"]) * 30, 1)
    print(f"{label:55s} | ${r['final']:>8,.2f} | {r['trades']:>6d} | {r['wr']:>4.1f}% | {r['max_dd']:>5.1f}%")

print(f"\n{'=' * 95}")
