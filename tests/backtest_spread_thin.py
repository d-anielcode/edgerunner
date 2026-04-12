"""Final backtest: Spread-thin strategy. 0.33x Kelly, 25/day, 19 markets, $100 start."""
import math
import duckdb
from collections import defaultdict
con = duckdb.connect()
mp = "data/trevorjs/markets-*.parquet"
tp = "data/trevorjs/trades-*.parquet"

sport_patterns = {
    "NBA": "KXNBAGAME%", "NHL": "KXNHLGAME%",
    "EPL": "KXEPLGAME%", "UCL": "KXUCLGAME%", "LALIGA": "KXLALIGAGAME%",
    "WNBA": "KXWNBAGAME%", "UFC": "KXUFCFIGHT%",
    "NCAAMB": "KXNCAAMBGAME%", "NCAAWB": "KXNCAAWBGAME%",
    "WTA": "KXWTAMATCH%", "NFLTD": "KXNFLANYTD%",
    "W1": "KXHIGHNY%", "W2": "KXHIGHCHI%", "W3": "KXHIGHMIA%",
    "W4": "KXHIGHLA%", "W5": "KXHIGHSF%", "W6": "KXHIGHHOU%",
    "W7": "KXHIGHDEN%", "W8": "KXHIGHDC%", "W9": "KXHIGHDAL%",
    "WA": "KXHIGHAUS%", "WP": "KXHIGHPHIL%",
    "NHLSP": "KXNHLSPREAD%", "NHLFG": "KXNHLFIRSTGOAL%",
    "NBASP": "KXNBASPREAD%", "NBA2D": "KXNBA2D%", "NFLSP": "KXNFLSPREAD%",
}
sport_map = {}
for k in sport_patterns:
    if k.startswith("W") and len(k) <= 2: sport_map[k] = "WEATHER"
    elif k in ("WA", "WP"): sport_map[k] = "WEATHER"
    elif k == "NHLSP": sport_map[k] = "NHLSPREAD"
    elif k == "NBASP": sport_map[k] = "NBASPREAD"
    elif k == "NFLSP": sport_map[k] = "NFLSPREAD"
    else: sport_map[k] = k
case_stmts = " ".join(f"WHEN event_ticker LIKE '{p}' THEN '{k}'" for k, p in sport_patterns.items())
like_clauses = " OR ".join(f"event_ticker LIKE '{p}'" for p in sport_patterns.values())

EDGE_TABLES = {
    "EPL": {(71,85): 0.485}, "UCL": {(66,70): 0.400, (76,85): 0.641},
    "LALIGA": {(81,90): 0.588}, "WNBA": {(55,62): 0.380, (71,77): 0.550, (83,87): 0.540},
    "UFC": {(76,85): 0.622}, "NCAAMB": {(66,70): 0.536, (71,80): 0.656, (82,90): 0.770},
    "NCAAWB": {(61,70): 0.600, (71,80): 0.680, (81,85): 0.750},
    "WTA": {(61,75): 0.650, (76,85): 0.680},
    "WEATHER": {(55,65): 0.404, (66,75): 0.417, (76,85): 0.417, (86,95): 0.419},
    "NFLTD": {(55,65): 0.492, (66,75): 0.452, (76,85): 0.545, (86,95): 0.286},
    "NHLSPREAD": {(55,65): 0.500, (66,75): 0.450, (76,90): 0.400},
    "NHLFG": {(55,70): 0.550, (71,90): 0.450},
    "NBASPREAD": {(55,65): 0.480, (66,75): 0.440, (76,90): 0.380},
    "NBA2D": {(55,65): 0.520, (66,79): 0.580},
    "NFLSPREAD": {(55,65): 0.480, (66,75): 0.440, (76,90): 0.380},
}
SP = {
    # Current live params (conservative, no boosts)
    "NFLTD": (0.20, 0.10, 0.05),    # Original, no boost
    "WEATHER": (0.00, 0.00, 0.99),   # DISABLED: categorical range markets, not binary favorites
    "NCAAMB": (0.10, 0.05, 0.08),    # Confirmed
    "NHLSPREAD": (0.15, 0.08, 0.05), # Confirmed
    "NBASPREAD": (0.06, 0.03, 0.12), # Tightened (OOS decayed)
    "NFLSPREAD": (0.06, 0.03, 0.12), # Heavy cut (OOS decayed)
    "NHLFG": (0.00, 0.00, 0.99),     # Disabled (OOS decayed)
    # Not validated — keep conservative
    "EPL": (0.25, 0.10, 0.10), "UCL": (0.12, 0.06, 0.08),
    "LALIGA": (0.08, 0.04, 0.15), "WNBA": (0.15, 0.08, 0.08),
    "UFC": (0.06, 0.03, 0.12), "NCAAWB": (0.12, 0.06, 0.08),
    "WTA": (0.08, 0.04, 0.10), "NBA2D": (0.10, 0.05, 0.10),
}

all_t = con.sql(f"""
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
all_t["sport"] = all_t["sk"].map(sport_map)

def pp(sp, yp):
    if sp == "NBA": return max(0.20, 0.50 - (yp - 60) * 0.004)
    if sp == "NHL": return max(0.30, 0.55 - (yp - 60) * 0.003)
    return None

br = 100.0; peak = 100.0; max_dd = 0.0; min_br = 100.0; tt = tw = dc = 0; ld = None
traded = set(); monthly = {}
mt = defaultdict(lambda: {"t": 0, "w": 0, "pnl": 0.0, "sbr": 0})
ss = defaultdict(lambda: {"t": 0, "w": 0, "pnl": 0.0, "wag": 0.0})

for _, row in all_t.iterrows():
    d = str(row["trade_date"]); mo = d[:7]; t = row["ticker"]
    yp = int(row["yes_price"]); np_ = int(row["no_price"])
    sp = row["sport"]; res = row["result"]; vol = float(row["volume"])
    m_num = int(d[5:7]); d_num = int(d[8:10])
    if d != ld:
        if ld: monthly[ld[:7]] = br
        dc = 0; ld = d
    if mt[mo]["sbr"] == 0: mt[mo]["sbr"] = br
    if dc >= 25 or br < 2 or t in traded: continue
    if sp == "NHL" and ((m_num > 4 or (m_num == 4 and d_num > 16)) and m_num < 10): continue
    if sp == "NBA" and 500_000 <= vol <= 2_000_000: continue
    min_p = 55 if sp in ("WEATHER", "NFLTD") else 61
    max_p = 95 if sp in ("WEATHER", "NFLTD") else 90
    if yp < min_p or yp > max_p: continue
    p2 = pp(sp, yp)
    if p2 is not None:
        ay = p2
        if sp == "NHL":
            km = 0.15 * 0.33; mp_ = 0.08 * 0.33; me = 0.08  # OOS mixed, halved from original
        else:
            km = 0.04 * 0.33; mp_ = 0.03 * 0.33; me = 0.15  # NBA severe decay, nearly disabled
    else:
        params = SP.get(sp)
        if not params: continue
        et = EDGE_TABLES.get(sp, {})
        ay = None
        for (lo, hi), rate in et.items():
            if lo <= yp <= hi: ay = rate; break
        if ay is None: continue
        km = params[0] * 0.33; mp_ = params[1] * 0.33; me = params[2]
    edge = (yp / 100.0) - ay
    if edge < me: continue
    nc = np_ / 100.0; fee = math.ceil(0.07 * nc * (1 - nc) * 100) / 100  # Taker fee w/ ceil
    if edge - (fee + 0.005) / nc < 0.03: continue
    b = (yp / 100) / nc if nc > 0 else 0
    kr = (b * (1 - ay) - ay) / b if b > 0 else 0
    ka = max(0, min(kr * km, mp_))
    if m_num == 4: ka *= 0.50
    if sp == "NBA" and m_num == 4 and 13 <= d_num <= 30: ka *= 0.25
    if sp in ("NBA", "NHL"):
        parts = t.split("-")
        if len(parts) >= 3 and len(parts[1]) >= 6 and len(parts[2]) >= 2:
            if parts[2] == parts[1][-6:][:3]: ka = min(ka * 1.5, mp_)
    if ka <= 0: continue
    bet = min(br * ka, 200)
    contracts = max(1, int(bet / nc)); cost = contracts * nc
    if cost > br: continue
    tf = math.ceil(0.07 * nc * (1 - nc) * 100) / 100 * contracts  # Taker fee w/ ceil
    if res == "no":
        pnl = contracts * (1.0 - nc) - tf; tw += 1
        mt[mo]["w"] += 1; ss[sp]["w"] += 1
    else:
        pnl = -(cost + tf)
    br += pnl; tt += 1; dc += 1; traded.add(t)
    mt[mo]["t"] += 1; mt[mo]["pnl"] += pnl
    ss[sp]["t"] += 1; ss[sp]["pnl"] += pnl; ss[sp]["wag"] += cost
    if br > peak: peak = br
    if br < min_br: min_br = br
    dd = (peak - br) / peak if peak > 0 else 0
    if dd > max_dd: max_dd = dd
if ld: monthly[ld[:7]] = br

wr = tw / max(tt, 1) * 100
print("=" * 90)
print("SPREAD-THIN BACKTEST: 0.33x Kelly, 25/day, 19 markets, $200 cap")
print(f"Period: {min(monthly.keys())} to {max(monthly.keys())}")
print("=" * 90)
print(f"""
  Starting:    $100.00
  Final:       ${br:>12,.2f}
  Trades:      {tt:>12,}
  Win rate:    {wr:>11.1f}%
  Max DD:      {max_dd*100:>11.1f}%
  Lowest:      ${min_br:>11.2f}
""")
print("MONTHLY:")
for mo in sorted(monthly.keys()):
    m = mt[mo]; ret = m["pnl"] / max(m["sbr"], 1) * 100
    mwr = m["w"] / max(m["t"], 1) * 100
    print(f"  {mo} | ${monthly[mo]:>10,.2f} | ${m['pnl']:>+8,.2f} | {ret:>+6.1f}% | {m['t']:>4d} trades | {mwr:.0f}% WR")

print("\nSPORT BREAKDOWN:")
for sp, s in sorted(ss.items(), key=lambda x: -x[1]["pnl"]):
    if s["t"] > 0:
        swr = s["w"] / s["t"] * 100
        roi = s["pnl"] / s["wag"] * 100 if s["wag"] > 0 else 0
        print(f"  {sp:10s} | {s['t']:>5d} trades | {swr:>4.1f}% WR | ${s['pnl']:>+9,.2f} | {roi:>+5.0f}% ROI")

print("\nMILESTONES:")
for t in [500, 1000, 5000, 10000, 50000, 100000, 200000]:
    for mo in sorted(monthly.keys()):
        if monthly[mo] >= t:
            print(f"  ${t:>7,} in {mo}"); break
    else:
        print(f"  ${t:>7,} not reached")
