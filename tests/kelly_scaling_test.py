"""Test different Kelly scaling factors with optimized PT + entry ranges."""
import math, time, duckdb
from collections import defaultdict

t0 = time.time()
STARTING = 100.0; MAX_BET = 200.0; DAILY_CAP = 25; MIN_TRADES = 20; APRIL_RED = 0.5

SP = {
    "NBA":{"km":0.04,"mp":0.03,"me":0.15}, "NHL":{"km":0.15,"mp":0.08,"me":0.08},
    "EPL":{"km":0.25,"mp":0.10,"me":0.10}, "UCL":{"km":0.12,"mp":0.06,"me":0.08},
    "LALIGA":{"km":0.08,"mp":0.04,"me":0.15}, "WNBA":{"km":0.15,"mp":0.08,"me":0.08},
    "UFC":{"km":0.06,"mp":0.03,"me":0.12}, "NCAAMB":{"km":0.10,"mp":0.05,"me":0.08},
    "NCAAWB":{"km":0.12,"mp":0.06,"me":0.08}, "ATP":{"km":0.12,"mp":0.06,"me":0.08},
    "NFLTD":{"km":0.20,"mp":0.10,"me":0.05}, "NHLSPREAD":{"km":0.15,"mp":0.08,"me":0.05},
    "NBASPREAD":{"km":0.06,"mp":0.03,"me":0.12}, "NFLSPREAD":{"km":0.06,"mp":0.03,"me":0.12},
}

ET = {
    "EPL":{(71,85):0.485}, "UCL":{(66,70):0.400,(76,85):0.641},
    "LALIGA":{(81,90):0.588}, "WNBA":{(55,62):0.380,(71,77):0.550,(83,87):0.540},
    "UFC":{(76,85):0.622}, "NCAAMB":{(66,70):0.536,(71,80):0.656,(82,90):0.770},
    "NCAAWB":{(61,70):0.600,(71,80):0.680,(81,85):0.750},
    "ATP":{(71,75):0.650,(76,80):0.654,(81,85):0.765},
    "NFLTD":{(55,65):0.492,(66,75):0.452,(76,85):0.545,(86,95):0.286},
    "NHLSPREAD":{(55,65):0.500,(66,75):0.450,(76,90):0.400},
    "NBASPREAD":{(55,65):0.480,(66,75):0.440,(76,90):0.380},
    "NFLSPREAD":{(55,65):0.480,(66,75):0.440,(76,90):0.380},
}

PFX = {
    "KXNBAGAME":"NBA","KXNHLGAME":"NHL","KXEPLGAME":"EPL","KXUCLGAME":"UCL",
    "KXLALIGAGAME":"LALIGA","KXWNBAGAME":"WNBA","KXUFCFIGHT":"UFC",
    "KXNCAAMBGAME":"NCAAMB","KXNCAAWBGAME":"NCAAWB","KXATPMATCH":"ATP",
    "KXNFLANYTD":"NFLTD","KXNHLSPREAD":"NHLSPREAD","KXNBASPREAD":"NBASPREAD",
    "KXNFLSPREAD":"NFLSPREAD",
}

OPT = {
    "EPL":{"pt":100,"lo":76,"hi":90}, "NBA":{"pt":150,"lo":76,"hi":90},
    "NBASPREAD":{"pt":150,"lo":76,"hi":90}, "NCAAMB":{"pt":100,"lo":61,"hi":90},
    "NFLSPREAD":{"pt":200,"lo":76,"hi":90}, "NFLTD":{"pt":100,"lo":71,"hi":85},
    "NHL":{"pt":100,"lo":76,"hi":90}, "NHLSPREAD":{"pt":300,"lo":76,"hi":90},
    "UCL":{"pt":100,"lo":71,"hi":85}, "UFC":{"pt":200,"lo":61,"hi":90},
    "WNBA":{"pt":100,"lo":76,"hi":90},
}

def pp(s, yp):
    if s == "NBA": return max(0.20, 0.50 - (yp - 60) * 0.004)
    if s == "NHL": return max(0.30, 0.55 - (yp - 60) * 0.003)
    return None

def kfee(c):
    p = c / 100.0
    return math.ceil(0.07 * p * (1 - p) * 100) / 100

def get_ay(s, yp):
    r = pp(s, yp)
    if r: return r
    for (lo, hi), rate in ET.get(s, {}).items():
        if lo <= yp <= hi: return rate
    return None

con = duckdb.connect()
mp = "data/trevorjs/markets-*.parquet"
tp = "data/trevorjs/trades-*.parquet"
cp = " ".join(f"WHEN event_ticker LIKE '{p}%' THEN '{s}'" for p, s in PFX.items())
lk = " OR ".join(f"event_ticker LIKE '{p}%'" for p in PFX.keys())

print("Loading data...")
mdf = con.sql(f"""
    WITH gm AS (SELECT ticker,result,event_ticker,volume,CASE {cp} END as sport
        FROM '{mp}' WHERE ({lk}) AND status='finalized' AND result IN ('yes','no')),
    ts AS (SELECT ticker,MIN(created_time) as ft,COUNT(*) as tc FROM '{tp}'
        WHERE ticker IN (SELECT ticker FROM gm) AND created_time>='2025-01-01'
        GROUP BY ticker HAVING COUNT(*)>={MIN_TRADES}),
    ft AS (SELECT t.ticker,t.yes_price,CAST(t.created_time AS DATE) as td
        FROM '{tp}' t JOIN ts ON t.ticker=ts.ticker AND t.created_time=ts.ft)
    SELECT ft.ticker,ft.yes_price,ft.td,gm.result,gm.sport,gm.volume
    FROM ft JOIN gm ON ft.ticker=gm.ticker JOIN ts ON ft.ticker=ts.ticker
    WHERE gm.sport IS NOT NULL ORDER BY ft.td
""").fetchdf()

markets = []
for _, row in mdf.iterrows():
    s = row["sport"]; yp = int(row["yes_price"]); d = str(row["td"])
    mn = int(d[5:7]); dn = int(d[8:10]); vol = float(row["volume"])
    if s == "NHL" and ((mn > 4 or (mn == 4 and dn > 16)) and mn < 10): continue
    if s == "NBA" and ((mn > 4 or (mn == 4 and dn > 19)) and mn < 10): continue
    if s == "NBA" and 500000 <= vol <= 2000000: continue
    opt = OPT.get(s)
    if not opt or yp < opt["lo"] or yp > opt["hi"]: continue
    ay = get_ay(s, yp)
    if not ay: continue
    p = SP.get(s)
    if not p: continue
    edge = (yp / 100.0) - ay
    nc = 100 - yp; noc = nc / 100.0
    f = 0.07 * noc * (1 - noc); fr = (f + 0.005) / noc if noc > 0 else 0
    if edge - fr < p["me"] or edge < 0.03: continue
    b = (yp / 100.0) / noc if noc > 0 else 0
    kr = (b * (1 - ay) - ay) / b if b > 0 else 0
    markets.append({"tk": row["ticker"], "yp": yp, "sp": s, "d": d, "res": row["result"],
        "mn": mn, "nc": nc, "kr": kr, "noc": noc})

print(f"  {len(markets)} tradeable markets")

# Load PT crossings
tks = [m["tk"] for m in markets]
con.execute("CREATE TEMP TABLE tk (ticker VARCHAR)")
for t in tks: con.execute("INSERT INTO tk VALUES (?)", [t])

ptx = {}
for s, opt in OPT.items():
    pt = opt["pt"]; mult = 1 + pt / 100.0
    cands = [(m["tk"], int(100 - mult * m["nc"])) for m in markets if m["sp"] == s and int(100 - mult * m["nc"]) > 0]
    if not cands: continue
    con.execute(f"DROP TABLE IF EXISTS ptc_{s}")
    con.execute(f"CREATE TEMP TABLE ptc_{s} (ticker VARCHAR, ym INTEGER)")
    for tk, ym in cands: con.execute(f"INSERT INTO ptc_{s} VALUES (?, ?)", [tk, ym])
    cdf = con.sql(f"""
        WITH ft AS (SELECT ticker,MIN(created_time) as ft FROM '{tp}'
            WHERE ticker IN (SELECT ticker FROM ptc_{s}) AND created_time>='2025-01-01' GROUP BY ticker),
        cr AS (SELECT t.ticker,t.yes_price,ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
            FROM '{tp}' t JOIN ft ON t.ticker=ft.ticker JOIN ptc_{s} c ON t.ticker=c.ticker
            WHERE t.created_time>ft.ft AND t.created_time>='2025-01-01' AND t.yes_price<=c.ym)
        SELECT ticker,yes_price FROM cr WHERE rn=1
    """).fetchdf()
    for _, r in cdf.iterrows():
        ptx.setdefault(r["ticker"], {})[pt] = int(r["yes_price"])

print(f"  PT crossings loaded ({time.time()-t0:.1f}s)")

# Test Kelly scales
SCALES = [0.33, 0.50, 0.66, 1.00, 1.50, 2.00, 3.00]

print()
print("=" * 80)
print("KELLY SCALING TEST — Optimal PT + Entry Ranges")
print("=" * 80)
print(f"  {'Scale':>6} {'Trades':>7} {'Final':>14} {'P&L':>14} {'MaxDD':>7} {'Sharpe':>7} {'AvgBet':>8}")
print("-" * 70)

for scale in SCALES:
    br = STARTING; pk = STARTING; mdd = 0.0; tt = 0; tw = 0; pnls = []; ld = None; dc = 0; tb = 0.0
    for m in sorted(markets, key=lambda x: x["d"]):
        s = m["sp"]; p = SP[s]; opt = OPT[s]
        if m["d"] != ld: ld = m["d"]; dc = 0
        if dc >= DAILY_CAP: continue
        km = p["km"] * scale
        mp_ = p["mp"] * scale
        if m["mn"] == 4: km *= APRIL_RED; mp_ *= APRIL_RED
        ka = max(0.0, min(m["kr"] * km, mp_))
        if ka <= 0: continue
        bet = min(br * ka, MAX_BET); noc = m["noc"]; nc = m["nc"]
        contracts = max(1, int(bet / noc)); cost = contracts * noc
        if cost > br: continue
        ef = kfee(nc) * contracts; dc += 1; tt += 1; tb += cost

        pt_trig = False
        cross = ptx.get(m["tk"], {}).get(opt["pt"])
        if cross is not None:
            enoc = 100 - cross; enocf = enoc / 100.0
            xf = kfee(enoc) * contracts
            pnl = contracts * (enocf - noc) - ef - xf; pt_trig = True
        if not pt_trig:
            if m["res"] == "no": pnl = contracts * (1.0 - noc) - ef; tw += 1
            else: pnl = -(cost + ef)

        br += pnl; pnls.append(pnl)
        if br > pk: pk = br
        dd = (pk - br) / pk if pk > 0 else 0
        if dd > mdd: mdd = dd
        if br < 2: break

    ap = sum(pnls) / len(pnls) if pnls else 0
    sd = (sum((p2 - ap) ** 2 for p2 in pnls) / len(pnls)) ** 0.5 if len(pnls) > 1 else 1
    sh = ap / sd if sd > 0 else 0
    ab = tb / tt if tt > 0 else 0
    print(f"  {scale:>5.2f}x {tt:>7} ${br:>13,.2f} ${br-STARTING:>13,.2f} {mdd:>6.1%} {sh:>7.3f} ${ab:>7.2f}")

print()
print("  0.33x = current spread-thin (conservative)")
print("  1.00x = full sport-specific Kelly")
print("  2.00x = aggressive (2x sport Kelly)")
print("  3.00x = very aggressive")
