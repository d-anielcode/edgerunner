"""Test: What if we reduce Kelly across all sports during April?"""
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
    "WTA": "KXWTAMATCH%",
    "W1": "KXHIGHNY%", "W2": "KXHIGHCHI%", "W3": "KXHIGHMIA%",
    "W4": "KXHIGHLA%", "W5": "KXHIGHSF%", "W6": "KXHIGHHOU%",
    "W7": "KXHIGHDEN%", "W8": "KXHIGHDC%", "W9": "KXHIGHDAL%",
    "NFLTD": "KXNFLANYTD%",
}
sport_map = {k: ("WEATHER" if k.startswith("W") and len(k) <= 2 else k) for k in sport_patterns}
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
}
SPORT_PARAMS = {
    "EPL": {"kelly_mult": 0.25, "max_position": 0.10, "min_edge": 0.10},
    "UCL": {"kelly_mult": 0.12, "max_position": 0.06, "min_edge": 0.08},
    "LALIGA": {"kelly_mult": 0.08, "max_position": 0.04, "min_edge": 0.15},
    "WNBA": {"kelly_mult": 0.12, "max_position": 0.06, "min_edge": 0.05},
    "UFC": {"kelly_mult": 0.12, "max_position": 0.06, "min_edge": 0.08},
    "NCAAMB": {"kelly_mult": 0.10, "max_position": 0.05, "min_edge": 0.08},
    "NCAAWB": {"kelly_mult": 0.12, "max_position": 0.06, "min_edge": 0.08},
    "WTA": {"kelly_mult": 0.08, "max_position": 0.04, "min_edge": 0.10},
    "WEATHER": {"kelly_mult": 0.25, "max_position": 0.10, "min_edge": 0.10},
    "NFLTD": {"kelly_mult": 0.20, "max_position": 0.10, "min_edge": 0.05},
}

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


def per_price(sp, yp):
    if sp == "NBA": return max(0.20, 0.50 - (yp - 60) * 0.004)
    if sp == "NHL": return max(0.30, 0.55 - (yp - 60) * 0.003)
    return None


def run(april_mult):
    br = 100.0
    peak = 100.0
    max_dd = 0.0
    tt = tw = dc = 0
    ld = None
    traded = set()
    apr_pnl = 0.0
    apr_trades = 0
    apr_start = 0.0
    monthly = {}

    for _, row in all_trades.iterrows():
        d = str(row["trade_date"]); t = row["ticker"]
        yp = int(row["yes_price"]); np_ = int(row["no_price"])
        sp = row["sport"]; res = row["result"]; vol = float(row["volume"])
        m_num = int(d[5:7]); d_num = int(d[8:10])

        if d != ld:
            if ld: monthly[ld[:7]] = br
            dc = 0; ld = d
        if dc >= 15 or br < 2 or t in traded: continue
        if sp == "NHL" and ((m_num > 4 or (m_num == 4 and d_num > 16)) and m_num < 10): continue
        if sp == "NBA" and 500_000 <= vol <= 2_000_000: continue

        min_p = 55 if sp in ("WEATHER", "NFLTD") else 61
        max_p = 95 if sp in ("WEATHER", "NFLTD", "NBA", "NHL") else 90
        if yp < min_p or yp > max_p: continue

        pp = per_price(sp, yp)
        if pp is not None:
            ay = pp; km = 0.25; mp_ = 0.12; me = 0.05 if sp == "NHL" else 0.08
        else:
            params = SPORT_PARAMS.get(sp)
            if not params: continue
            et = EDGE_TABLES.get(sp, {})
            ay = None
            for (lo, hi), rate in et.items():
                if lo <= yp <= hi: ay = rate; break
            if ay is None: continue
            km = params["kelly_mult"]; mp_ = params["max_position"]; me = params["min_edge"]

        edge = (yp / 100.0) - ay
        if edge < me: continue
        nc = np_ / 100.0
        fee = 0.07 * nc * (1 - nc)
        if edge - (fee + 0.005) / nc < 0.03: continue

        b = (yp / 100) / nc if nc > 0 else 0
        kr = (b * (1 - ay) - ay) / b if b > 0 else 0
        ka = max(0, min(kr * km, mp_))

        # April reduction
        if m_num == 4:
            if apr_start == 0: apr_start = br
            ka *= april_mult
        if ka <= 0: continue

        bet = min(br * ka, 100)
        contracts = max(1, int(bet / nc))
        cost = contracts * nc
        if cost > br: continue

        tf = 0.07 * nc * (1 - nc) * contracts
        if res == "no":
            pnl = contracts * (1.0 - nc) - tf; tw += 1
        else:
            pnl = -(cost + tf)

        br += pnl; tt += 1; dc += 1; traded.add(t)
        if m_num == 4: apr_pnl += pnl; apr_trades += 1
        if br > peak: peak = br
        dd = (peak - br) / peak if peak > 0 else 0
        if dd > max_dd: max_dd = dd

    if ld: monthly[ld[:7]] = br
    apr_ret = (apr_pnl / apr_start * 100) if apr_start > 0 else 0
    return {
        "final": br, "trades": tt, "max_dd": max_dd * 100,
        "apr_pnl": apr_pnl, "apr_trades": apr_trades, "apr_ret": apr_ret
    }

print("=" * 95)
print("APRIL KELLY REDUCTION: Risk management during weak month")
print("=" * 95)
print()
print(f"{'Strategy':35s} | {'Year Final':>10s} | {'MaxDD':>6s} | {'Apr P&L':>9s} | {'Apr Ret':>8s} | {'Apr Trades':>10s}")
print("-" * 95)

for mult, label in [
    (1.00, "No reduction (current)"),
    (0.75, "25% reduction in April"),
    (0.50, "50% reduction in April"),
    (0.33, "67% reduction in April"),
    (0.25, "75% reduction in April"),
    (0.00, "Full April veto"),
]:
    r = run(mult)
    print(f"{label:35s} | ${r['final']:>8,.2f} | {r['max_dd']:>5.1f}% | ${r['apr_pnl']:>+7,.2f} | {r['apr_ret']:>+7.1f}% | {r['apr_trades']:>10d}")
