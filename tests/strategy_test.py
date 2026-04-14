"""
Strategy optimization test: what parameter changes maximize returns?

Tests:
1. Higher Kelly (more aggressive sizing)
2. No profit-take vs various PT thresholds
3. Higher min_edge (more selective)
4. Concentrated portfolio (only top Sharpe sports)
5. Combined optimal strategy

Goal: find a realistic path from $150 → $50-100K in 8 months
"""
import duckdb
import math
import time

t0 = time.time()
con = duckdb.connect()
mp = 'data/trevorjs/markets-*.parquet'
tp = 'data/trevorjs/trades-*.parquet'


def _isna(val):
    if val is None:
        return True
    try:
        import pandas as pd
        return pd.isna(val)
    except Exception:
        try:
            return math.isnan(float(val))
        except (TypeError, ValueError):
            return False


# Current agent params (baseline)
BASELINE_PARAMS = {
    "NHL":       {"km": 0.15, "mp": 0.08, "me": 0.12},
    "NHLSPREAD": {"km": 0.15, "mp": 0.08, "me": 0.10},
    "NBASPREAD": {"km": 0.06, "mp": 0.03, "me": 0.12},
    "NFLTD":     {"km": 0.20, "mp": 0.10, "me": 0.10},
    "NFLTT":     {"km": 0.10, "mp": 0.05, "me": 0.10},
    "NFLSPREAD": {"km": 0.06, "mp": 0.03, "me": 0.12},
    "NBA":       {"km": 0.04, "mp": 0.03, "me": 0.15},
    "ATP":       {"km": 0.12, "mp": 0.06, "me": 0.10},
    "UCL":       {"km": 0.12, "mp": 0.06, "me": 0.10},
    "WNBA":      {"km": 0.15, "mp": 0.08, "me": 0.10},
    "CFB":       {"km": 0.08, "mp": 0.04, "me": 0.12},
    "MLB":       {"km": 0.06, "mp": 0.03, "me": 0.12},
    "NBA_3PT":   {"km": 0.12, "mp": 0.06, "me": 0.10},
    "NBA_PTS":   {"km": 0.10, "mp": 0.05, "me": 0.10},
    "NBA_REB":   {"km": 0.08, "mp": 0.04, "me": 0.10},
}

EDGE_TABLES = {
    "UCL":      {(66, 70): 0.400, (76, 85): 0.641},
    "WNBA":     {(55, 62): 0.380, (71, 77): 0.550, (83, 87): 0.540},
    "ATP":      {(71, 75): 0.650, (76, 80): 0.654, (81, 85): 0.765},
    "NFLTD":    {(55, 65): 0.492, (66, 75): 0.452, (76, 85): 0.545, (86, 95): 0.286},
    "NHLSPREAD": {(55, 65): 0.500, (66, 75): 0.450, (76, 90): 0.400},
    "NBASPREAD": {(55, 65): 0.480, (66, 75): 0.440, (76, 90): 0.380},
    "NFLSPREAD": {(55, 65): 0.480, (66, 75): 0.440, (76, 90): 0.380},
    "MLB":      {(76, 84): 0.640},
    "NFLTT":    {(55, 65): 0.500, (66, 75): 0.480, (76, 85): 0.450},
    "CFB":      {(55, 65): 0.520, (66, 75): 0.550, (76, 85): 0.620},
    "NBA_3PT":  {(55, 64): 0.497, (65, 74): 0.594, (75, 84): 0.707, (85, 95): 0.771},
    "NBA_PTS":  {(55, 64): 0.538, (65, 74): 0.657, (75, 84): 0.736, (85, 95): 0.765},
    "NBA_REB":  {(55, 64): 0.574, (65, 74): 0.629, (75, 84): 0.701, (85, 95): 0.864},
}

SPORT_FROM_PREFIX = {
    "KXNBAGAME": "NBA", "KXNHLGAME": "NHL", "KXUCLGAME": "UCL",
    "KXWNBAGAME": "WNBA", "KXATPMATCH": "ATP", "KXNFLANYTD": "NFLTD",
    "KXNHLSPREAD": "NHLSPREAD", "KXNBASPREAD": "NBASPREAD",
    "KXNFLSPREAD": "NFLSPREAD", "KXMLBGAME": "MLB",
    "KXNFLTEAMTOTAL": "NFLTT", "KXCFBGAME": "CFB",
    "KXNBAPTS": "NBA_PTS", "KXNBA3PT": "NBA_3PT", "KXNBAREB": "NBA_REB",
}

SPORT_PT = {
    "NBA": 150, "NBASPREAD": 150, "NFLSPREAD": 200, "NFLTD": 100,
    "NHL": 100, "NHLSPREAD": 300, "UCL": 100, "WNBA": 100, "ATP": 100,
    "CFB": 200, "MLB": 50, "NFLTT": 150,
    "NBA_3PT": 200, "NBA_PTS": 150, "NBA_REB": 150,
}


def per_price_yes_rate(sport, yp):
    if sport == "NBA": return max(0.20, 0.50 - (yp - 60) * 0.004)
    if sport == "NHL": return max(0.30, 0.55 - (yp - 60) * 0.003)
    return None


def get_yes_rate(sport, yp):
    pp = per_price_yes_rate(sport, yp)
    if pp is not None: return pp
    et = EDGE_TABLES.get(sport, {})
    for (lo, hi), v in et.items():
        if lo <= yp <= hi: return v if not isinstance(v, tuple) else v[0]
    return None


def kalshi_fee(yp_cents):
    p = yp_cents / 100.0
    return math.ceil(0.07 * p * (1 - p) * 100) / 100.0


# Load data
case_parts = [f"WHEN event_ticker LIKE '{p}%' THEN '{s}'" for p, s in SPORT_FROM_PREFIX.items()]
case_stmt = ' '.join(case_parts)
like_clauses = ' OR '.join(f"event_ticker LIKE '{p}%'" for p in SPORT_FROM_PREFIX.keys())

print("Loading data...")
df = con.sql(f"""
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
df['yp'] = pd.to_numeric(df['yp'], errors='coerce')
df['max_no_cents'] = pd.to_numeric(df['max_no_cents'], errors='coerce')
df = df.dropna(subset=['yp', 'max_no_cents'])
df['yp'] = df['yp'].astype(int)
df['max_no_cents'] = df['max_no_cents'].astype(int)
print(f"  {len(df)} markets loaded ({time.time()-t0:.1f}s)")


def run_sim(sport_params, kelly_mult=0.33, starting=150, max_bet=200, use_pt=True,
            label="", dd_enabled=True, concentrated_sports=None):
    """Run a full simulation with given parameters."""
    bankroll = float(starting)
    peak = bankroll
    max_dd = 0.0
    trades = 0
    wins = 0

    for _, row in df.iterrows():
        sport = row['sport']
        yp = int(row['yp'])
        result = row['result']
        max_no = int(row['max_no_cents'])

        if concentrated_sports and sport not in concentrated_sports:
            continue

        params = sport_params.get(sport)
        if not params:
            continue

        yes_rate = get_yes_rate(sport, yp)
        if yes_rate is None:
            continue

        no_price = (100 - yp) / 100.0
        fee = kalshi_fee(yp)
        edge = (yp / 100.0) - yes_rate
        fee_drag = fee / no_price if no_price > 0 else 0

        if edge - fee_drag < params["me"]:
            continue

        # Kelly sizing
        bet = params["km"] * kelly_mult * bankroll
        bet = min(bet, max_bet, params["mp"] * bankroll)
        if bet < 0.10:
            continue

        # Drawdown circuit breaker
        if dd_enabled and peak > 0:
            dd = (peak - bankroll) / peak
            if dd >= 0.40:
                continue  # Halted
            elif dd >= 0.25:
                bet *= 0.25
            elif dd >= 0.15:
                bet *= 0.50

        contracts = bet / no_price
        cost = no_price * contracts + fee * (contracts / 1)

        if cost > bankroll:
            continue

        bankroll -= cost
        trades += 1

        # Profit-take check
        pt_pct = SPORT_PT.get(sport, 150) / 100.0
        max_no_price = max_no / 100.0
        pt_triggered = use_pt and max_no_price >= no_price * (1 + pt_pct)

        if pt_triggered:
            exit_price = no_price * (1 + pt_pct)
            exit_fee = kalshi_fee(int((1 - exit_price) * 100))
            revenue = exit_price * contracts - exit_fee
            bankroll += revenue
        elif result == "no":
            bankroll += 1.0 * contracts
            wins += 1
        # else: loss, bankroll already deducted

        if bankroll > peak:
            peak = bankroll
        dd = (peak - bankroll) / peak if peak > 0 else 0
        if dd > max_dd:
            max_dd = dd

    wr = wins / trades * 100 if trades > 0 else 0
    return {
        "label": label,
        "final": round(bankroll, 2),
        "trades": trades,
        "wr": round(wr, 1),
        "max_dd": round(max_dd * 100, 1),
        "return_x": round(bankroll / starting, 1),
    }


print()
print("=" * 80)
print("  STRATEGY OPTIMIZATION TESTS")
print(f"  Starting bankroll: $150 | Data: TrevorJS 2024-2025 (pre-game pricing)")
print("=" * 80)
print()

results = []

# Test 1: Baseline (current agent)
r = run_sim(BASELINE_PARAMS, kelly_mult=0.33, use_pt=True, label="BASELINE (current agent)")
results.append(r)

# Test 2: Higher Kelly (0.50 instead of 0.33)
r = run_sim(BASELINE_PARAMS, kelly_mult=0.50, use_pt=True, label="Kelly 0.50x (was 0.33)")
results.append(r)

# Test 3: Full Kelly (1.0)
r = run_sim(BASELINE_PARAMS, kelly_mult=1.0, use_pt=True, label="Kelly 1.0x (FULL Kelly)")
results.append(r)

# Test 4: No profit-take (hold to settlement)
r = run_sim(BASELINE_PARAMS, kelly_mult=0.33, use_pt=False, label="No profit-take (hold)")
results.append(r)

# Test 5: Higher Kelly + No PT
r = run_sim(BASELINE_PARAMS, kelly_mult=0.50, use_pt=False, label="Kelly 0.50 + No PT")
results.append(r)

# Test 6: Concentrated on top sports only (NHLSPREAD, NFLTD, NBA_3PT, NBA_PTS)
top_sports = {"NHLSPREAD", "NFLTD", "NBA_3PT", "NBA_PTS", "NBASPREAD"}
r = run_sim(BASELINE_PARAMS, kelly_mult=0.50, use_pt=True, label="Concentrated top 5 sports",
            concentrated_sports=top_sports)
results.append(r)

# Test 7: Higher min_edge (0.15 across all sports)
high_edge_params = {}
for sport, p in BASELINE_PARAMS.items():
    high_edge_params[sport] = {**p, "me": 0.15}
r = run_sim(high_edge_params, kelly_mult=0.50, use_pt=True, label="Min edge 15% + Kelly 0.50")
results.append(r)

# Test 8: Aggressive Kelly on top sports, higher min_edge
agg_params = {}
for sport, p in BASELINE_PARAMS.items():
    if sport in top_sports:
        agg_params[sport] = {"km": p["km"] * 2, "mp": p["mp"] * 2, "me": 0.12}
    else:
        agg_params[sport] = {**p, "me": 0.15}
r = run_sim(agg_params, kelly_mult=0.50, use_pt=True, label="2x Kelly top sports + 15% edge others")
results.append(r)

# Test 9: No drawdown circuit breakers
r = run_sim(BASELINE_PARAMS, kelly_mult=0.50, use_pt=True, dd_enabled=False,
            label="Kelly 0.50 + No DD breakers")
results.append(r)

# Test 10: The "moonshot" — full Kelly, concentrated, no PT, no DD
r = run_sim(BASELINE_PARAMS, kelly_mult=1.0, use_pt=False, dd_enabled=False,
            label="MOONSHOT: Full Kelly + No PT + No DD",
            concentrated_sports=top_sports)
results.append(r)

# Test 11: Balanced aggressive — 0.50 Kelly, top sports, higher edge, with PT
balanced_params = {}
for sport in top_sports:
    p = BASELINE_PARAMS.get(sport, {"km": 0.10, "mp": 0.05, "me": 0.10})
    balanced_params[sport] = {"km": p["km"] * 1.5, "mp": min(p["mp"] * 1.5, 0.15), "me": 0.12}
r = run_sim(balanced_params, kelly_mult=0.50, use_pt=True, label="BALANCED: 1.5x Kelly top5, 12% edge")
results.append(r)

# Print results
print(f"{'Strategy':<45} {'Final':>10} {'Trades':>7} {'WR':>5} {'MaxDD':>6} {'Return':>8}")
print("-" * 85)
for r in sorted(results, key=lambda x: -x['final']):
    print(f"{r['label']:<45} ${r['final']:>9,.2f} {r['trades']:>7} {r['wr']:>4.1f}% {r['max_dd']:>5.1f}% {r['return_x']:>7.1f}x")

print()

# Extrapolate to 8 months
print("EXTRAPOLATION TO 8 MONTHS (if edge holds):")
print(f"{'Strategy':<45} {'Monthly':>8} {'8-Month Est':>12}")
print("-" * 70)
# The backtest covers ~12 months. Monthly growth = (final/start)^(1/12) - 1
for r in sorted(results, key=lambda x: -x['final']):
    if r['final'] > 150:
        monthly = (r['final'] / 150) ** (1/12) - 1
        est_8mo = 150 * (1 + monthly) ** 8
        print(f"{r['label']:<45} {monthly*100:>6.1f}% ${est_8mo:>11,.0f}")

print(f"\n  Runtime: {time.time()-t0:.1f}s")
