"""
Dynamic Kelly analysis: should bet sizing vary by entry price?

Hypothesis: cheap NO entries (5-15c) have higher edge but higher variance.
Expensive NO entries (35-45c) have lower edge but more consistent.
Optimal Kelly should scale with the risk/reward profile at each price level.

Tests:
1. Fixed Kelly vs price-scaled Kelly
2. Win rate by entry price (does cheaper = riskier?)
3. Variance by entry price
4. Combined dynamic PT + dynamic Kelly optimization
"""
import duckdb
import math
import time

t0 = time.time()
con = duckdb.connect()
mp = 'data/trevorjs/markets-*.parquet'
tp = 'data/trevorjs/trades-*.parquet'

SPORT_FROM_PREFIX = {
    "KXNBAGAME": "NBA", "KXNHLGAME": "NHL", "KXUCLGAME": "UCL",
    "KXWNBAGAME": "WNBA", "KXATPMATCH": "ATP", "KXNFLANYTD": "NFLTD",
    "KXNHLSPREAD": "NHLSPREAD", "KXNBASPREAD": "NBASPREAD",
    "KXNFLSPREAD": "NFLSPREAD", "KXMLBGAME": "MLB",
    "KXNFLTEAMTOTAL": "NFLTT", "KXCFBGAME": "CFB",
    "KXNBAPTS": "NBA_PTS", "KXNBA3PT": "NBA_3PT", "KXNBAREB": "NBA_REB",
}

case_parts = [f"WHEN event_ticker LIKE '{p}%' THEN '{s}'" for p, s in SPORT_FROM_PREFIX.items()]
case_stmt = ' '.join(case_parts)
like_clauses = ' OR '.join(f"event_ticker LIKE '{p}%'" for p in SPORT_FROM_PREFIX.keys())

EDGE_TABLES = {
    "UCL": {(66, 70): 0.400, (76, 85): 0.641},
    "WNBA": {(55, 62): 0.380, (71, 77): 0.550, (83, 87): 0.540},
    "ATP": {(71, 75): 0.650, (76, 80): 0.654, (81, 85): 0.765},
    "NFLTD": {(55, 65): 0.492, (66, 75): 0.452, (76, 85): 0.545, (86, 95): 0.286},
    "NHLSPREAD": {(55, 65): 0.500, (66, 75): 0.450, (76, 90): 0.400},
    "NBASPREAD": {(55, 65): 0.480, (66, 75): 0.440, (76, 90): 0.380},
    "NFLSPREAD": {(55, 65): 0.480, (66, 75): 0.440, (76, 90): 0.380},
    "MLB": {(76, 84): 0.640},
    "NFLTT": {(55, 65): 0.500, (66, 75): 0.480, (76, 85): 0.450},
    "CFB": {(55, 65): 0.520, (66, 75): 0.550, (76, 85): 0.620},
    "NBA_3PT": {(55, 64): 0.497, (65, 74): 0.594, (75, 84): 0.707, (85, 95): 0.771},
    "NBA_PTS": {(55, 64): 0.538, (65, 74): 0.657, (75, 84): 0.736, (85, 95): 0.765},
    "NBA_REB": {(55, 64): 0.574, (65, 74): 0.629, (75, 84): 0.701, (85, 95): 0.864},
}

SPORT_PARAMS = {
    "NHL": {"km": 0.15, "mp": 0.08, "me": 0.12},
    "NHLSPREAD": {"km": 0.15, "mp": 0.08, "me": 0.10},
    "NBASPREAD": {"km": 0.06, "mp": 0.03, "me": 0.12},
    "NFLTD": {"km": 0.20, "mp": 0.10, "me": 0.10},
    "NFLTT": {"km": 0.10, "mp": 0.05, "me": 0.10},
    "NFLSPREAD": {"km": 0.06, "mp": 0.03, "me": 0.12},
    "NBA": {"km": 0.04, "mp": 0.03, "me": 0.15},
    "ATP": {"km": 0.12, "mp": 0.06, "me": 0.10},
    "UCL": {"km": 0.12, "mp": 0.06, "me": 0.10},
    "WNBA": {"km": 0.15, "mp": 0.08, "me": 0.10},
    "CFB": {"km": 0.08, "mp": 0.04, "me": 0.12},
    "MLB": {"km": 0.06, "mp": 0.03, "me": 0.12},
    "NBA_3PT": {"km": 0.12, "mp": 0.06, "me": 0.10},
    "NBA_PTS": {"km": 0.10, "mp": 0.05, "me": 0.10},
    "NBA_REB": {"km": 0.08, "mp": 0.04, "me": 0.10},
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
for col in ['yp', 'max_no_cents']:
    df[col] = pd.to_numeric(df[col], errors='coerce')
df = df.dropna(subset=['yp', 'max_no_cents'])
df['yp'] = df['yp'].astype(int)
df['max_no_cents'] = df['max_no_cents'].astype(int)
print(f"  {len(df)} markets loaded ({time.time()-t0:.1f}s)")


# ═══════════════════════════════════════════════════════════════
# ANALYSIS 1: Win rate and variance by entry price
# ═══════════════════════════════════════════════════════════════
print()
print("=" * 80)
print("  ANALYSIS 1: WIN RATE & VARIANCE BY ENTRY PRICE")
print("=" * 80)
print()

for bucket_lo, bucket_hi in [(55, 64), (65, 74), (75, 84), (85, 95)]:
    bdf = df[(df['yp'] >= bucket_lo) & (df['yp'] <= bucket_hi)]
    if len(bdf) < 20:
        continue
    no_price = (100 - (bucket_lo + bucket_hi) / 2) / 100.0
    no_wins = len(bdf[bdf['result'] == 'no'])
    wr = no_wins / len(bdf) * 100

    # Per-trade P&L variance (hold to settlement)
    pnls = []
    for _, row in bdf.iterrows():
        entry_no = (100 - row['yp']) / 100.0
        fee = kalshi_fee(row['yp'])
        if row['result'] == 'no':
            pnls.append(1.0 - entry_no - fee)
        else:
            pnls.append(-(entry_no + fee))

    avg_pnl = sum(pnls) / len(pnls)
    var_pnl = sum((p - avg_pnl)**2 for p in pnls) / len(pnls)
    std_pnl = var_pnl ** 0.5
    sharpe = avg_pnl / std_pnl if std_pnl > 0 else 0

    # Theoretical optimal Kelly
    if wr > 0 and no_price > 0:
        b = (1.0 - no_price) / no_price  # payout odds
        p = wr / 100.0
        q = 1 - p
        kelly_opt = (b * p - q) / b if b > 0 else 0
        kelly_opt = max(0, kelly_opt)
    else:
        kelly_opt = 0

    print(f"  YES {bucket_lo}-{bucket_hi}c (NO {100-bucket_hi}-{100-bucket_lo}c):")
    print(f"    Markets: {len(bdf):>5}  NO Win Rate: {wr:.1f}%")
    print(f"    Avg P&L: ${avg_pnl:.4f}  Std: ${std_pnl:.4f}  Sharpe: {sharpe:.3f}")
    print(f"    Optimal Kelly: {kelly_opt:.3f}  (implies {kelly_opt*100:.1f}% of bankroll)")
    print()


# ═══════════════════════════════════════════════════════════════
# ANALYSIS 2: Full simulation - fixed vs dynamic Kelly+PT
# ═══════════════════════════════════════════════════════════════
print("=" * 80)
print("  ANALYSIS 2: FULL SIMULATION - FIXED vs DYNAMIC KELLY+PT")
print("  Starting: $150, pre-game pricing, 12 months")
print("=" * 80)
print()


def get_dynamic_pt(sport, entry_no_cents):
    """Dynamic PT based on sport + entry price."""
    # Props: always hold to settlement
    if sport in ("NBA_3PT", "NBA_PTS", "NBA_REB", "NFLTD", "NFLTT"):
        return None  # Hold

    # Game winners/spreads: PT inversely proportional to entry price
    if entry_no_cents <= 15:
        return 500  # Cheap: let winners run
    elif entry_no_cents <= 25:
        return 200  # Moderate: take at 2x
    elif entry_no_cents <= 35:
        return 100  # Mid: take at 1x gain
    else:
        return 50   # Expensive: grab any gain


def get_dynamic_kelly_mult(sport, entry_no_cents, base_km):
    """Dynamic Kelly multiplier based on entry price risk profile."""
    # Cheap entries (5-15c): high upside but high risk
    # Scale down Kelly slightly for cheap (more variance)
    # Scale up Kelly for mid-range (sweet spot)
    # Scale down for expensive (low upside, not worth large bet)

    if entry_no_cents <= 15:
        return base_km * 0.7   # Reduce: high variance lottery tickets
    elif entry_no_cents <= 25:
        return base_km * 1.2   # Boost: best risk/reward zone
    elif entry_no_cents <= 35:
        return base_km * 1.0   # Standard
    else:
        return base_km * 0.8   # Reduce: low upside


def run_full_sim(label, use_dynamic_pt=False, use_dynamic_kelly=False,
                 kelly_global=0.50, starting=150, max_bet=200):
    """Run a full portfolio simulation."""
    bankroll = float(starting)
    peak = bankroll
    max_dd = 0.0
    trades = 0
    wins = 0
    total_cost = 0.0

    for _, row in df.iterrows():
        sport = row['sport']
        yp = int(row['yp'])
        result = row['result']
        max_no = int(row['max_no_cents'])

        params = SPORT_PARAMS.get(sport)
        if not params:
            continue

        yes_rate = get_yes_rate(sport, yp)
        if yes_rate is None:
            continue

        no_price = (100 - yp) / 100.0
        entry_no_cents = 100 - yp
        fee = kalshi_fee(yp)
        edge = (yp / 100.0) - yes_rate
        fee_drag = fee / no_price if no_price > 0 else 0

        if edge - fee_drag < params["me"]:
            continue

        # Kelly sizing
        if use_dynamic_kelly:
            km = get_dynamic_kelly_mult(sport, entry_no_cents, params["km"])
        else:
            km = params["km"]

        bet = km * kelly_global * bankroll
        mp_limit = params["mp"]
        if use_dynamic_kelly:
            mp_limit = get_dynamic_kelly_mult(sport, entry_no_cents, params["mp"])
        bet = min(bet, max_bet, mp_limit * bankroll)

        if bet < 0.10:
            continue

        # Drawdown circuit breaker
        if peak > 0:
            dd = (peak - bankroll) / peak
            if dd >= 0.40: continue
            elif dd >= 0.25: bet *= 0.25
            elif dd >= 0.15: bet *= 0.50

        contracts = bet / no_price
        cost = no_price * contracts + fee * (contracts / 1)
        if cost > bankroll:
            continue

        bankroll -= cost
        total_cost += cost
        trades += 1

        # Profit-take
        if use_dynamic_pt:
            pt_pct_val = get_dynamic_pt(sport, entry_no_cents)
        else:
            sport_pt_map = {
                "NBA": 150, "NBASPREAD": 150, "NFLSPREAD": 200, "NFLTD": 100,
                "NHL": 100, "NHLSPREAD": 300, "UCL": 100, "WNBA": 100, "ATP": 100,
                "CFB": 200, "MLB": 50, "NFLTT": 150,
                "NBA_3PT": 200, "NBA_PTS": 150, "NBA_REB": 150,
            }
            pt_pct_val = sport_pt_map.get(sport, 150)

        if pt_pct_val is not None:
            pt_price = no_price * (1 + pt_pct_val / 100.0)
            max_no_price = max_no / 100.0
            if max_no_price >= pt_price:
                exit_fee = kalshi_fee(int((1 - pt_price) * 100))
                revenue = pt_price * contracts - exit_fee
                bankroll += revenue
                if revenue > cost: wins += 1
                if bankroll > peak: peak = bankroll
                dd = (peak - bankroll) / peak if peak > 0 else 0
                if dd > max_dd: max_dd = dd
                continue

        # Hold to settlement
        if result == "no":
            bankroll += 1.0 * contracts
            wins += 1

        if bankroll > peak: peak = bankroll
        dd = (peak - bankroll) / peak if peak > 0 else 0
        if dd > max_dd: max_dd = dd

    wr = wins / trades * 100 if trades > 0 else 0
    return {
        "label": label, "final": round(bankroll, 2), "trades": trades,
        "wr": round(wr, 1), "max_dd": round(max_dd * 100, 1),
        "roi": round((bankroll - starting) / total_cost * 100, 1) if total_cost > 0 else 0,
    }


results = []

# Baseline
r = run_full_sim("1. CURRENT AGENT (fixed PT + fixed Kelly)")
results.append(r)

# Dynamic PT only
r = run_full_sim("2. DYNAMIC PT only", use_dynamic_pt=True)
results.append(r)

# Dynamic Kelly only
r = run_full_sim("3. DYNAMIC KELLY only", use_dynamic_kelly=True)
results.append(r)

# Both dynamic
r = run_full_sim("4. DYNAMIC PT + DYNAMIC KELLY", use_dynamic_pt=True, use_dynamic_kelly=True)
results.append(r)

# Aggressive: higher global Kelly
r = run_full_sim("5. Dynamic PT+Kelly + Kelly 0.75", use_dynamic_pt=True, use_dynamic_kelly=True, kelly_global=0.75)
results.append(r)

# Very aggressive
r = run_full_sim("6. Dynamic PT+Kelly + Kelly 1.0", use_dynamic_pt=True, use_dynamic_kelly=True, kelly_global=1.0)
results.append(r)

print(f"{'Strategy':<48} {'Final':>12} {'Trades':>7} {'WR':>5} {'MaxDD':>6} {'ROI':>6}")
print("-" * 90)
for r in sorted(results, key=lambda x: -x['final']):
    print(f"{r['label']:<48} ${r['final']:>11,.2f} {r['trades']:>7} {r['wr']:>4.1f}% {r['max_dd']:>5.1f}% {r['roi']:>5.1f}%")

print()

# Monthly projection for top strategies
print("1-YEAR PROJECTION FROM $150:")
print(f"{'Strategy':<48} {'Monthly':>8} {'1-Year':>12}")
print("-" * 70)
for r in sorted(results, key=lambda x: -x['final'])[:4]:
    if r['final'] > 150:
        monthly = (r['final'] / 150) ** (1/12) - 1
        yr = 150 * (1 + monthly) ** 12
        print(f"{r['label']:<48} {monthly*100:>6.1f}% ${yr:>11,.0f}")

print(f"\n  Runtime: {time.time()-t0:.1f}s")
