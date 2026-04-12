"""
Test the 3 proposed strategy improvements against real data.
Compare current strategy vs each improvement individually.
$300 start, $100 max bet, recent data only (2025+).
"""
import duckdb
from collections import defaultdict

con = duckdb.connect()
mp = "data/trevorjs/markets-*.parquet"
tp = "data/trevorjs/trades-*.parquet"

# Current edge tables (our baseline)
EDGE_TABLES = {
    "NBA":    {(61, 75): 0.608, (76, 90): 0.719},
    "NHL":    {(61, 75): 0.545, (76, 90): 0.563},
}

# Load NBA + NHL data with ALL trade info (not just first trade)
print("Loading full trade data...")
all_data = con.sql(f"""
    WITH gm AS (
        SELECT ticker, result, event_ticker, volume,
               CASE WHEN event_ticker LIKE 'KXNBAGAME%' THEN 'NBA'
                    WHEN event_ticker LIKE 'KXNHLGAME%' THEN 'NHL' END as sport
        FROM '{mp}'
        WHERE (event_ticker LIKE 'KXNBAGAME%' OR event_ticker LIKE 'KXNHLGAME%')
              AND status = 'finalized' AND result IN ('yes','no')
    ),
    all_trades AS (
        SELECT t.ticker, t.yes_price, t.no_price, t.taker_side, t.count as trade_size,
               t.created_time, CAST(t.created_time AS DATE) as trade_date,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
        FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM gm)
              AND t.created_time >= '2025-01-01'
    )
    SELECT all_trades.*, gm.result, gm.sport, gm.volume
    FROM all_trades JOIN gm ON all_trades.ticker = gm.ticker
    ORDER BY all_trades.created_time
""").fetchdf()

print(f"Loaded {len(all_data)} trades across {all_data['ticker'].nunique()} markets")

# Get first trades only (our current strategy baseline)
first_trades = all_data[all_data['rn'] == 1].copy()

# ================================================================
# STRATEGY A: Per-Price Kelly (research recommendation)
# Instead of 2 buckets, use exact per-cent win rates
# ================================================================
print("\n" + "=" * 80)
print("STRATEGY A: PER-PRICE KELLY SIZING")
print("Current: 2 buckets (61-75c, 76-90c) with fixed Kelly per bucket")
print("Proposed: Exact Kelly at each cent based on empirical win rate")
print("=" * 80)

# Calculate per-cent win rates
for sport in ['NBA', 'NHL']:
    df = first_trades[first_trades['sport'] == sport]
    print(f"\n  {sport} per-cent analysis:")
    print(f"  {'Price':>5s} | {'Trades':>6s} | {'NO Win%':>7s} | {'Payout':>7s} | {'EV/dollar':>9s} | {'Full Kelly':>10s} | {'0.25x Kelly':>11s}")
    print("  " + "-" * 75)

    for price in range(61, 91):
        subset = df[df['yes_price'] == price]
        if len(subset) < 3:
            continue
        no_wins = (subset['result'] == 'no').sum()
        total = len(subset)
        p = no_wins / total  # probability of NO winning
        q = 1 - p
        no_cost = (100 - price) / 100
        b = price / (100 - price)  # payout ratio

        ev = b * p - q  # EV per dollar
        kelly = (b * p - q) / b if b > 0 else 0
        kelly_frac = max(0, kelly * 0.25)

        marker = " ***" if ev > 0.3 else (" *" if ev > 0.1 else "")
        print(f"  {price:>5d}c | {total:>6d} | {p*100:>6.1f}% | {b:>6.2f}x | ${ev:>+8.3f} | {kelly*100:>9.2f}% | {kelly_frac*100:>10.2f}%{marker}")

# ================================================================
# STRATEGY B: Order Flow Signal
# Check if first 20 trades' NO/YES ratio predicts outcomes
# ================================================================
print("\n" + "=" * 80)
print("STRATEGY B: ORDER FLOW DETECTION")
print("When early trades show heavy NO buying, bet bigger")
print("=" * 80)

# Calculate order flow for first 20 trades per market
market_flow = all_data[all_data['rn'] <= 20].groupby('ticker').agg(
    yes_vol=('trade_size', lambda x: x[all_data.loc[x.index, 'taker_side'] == 'yes'].sum()),
    no_vol=('trade_size', lambda x: x[all_data.loc[x.index, 'taker_side'] == 'no'].sum()),
).reset_index()

# Merge with first trade data
first_with_flow = first_trades.merge(market_flow, on='ticker', how='left')
first_with_flow['flow_ratio'] = first_with_flow['no_vol'] / (first_with_flow['yes_vol'] + 1)

for sport in ['NBA', 'NHL']:
    df = first_with_flow[(first_with_flow['sport'] == sport) & (first_with_flow['yes_price'].between(61, 90))]
    print(f"\n  {sport} Order Flow Analysis:")

    for label, condition in [
        ("Heavy NO flow (2x+)", df['flow_ratio'] >= 2),
        ("Moderate NO flow (1.3-2x)", (df['flow_ratio'] >= 1.3) & (df['flow_ratio'] < 2)),
        ("Balanced flow", (df['flow_ratio'] >= 0.7) & (df['flow_ratio'] < 1.3)),
        ("Moderate YES flow (1.3-2x)", (df['flow_ratio'] < 0.7) & (df['flow_ratio'] >= 0.35)),
        ("Heavy YES flow (2x+)", df['flow_ratio'] < 0.35),
    ]:
        subset = df[condition]
        if len(subset) >= 5:
            no_wins = (subset['result'] == 'no').sum()
            wr = no_wins / len(subset) * 100
            print(f"    {label:30s}: {len(subset):>5d} trades | NO wins {wr:>5.1f}%")

# ================================================================
# STRATEGY C: Volume-Based Sizing
# Bet bigger on high-volume (marquee) games
# ================================================================
print("\n" + "=" * 80)
print("STRATEGY C: VOLUME-BASED SIZING")
print("High volume = more noise traders = more mispricing")
print("=" * 80)

for sport in ['NBA', 'NHL']:
    df = first_trades[(first_trades['sport'] == sport) & (first_trades['yes_price'].between(61, 90))]
    print(f"\n  {sport} Volume Analysis:")

    for label, lo, hi in [
        ("Low (<500K)", 0, 500000),
        ("Medium (500K-2M)", 500000, 2000000),
        ("High (2M-5M)", 2000000, 5000000),
        ("Very High (5M+)", 5000000, 999999999),
    ]:
        subset = df[(df['volume'] >= lo) & (df['volume'] < hi)]
        if len(subset) >= 5:
            no_wins = (subset['result'] == 'no').sum()
            wr = no_wins / len(subset) * 100
            # Calculate ROI
            total_cost = sum((100 - p) / 100 for p in subset['yes_price'])
            total_pnl = sum(
                (1.0 - (100 - p) / 100) if r == 'no' else -(100 - p) / 100
                for p, r in zip(subset['yes_price'], subset['result'])
            )
            roi = total_pnl / total_cost * 100 if total_cost > 0 else 0
            print(f"    {label:20s}: {len(subset):>5d} trades | NO wins {wr:>5.1f}% | ROI {roi:>+6.1f}%")

# ================================================================
# BACKTEST COMPARISON: Current vs Each Strategy
# ================================================================
print("\n" + "=" * 80)
print("BACKTEST: CURRENT vs IMPROVED STRATEGIES ($300 start, $100 max)")
print("=" * 80)

def run_backtest(trades_df, kelly_func, label, max_bet=100):
    bankroll = 300.0
    total_t = 0
    total_w = 0
    traded = set()

    for _, row in trades_df.iterrows():
        ticker = row['ticker']
        if ticker in traded:
            continue
        yes_p = int(row['yes_price'])
        sport = row['sport']
        result = row['result']
        volume = int(row['volume'])
        flow_ratio = row.get('flow_ratio', 0.5)

        if yes_p < 61 or yes_p > 90:
            continue

        # NHL playoff veto
        date = str(row['trade_date'])
        mo = int(date[5:7]); dy = int(date[8:10])
        if sport == 'NHL' and ((mo > 4 or (mo == 4 and dy > 16)) and mo < 10):
            continue

        # Get Kelly from the strategy function
        kelly_adj = kelly_func(yes_p, sport, volume, flow_ratio)
        if kelly_adj <= 0:
            continue

        no_cost = (100 - yes_p) / 100
        bet = min(bankroll * kelly_adj, max_bet)
        contracts = max(1, int(bet / no_cost))
        cost = contracts * no_cost
        if cost > bankroll:
            continue

        fee = 0.07 * no_cost * (1 - no_cost) * contracts
        if result == 'no':
            bankroll += contracts * (1.0 - no_cost) - fee
            total_w += 1
        else:
            bankroll -= cost + fee

        total_t += 1
        traded.add(ticker)

    wr = total_w / max(total_t, 1) * 100
    return {"label": label, "final": bankroll, "trades": total_t, "wr": wr}

# Merge flow data
bt_data = first_trades.merge(market_flow, on='ticker', how='left')
bt_data['flow_ratio'] = bt_data['no_vol'] / (bt_data['yes_vol'] + 1)
bt_data = bt_data.sort_values('created_time')

# Current strategy
def current_kelly(yes_p, sport, volume, flow):
    params = {"NBA": 0.10, "NHL": 0.30}
    edges = {
        "NBA": {(61,75): 0.608, (76,90): 0.719},
        "NHL": {(61,75): 0.545, (76,90): 0.563},
    }
    et = edges.get(sport, {})
    actual_yes = None
    for (lo, hi), rate in et.items():
        if lo <= yes_p <= hi:
            actual_yes = rate
            break
    if actual_yes is None:
        return 0
    edge = (yes_p / 100) - actual_yes
    min_edge = 0.08 if sport == 'NBA' else 0.05
    if edge < min_edge:
        return 0
    b = (yes_p / 100) / ((100 - yes_p) / 100)
    p = 1 - actual_yes
    q = actual_yes
    kr = (b * p - q) / b if b > 0 else 0
    km = params.get(sport, 0.10)
    return max(0, min(kr * km, 0.12))

# Strategy A: Per-price Kelly (0.25x fractional)
def perprice_kelly(yes_p, sport, volume, flow):
    # Use empirical win rates at each price (pre-computed from data)
    # For now, use a smooth interpolation
    if sport == 'NBA':
        p = max(0.20, 0.50 - (yes_p - 60) * 0.004)  # ~40% at 65c, ~30% at 85c
    elif sport == 'NHL':
        p = max(0.30, 0.55 - (yes_p - 60) * 0.003)  # ~50% at 65c, ~40% at 85c
    else:
        return 0
    b = (yes_p / 100) / ((100 - yes_p) / 100)
    q = 1 - p
    kr = (b * p - q) / b if b > 0 else 0
    return max(0, min(kr * 0.25, 0.12))  # 0.25x Kelly, cap 12%

# Strategy B: Order flow boost (current Kelly + 1.5x when smart money agrees)
def flow_kelly(yes_p, sport, volume, flow):
    base = current_kelly(yes_p, sport, volume, flow)
    if base <= 0:
        return 0
    if flow >= 2.0:  # Heavy NO flow = smart money
        return min(base * 2.0, 0.12)  # Double the bet
    elif flow >= 1.3:  # Moderate NO flow
        return min(base * 1.5, 0.12)
    return base

# Strategy C: Volume boost (current Kelly + boost for high volume)
def volume_kelly(yes_p, sport, volume, flow):
    base = current_kelly(yes_p, sport, volume, flow)
    if base <= 0:
        return 0
    if volume >= 5000000:
        return min(base * 1.5, 0.12)
    elif volume >= 2000000:
        return min(base * 1.2, 0.12)
    elif volume < 500000:
        return base * 0.5  # Reduce for low volume
    return base

# Strategy D: All three combined
def combined_kelly(yes_p, sport, volume, flow):
    # Per-price base
    if sport == 'NBA':
        p = max(0.20, 0.50 - (yes_p - 60) * 0.004)
    elif sport == 'NHL':
        p = max(0.30, 0.55 - (yes_p - 60) * 0.003)
    else:
        return 0
    b = (yes_p / 100) / ((100 - yes_p) / 100)
    q = 1 - p
    kr = (b * p - q) / b if b > 0 else 0
    base = max(0, kr * 0.25)

    # Flow boost
    if flow >= 2.0:
        base *= 2.0
    elif flow >= 1.3:
        base *= 1.5

    # Volume boost
    if volume >= 5000000:
        base *= 1.3
    elif volume < 500000:
        base *= 0.5

    return min(base, 0.15)

print()
strategies = [
    (current_kelly, "A) Current strategy"),
    (perprice_kelly, "B) Per-price Kelly (0.25x)"),
    (flow_kelly, "C) Order flow boost"),
    (volume_kelly, "D) Volume-based sizing"),
    (combined_kelly, "E) All three combined"),
]

print(f"{'Strategy':35s} | {'Final':>12s} | {'Trades':>6s} | {'WR':>5s} | {'P&L':>10s}")
print("-" * 80)

for func, label in strategies:
    r = run_backtest(bt_data, func, label)
    pnl = r['final'] - 300
    print(f"{label:35s} | ${r['final']:>10,.2f} | {r['trades']:>6d} | {r['wr']:>4.1f}% | ${pnl:>+9,.2f}")

print()
print("=" * 80)
