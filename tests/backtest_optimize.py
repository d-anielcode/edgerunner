"""
EdgeRunner v2 Optimization Backtest.

Tests multiple parameter combinations and additional filters
to find the optimal configuration that maximizes profit while
minimizing drawdown.
"""

import duckdb
from collections import defaultdict

con = duckdb.connect()
kp = "data/dataset/data/data/kalshi"

# Load data once
print("Loading data...")
markets = con.sql(f"""
    SELECT ticker, event_ticker, result, title, volume
    FROM '{kp}/markets/*.parquet'
    WHERE event_ticker LIKE 'KXNBAGAME%' AND status = 'finalized' AND result IN ('yes', 'no')
""").fetchdf()
con.register("gw_markets", markets)

trades_df = con.sql(f"""
    WITH ft AS (
        SELECT t.ticker, t.yes_price, t.no_price, t.created_time, t.taker_side, t.count as size,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
        FROM '{kp}/trades/*.parquet' t
        WHERE t.ticker IN (SELECT ticker FROM gw_markets)
    ),
    o AS (
        SELECT ft.*, m.result, m.volume,
               CAST(ft.created_time AS DATE) as trade_date
        FROM ft JOIN gw_markets m ON ft.ticker = m.ticker
        WHERE ft.rn = 1
    )
    SELECT * FROM o ORDER BY created_time
""").fetchdf()

print(f"Loaded {len(trades_df)} game winner markets with opening trades")

HIT_RATES = {
    (61, 75): 0.593,
    (76, 95): 0.758,
}


def run_backtest(
    df,
    starting_bankroll=100.0,
    min_yes=60,
    max_yes=95,
    kelly_fraction=0.35,
    max_position_pct=0.15,
    max_daily_trades=5,
    min_edge=0.05,
    volume_filter=0,
    label="",
):
    """Run a single backtest with given parameters."""
    bankroll = starting_bankroll
    peak = starting_bankroll
    max_dd = 0.0
    trades = 0
    wins = 0
    daily_count = 0
    last_date = None
    consecutive_losses = 0
    max_streak = 0
    min_bankroll = starting_bankroll

    for _, row in df.iterrows():
        d = str(row["trade_date"])
        if d != last_date:
            daily_count = 0
            last_date = d
        if daily_count >= max_daily_trades or bankroll < 5:
            continue

        yes_p = int(row["yes_price"])
        no_p = int(row["no_price"])
        vol = int(row["volume"])

        if yes_p < min_yes or yes_p > max_yes:
            continue
        if vol < volume_filter:
            continue

        # Edge calculation
        actual_yes = 0.65
        for (lo, hi), rate in HIT_RATES.items():
            if lo <= yes_p <= hi:
                actual_yes = rate
                break

        edge = (yes_p / 100.0) - actual_yes
        if edge < min_edge:
            continue

        no_cost = no_p / 100.0
        yes_cost = yes_p / 100.0

        # Kelly
        b = yes_cost / no_cost if no_cost > 0 else 0
        p = 1 - actual_yes
        q = actual_yes
        kr = (b * p - q) / b if b > 0 else 0
        ka = max(0, min(kr * kelly_fraction, max_position_pct))

        pos_size = bankroll * ka
        contracts = max(1, int(pos_size / no_cost)) if no_cost > 0 else 0
        if contracts == 0:
            continue
        cost = contracts * no_cost
        if cost > bankroll:
            contracts = max(1, int(bankroll / no_cost))
            cost = contracts * no_cost
        if cost > bankroll:
            continue

        fee = 0.07 * no_cost * (1 - no_cost) * contracts

        if row["result"] == "no":
            bankroll += contracts * (1.0 - no_cost) - fee
            wins += 1
            consecutive_losses = 0
        else:
            bankroll -= cost + fee
            consecutive_losses += 1
            if consecutive_losses > max_streak:
                max_streak = consecutive_losses

        trades += 1
        daily_count += 1
        if bankroll > peak:
            peak = bankroll
        if bankroll < min_bankroll:
            min_bankroll = bankroll
        dd = (peak - bankroll) / peak if peak > 0 else 0
        if dd > max_dd:
            max_dd = dd

    pnl = bankroll - starting_bankroll
    wr = wins / max(trades, 1) * 100
    return {
        "label": label,
        "trades": trades,
        "wins": wins,
        "win_rate": round(wr, 1),
        "final": round(bankroll, 2),
        "pnl": round(pnl, 2),
        "roi": round(pnl / starting_bankroll * 100, 1),
        "peak": round(peak, 2),
        "min": round(min_bankroll, 2),
        "max_dd": round(max_dd * 100, 1),
        "max_streak": max_streak,
    }


# ============================================================
# TEST 1: Kelly fraction sweep
# ============================================================
print("\n" + "=" * 80)
print("TEST 1: KELLY FRACTION SWEEP")
print("=" * 80)
print(f"{'Kelly':>8s} | {'Trades':>6s} | {'Win%':>5s} | {'Final':>8s} | {'P&L':>8s} | {'ROI':>6s} | {'MaxDD':>6s} | {'Streak':>6s}")
print("-" * 75)

for kelly in [0.10, 0.15, 0.20, 0.25, 0.30, 0.35, 0.40, 0.50]:
    r = run_backtest(trades_df, kelly_fraction=kelly, label=f"Kelly {kelly}")
    print(f"  {kelly:.2f}  | {r['trades']:>6d} | {r['win_rate']:>4.1f}% | ${r['final']:>7.2f} | ${r['pnl']:>+7.2f} | {r['roi']:>+5.1f}% | {r['max_dd']:>5.1f}% | {r['max_streak']:>6d}")

# ============================================================
# TEST 2: Max position size sweep
# ============================================================
print("\n" + "=" * 80)
print("TEST 2: MAX POSITION SIZE SWEEP")
print("=" * 80)
print(f"{'MaxPos':>8s} | {'Trades':>6s} | {'Final':>8s} | {'P&L':>8s} | {'ROI':>6s} | {'MaxDD':>6s}")
print("-" * 60)

for pos in [0.05, 0.08, 0.10, 0.12, 0.15, 0.20, 0.25]:
    r = run_backtest(trades_df, max_position_pct=pos, label=f"MaxPos {pos}")
    print(f"  {pos:.2f}  | {r['trades']:>6d} | ${r['final']:>7.2f} | ${r['pnl']:>+7.2f} | {r['roi']:>+5.1f}% | {r['max_dd']:>5.1f}%")

# ============================================================
# TEST 3: YES price range sweep
# ============================================================
print("\n" + "=" * 80)
print("TEST 3: YES PRICE RANGE")
print("=" * 80)
print(f"{'Range':>12s} | {'Trades':>6s} | {'Win%':>5s} | {'Final':>8s} | {'ROI':>6s} | {'MaxDD':>6s}")
print("-" * 60)

for (lo, hi) in [(60, 95), (60, 80), (60, 70), (65, 85), (65, 95), (70, 95), (70, 85), (75, 95)]:
    r = run_backtest(trades_df, min_yes=lo, max_yes=hi, label=f"{lo}-{hi}c")
    print(f"  {lo:>3d}-{hi}c | {r['trades']:>6d} | {r['win_rate']:>4.1f}% | ${r['final']:>7.2f} | {r['roi']:>+5.1f}% | {r['max_dd']:>5.1f}%")

# ============================================================
# TEST 4: Daily trade limit
# ============================================================
print("\n" + "=" * 80)
print("TEST 4: MAX DAILY TRADES")
print("=" * 80)
print(f"{'MaxDaily':>8s} | {'Trades':>6s} | {'Final':>8s} | {'ROI':>6s} | {'MaxDD':>6s}")
print("-" * 50)

for dt in [1, 2, 3, 5, 8, 10, 20]:
    r = run_backtest(trades_df, max_daily_trades=dt)
    print(f"  {dt:>5d}  | {r['trades']:>6d} | ${r['final']:>7.2f} | {r['roi']:>+5.1f}% | {r['max_dd']:>5.1f}%")

# ============================================================
# TEST 5: Minimum edge threshold
# ============================================================
print("\n" + "=" * 80)
print("TEST 5: MINIMUM EDGE THRESHOLD")
print("=" * 80)
print(f"{'MinEdge':>8s} | {'Trades':>6s} | {'Win%':>5s} | {'Final':>8s} | {'ROI':>6s} | {'MaxDD':>6s}")
print("-" * 60)

for edge in [0.03, 0.05, 0.08, 0.10, 0.12, 0.15]:
    r = run_backtest(trades_df, min_edge=edge)
    print(f"  {edge:.2f}  | {r['trades']:>6d} | {r['win_rate']:>4.1f}% | ${r['final']:>7.2f} | {r['roi']:>+5.1f}% | {r['max_dd']:>5.1f}%")

# ============================================================
# TEST 6: Volume filter
# ============================================================
print("\n" + "=" * 80)
print("TEST 6: MINIMUM VOLUME FILTER")
print("=" * 80)
print(f"{'MinVol':>8s} | {'Trades':>6s} | {'Win%':>5s} | {'Final':>8s} | {'ROI':>6s} | {'MaxDD':>6s}")
print("-" * 60)

for vol in [0, 500, 1000, 5000, 10000, 50000, 100000]:
    r = run_backtest(trades_df, volume_filter=vol)
    print(f"  {vol:>6d} | {r['trades']:>6d} | {r['win_rate']:>4.1f}% | ${r['final']:>7.2f} | {r['roi']:>+5.1f}% | {r['max_dd']:>5.1f}%")

# ============================================================
# TEST 7: OPTIMAL COMBINATION
# ============================================================
print("\n" + "=" * 80)
print("TEST 7: FINDING OPTIMAL COMBINATION")
print("=" * 80)

best_roi = -999
best_params = {}
best_sharpe = -999

results = []

for kelly in [0.15, 0.20, 0.25, 0.30, 0.35]:
    for pos in [0.08, 0.10, 0.12, 0.15]:
        for (lo, hi) in [(60, 95), (65, 90), (65, 85), (70, 95)]:
            for edge in [0.05, 0.08, 0.10]:
                for dt in [3, 5, 8]:
                    r = run_backtest(
                        trades_df,
                        kelly_fraction=kelly,
                        max_position_pct=pos,
                        min_yes=lo,
                        max_yes=hi,
                        min_edge=edge,
                        max_daily_trades=dt,
                    )
                    if r["trades"] >= 30:  # Need minimum sample
                        # Score: ROI adjusted for drawdown
                        score = r["roi"] - r["max_dd"] * 1.5  # Penalize drawdown
                        results.append({
                            **r,
                            "kelly": kelly,
                            "max_pos": pos,
                            "yes_range": f"{lo}-{hi}",
                            "min_edge": edge,
                            "max_daily": dt,
                            "score": round(score, 1),
                        })

# Sort by score
results.sort(key=lambda x: x["score"], reverse=True)

print(f"\nTop 10 configurations (scored by ROI - 1.5*MaxDD):")
print(f"{'Kelly':>6s} | {'Pos':>4s} | {'Range':>7s} | {'Edge':>5s} | {'Daily':>5s} | {'Trades':>6s} | {'ROI':>7s} | {'MaxDD':>6s} | {'Score':>6s}")
print("-" * 75)

for r in results[:10]:
    print(
        f"  {r['kelly']:.2f} | {r['max_pos']:.2f} | {r['yes_range']:>7s} | "
        f"{r['min_edge']:.2f} | {r['max_daily']:>5d} | {r['trades']:>6d} | "
        f"{r['roi']:>+6.1f}% | {r['max_dd']:>5.1f}% | {r['score']:>+5.1f}"
    )

# Show the winner
winner = results[0]
print(f"\n{'='*80}")
print(f"OPTIMAL CONFIGURATION")
print(f"{'='*80}")
print(f"Kelly fraction: {winner['kelly']}")
print(f"Max position: {winner['max_pos']*100:.0f}%")
print(f"YES range: {winner['yes_range']}c")
print(f"Min edge: {winner['min_edge']*100:.0f}%")
print(f"Max daily trades: {winner['max_daily']}")
print(f"")
print(f"Trades: {winner['trades']}")
print(f"Win rate: {winner['win_rate']}%")
print(f"Final bankroll: ${winner['final']}")
print(f"ROI: {winner['roi']:+.1f}%")
print(f"Max drawdown: {winner['max_dd']:.1f}%")
print(f"Score (ROI - 1.5*DD): {winner['score']:+.1f}")

# Also show best pure ROI (for comparison)
best_roi_config = max(results, key=lambda x: x["roi"])
print(f"\nHighest pure ROI: {best_roi_config['roi']:+.1f}% (but {best_roi_config['max_dd']:.1f}% drawdown)")
print(f"  Config: Kelly={best_roi_config['kelly']}, Pos={best_roi_config['max_pos']}, Range={best_roi_config['yes_range']}, Edge={best_roi_config['min_edge']}, Daily={best_roi_config['max_daily']}")
