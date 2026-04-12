import pandas as pd
import numpy as np
import glob

print("Loading data...")

# Load all markets
markets_files = sorted(glob.glob('data/trevorjs/markets-*.parquet'))
markets = pd.concat([pd.read_parquet(f) for f in markets_files], ignore_index=True)

# Load all trades
trades_files = sorted(glob.glob('data/trevorjs/trades-*.parquet'))
trades = pd.concat([pd.read_parquet(f) for f in trades_files], ignore_index=True)

print(f"Markets: {len(markets):,} | Trades: {len(trades):,}")

# Filter markets: finalized with yes/no result
markets_fin = markets[
    (markets['status'] == 'finalized') &
    (markets['result'].isin(['yes', 'no']))
][['ticker', 'result']].drop_duplicates('ticker')

print(f"Finalized yes/no markets: {len(markets_fin):,}")

# Filter trades: from 2025-01-01 onward
cutoff = pd.Timestamp('2025-01-01', tz='UTC')
trades_recent = trades[trades['created_time'] >= cutoff].copy()
print(f"Trades from 2025-01-01: {len(trades_recent):,}")

# Get first trade per ticker (rn=1)
trades_recent = trades_recent.sort_values('created_time')
first_trades = trades_recent.drop_duplicates(subset='ticker', keep='first').copy()
print(f"First-trade-per-ticker: {len(first_trades):,}")

# Merge with results
first_trades = first_trades.merge(markets_fin, on='ticker', how='inner')
print(f"After merging with finalized results: {len(first_trades):,}")

# yes_price is in cents (0-99 integer scale)
first_trades['yes_price'] = pd.to_numeric(first_trades['yes_price'], errors='coerce')
first_trades = first_trades.dropna(subset=['yes_price'])

# NO cost = (100 - yes_price) / 100
first_trades['no_cost'] = (100 - first_trades['yes_price']) / 100

# Sport pattern definitions
sport_patterns = {
    "NBA":       ["KXNBAGAME"],
    "NHL":       ["KXNHLGAME"],
    "EPL":       ["KXEPLGAME"],
    "UCL":       ["KXUCLGAME"],
    "LALIGA":    ["KXLALIGAGAME"],
    "WNBA":      ["KXWNBAGAME"],
    "UFC":       ["KXUFCFIGHT"],
    "NCAAMB":    ["KXNCAAMBGAME"],
    "NCAAWB":    ["KXNCAAWBGAME"],
    "WEATHER":   ["KXHIGHNY","KXHIGHCHI","KXHIGHMIA","KXHIGHLA",
                  "KXHIGHSF","KXHIGHHOU","KXHIGHDEN","KXHIGHDC",
                  "KXHIGHDAL","KXHIGHAUS","KXHIGHPHIL"],
    "NFLTD":     ["KXNFLANYTD"],
    "NHLSPREAD": ["KXNHLSPREAD"],
    "NHLFG":     ["KXNHLFIRSTGOAL"],
    "NBASPREAD": ["KXNBASPREAD"],
    "NFLSPREAD": ["KXNFLSPREAD"],
}

def match_patterns(ticker, prefixes):
    for p in prefixes:
        if ticker.startswith(p):
            return True
    return False

def compute_pnl_vec(no_cost_series, result_series):
    fee = 0.07 * no_cost_series * (1 - no_cost_series)
    is_no = (result_series == 'no')
    pnl = np.where(is_no, (1.0 - no_cost_series) - fee, -(no_cost_series + fee))
    return pnl

def compute_max_drawdown(pnl_array):
    bankroll = 1000.0
    peak = 1000.0
    max_dd = 0.0
    for p in pnl_array:
        bankroll += p
        if bankroll > peak:
            peak = bankroll
        if peak > 0:
            dd = (peak - bankroll) / peak * 100
            if dd > max_dd:
                max_dd = dd
    return max_dd

def compute_longest_losing_streak(result_series):
    losing_streak = 0
    max_losing_streak = 0
    for r in result_series:
        if r == 'yes':
            losing_streak += 1
            if losing_streak > max_losing_streak:
                max_losing_streak = losing_streak
        else:
            losing_streak = 0
    return max_losing_streak

def analyze_sport(df):
    if len(df) == 0:
        return None

    df = df.sort_values('created_time').copy()
    df['pnl'] = compute_pnl_vec(df['no_cost'].values, df['result'].values)

    total_trades = len(df)
    no_win_rate = (df['result'] == 'no').mean()
    total_pnl = df['pnl'].sum()
    total_wagered = df['no_cost'].sum()
    roi_pct = (total_pnl / total_wagered * 100) if total_wagered > 0 else 0

    max_dd = compute_max_drawdown(df['pnl'].values)
    max_losing_streak = compute_longest_losing_streak(df['result'].values)

    mean_pnl = df['pnl'].mean()
    std_pnl = df['pnl'].std()
    sharpe = mean_pnl / std_pnl if std_pnl > 0 else 0

    return {
        'total_trades': total_trades,
        'no_win_rate': no_win_rate,
        'total_pnl': total_pnl,
        'total_wagered': total_wagered,
        'roi_pct': roi_pct,
        'max_drawdown_pct': max_dd,
        'longest_losing_streak': max_losing_streak,
        'sharpe': sharpe,
        'df': df
    }

def bucket_breakdown(df):
    buckets = range(55, 96, 5)
    rows = []
    for b in buckets:
        lo, hi = b, b + 5
        mask = (df['yes_price'] >= lo) & (df['yes_price'] < hi)
        sub = df[mask].sort_values('created_time')
        if len(sub) == 0:
            continue
        count = len(sub)
        win_rate = (sub['result'] == 'no').mean()
        wag = sub['no_cost'].sum()
        roi = sub['pnl'].sum() / wag * 100 if wag > 0 else 0
        mdd = compute_max_drawdown(sub['pnl'].values)
        rows.append({
            'bucket': f"{lo}-{hi}c",
            'count': count,
            'no_win_rate': win_rate,
            'roi_pct': roi,
            'max_drawdown_pct': mdd
        })
    return rows

# Run analysis for each sport
results = {}
print("\nAnalyzing sports...")

for sport, prefixes in sport_patterns.items():
    mask = first_trades['ticker'].apply(lambda t: match_patterns(t, prefixes))
    sport_df = first_trades[mask].copy()

    # Fade favorites: only where yes_price >= 55
    sport_df = sport_df[sport_df['yes_price'] >= 55]

    print(f"  {sport}: {len(sport_df)} trades (yes_price >= 55)")

    res = analyze_sport(sport_df)
    if res:
        results[sport] = res

# Print summary sorted by Sharpe
print()
print("=" * 100)
print("FADE FAVORITES BACKTEST SUMMARY -- BUY NO CONTRACTS (YES PRICE >= 55c)")
print("Strategy: First trade per ticker, 2025-01-01+, finalized markets, $1 flat bet sizing")
print("=" * 100)

header = (
    f"{'Sport':<12} {'Trades':>7} {'NO Win%':>8} {'Total P&L':>10} "
    f"{'ROI%':>8} {'MaxDD%':>8} {'LongLoss':>9} {'Sharpe':>8}"
)
print(header)
print("-" * 100)

sorted_sports = sorted(results.keys(), key=lambda s: results[s]['sharpe'], reverse=True)

for sport in sorted_sports:
    r = results[sport]
    print(
        f"{sport:<12} {r['total_trades']:>7} {r['no_win_rate']:>7.1%} "
        f"{r['total_pnl']:>+10.2f} {r['roi_pct']:>+7.1f}% "
        f"{r['max_drawdown_pct']:>7.1f}% {r['longest_losing_streak']:>9} "
        f"{r['sharpe']:>8.4f}"
    )

# Per-sport bucket breakdown
print()
print("=" * 100)
print("PER-PRICE-BUCKET BREAKDOWN (YES price buckets, 5c wide, sorted by ROI%)")
print("=" * 100)

for sport in sorted_sports:
    r = results[sport]
    df = r['df']
    if len(df) == 0:
        continue

    buckets = bucket_breakdown(df)
    if not buckets:
        continue

    print()
    print("-" * 70)
    print(
        f"  {sport} -- Sharpe: {r['sharpe']:.4f} | "
        f"Total P&L: {r['total_pnl']:+.2f} | "
        f"ROI: {r['roi_pct']:+.1f}% | "
        f"MaxDD: {r['max_drawdown_pct']:.1f}%"
    )
    print("-" * 70)
    print(f"  {'Bucket':<10} {'Count':>6} {'NO Win%':>8} {'ROI%':>8} {'MaxDD%':>8}")
    print(f"  {'-' * 46}")

    for b in sorted(buckets, key=lambda x: x['roi_pct'], reverse=True):
        print(
            f"  {b['bucket']:<10} {b['count']:>6} {b['no_win_rate']:>7.1%} "
            f"{b['roi_pct']:>+7.1f}% {b['max_drawdown_pct']:>7.1f}%"
        )

print()
print("=" * 100)
print("NOTES:")
print("  - Fee: 0.07 * no_cost * (1 - no_cost)  [Kalshi maker-taker approx]")
print("  - Drawdown on running $1000 bankroll, $1 flat per trade, sequential order")
print("  - Sharpe = mean_pnl / std_pnl per trade (higher = smoother returns)")
print("  - Only first trade per ticker (opening price signal), YES >= 55c only")
print("=" * 100)
