"""
Fade Favorites backtest: buy NO contracts on heavy favorites (YES price >= 55c).
Memory-efficient: scans trades files in chunks, only keeps first trade per ticker.
"""
import pandas as pd
import numpy as np
import glob

print("Loading markets (finalized yes/no only)...")
markets_files = sorted(glob.glob('data/trevorjs/markets-*.parquet'))
markets = pd.concat([
    pd.read_parquet(f, columns=['ticker', 'result', 'status'])
    for f in markets_files
], ignore_index=True)

markets_fin = markets[
    (markets['status'] == 'finalized') &
    (markets['result'].isin(['yes', 'no']))
][['ticker', 'result']].drop_duplicates('ticker')

print(f"Finalized yes/no markets: {len(markets_fin):,}")
del markets

# Build lookup dict for fast O(1) result lookups
result_lookup = dict(zip(markets_fin['ticker'], markets_fin['result']))
del markets_fin

# Sport prefix definitions
sport_prefixes = {
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

# All prefixes flat set for fast pre-filter
all_prefixes = [p for prefixes in sport_prefixes.values() for p in prefixes]

def get_sport(ticker):
    for sport, prefixes in sport_prefixes.items():
        for p in prefixes:
            if ticker.startswith(p):
                return sport
    return None

def ticker_in_scope(ticker):
    for p in all_prefixes:
        if ticker.startswith(p):
            return True
    return False

cutoff = pd.Timestamp('2025-01-01', tz='UTC')

print("\nScanning trades files for first trade per ticker (2025-01-01+)...")
print("(Only sport tickers with YES >= 55 and finalized result)")

# We need global first trades: collect all candidates then deduplicate
# Strategy: process each file, filter to scope + date, sort by time, keep track
# of earliest seen per ticker across all files

first_trades_per_ticker = {}  # ticker -> (created_time, yes_price)

trades_files = sorted(glob.glob('data/trevorjs/trades-*.parquet'))

for i, f in enumerate(trades_files):
    print(f"  File {i+1}/{len(trades_files)}: {f.split('/')[-1]}", end='', flush=True)
    t = pd.read_parquet(f, columns=['ticker', 'yes_price', 'created_time'])

    # Filter to 2025+
    t = t[t['created_time'] >= cutoff]
    if len(t) == 0:
        print(" -> 0 rows after date filter")
        continue

    # Filter to sport tickers only
    t = t[t['ticker'].apply(ticker_in_scope)]
    if len(t) == 0:
        print(" -> 0 rows after ticker filter")
        continue

    # Filter to tickers with finalized result
    t = t[t['ticker'].isin(result_lookup)]
    if len(t) == 0:
        print(" -> 0 rows after result filter")
        continue

    # Filter to YES >= 55 (fade favorites only)
    t = t[pd.to_numeric(t['yes_price'], errors='coerce') >= 55]
    if len(t) == 0:
        print(" -> 0 rows after price filter")
        continue

    print(f" -> {len(t):,} candidate rows", end='')

    # For each ticker, find the earliest trade in this file
    earliest = t.sort_values('created_time').drop_duplicates('ticker', keep='first')

    # Update global first_trades dict
    updates = 0
    for row in earliest.itertuples(index=False):
        ticker = row.ticker
        ct = row.created_time
        yp = row.yes_price
        if ticker not in first_trades_per_ticker or ct < first_trades_per_ticker[ticker][0]:
            first_trades_per_ticker[ticker] = (ct, yp)
            updates += 1

    print(f" | {updates} ticker updates")

print(f"\nTotal unique sport tickers with first trade: {len(first_trades_per_ticker):,}")

# Build the final DataFrame
rows = []
for ticker, (ct, yp) in first_trades_per_ticker.items():
    result = result_lookup.get(ticker, None)
    sport = get_sport(ticker)
    if result and sport:
        rows.append({
            'ticker': ticker,
            'created_time': ct,
            'yes_price': float(yp),
            'result': result,
            'sport': sport
        })

first_trades_df = pd.DataFrame(rows)
first_trades_df['no_cost'] = (100 - first_trades_df['yes_price']) / 100

print(f"Final dataset: {len(first_trades_df):,} trades across {first_trades_df['sport'].nunique()} sports")
print(f"Date range: {first_trades_df['created_time'].min().date()} to {first_trades_df['created_time'].max().date()}")

# ---- Analysis functions ----

def compute_pnl_vec(no_cost_arr, result_arr):
    no_cost_arr = np.asarray(no_cost_arr, dtype=float)
    fee = 0.07 * no_cost_arr * (1 - no_cost_arr)
    is_no = np.array([r == 'no' for r in result_arr])
    pnl = np.where(is_no, (1.0 - no_cost_arr) - fee, -(no_cost_arr + fee))
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

def compute_longest_losing_streak(result_arr):
    streak = 0
    max_streak = 0
    for r in result_arr:
        if r == 'yes':
            streak += 1
            if streak > max_streak:
                max_streak = streak
        else:
            streak = 0
    return max_streak

def analyze_sport_df(df):
    df = df.sort_values('created_time').copy()
    pnl = compute_pnl_vec(df['no_cost'].values, df['result'].values)
    df['pnl'] = pnl

    total_trades = len(df)
    no_win_rate = (df['result'] == 'no').mean()
    total_pnl = float(pnl.sum())
    total_wagered = float(df['no_cost'].sum())
    roi_pct = (total_pnl / total_wagered * 100) if total_wagered > 0 else 0.0

    max_dd = compute_max_drawdown(pnl)
    max_losing_streak = compute_longest_losing_streak(df['result'].values)

    mean_pnl = float(pnl.mean())
    std_pnl = float(pnl.std())
    sharpe = mean_pnl / std_pnl if std_pnl > 0 else 0.0

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

# ---- Run per-sport analysis ----
results = {}
print("\nRunning per-sport analysis...")

for sport in sport_prefixes:
    sdf = first_trades_df[first_trades_df['sport'] == sport]
    print(f"  {sport}: {len(sdf)} trades")
    if len(sdf) > 0:
        results[sport] = analyze_sport_df(sdf)

# ---- Print results ----
print()
print("=" * 105)
print("FADE FAVORITES BACKTEST -- BUY NO CONTRACTS (YES PRICE >= 55c)")
print("First trade per ticker | 2025-01-01+ | Finalized markets | $1 flat per trade (fee-inclusive)")
print("=" * 105)

header = (
    f"{'Sport':<12} {'Trades':>7} {'NO Win%':>8} {'Total P&L':>10} "
    f"{'ROI%':>8} {'MaxDD%':>8} {'LongLoss':>9} {'Sharpe':>8}"
)
print(header)
print("-" * 105)

sorted_sports = sorted(results.keys(), key=lambda s: results[s]['sharpe'], reverse=True)

for sport in sorted_sports:
    r = results[sport]
    print(
        f"{sport:<12} {r['total_trades']:>7,} {r['no_win_rate']:>7.1%} "
        f"{r['total_pnl']:>+10.2f} {r['roi_pct']:>+7.1f}% "
        f"{r['max_drawdown_pct']:>7.1f}% {r['longest_losing_streak']:>9} "
        f"{r['sharpe']:>8.4f}"
    )

# ---- Per-sport bucket breakdown ----
print()
print("=" * 105)
print("PER-PRICE-BUCKET BREAKDOWN (YES price buckets, 5c wide)")
print("Sorted by ROI% within each sport | Only buckets with >= 1 trade shown")
print("=" * 105)

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
        f"P&L: {r['total_pnl']:+.2f} | "
        f"ROI: {r['roi_pct']:+.1f}% | "
        f"MaxDD: {r['max_drawdown_pct']:.1f}%"
    )
    print("-" * 70)
    print(f"  {'Bucket':<10} {'Count':>6} {'NO Win%':>8} {'ROI%':>8} {'MaxDD%':>8}")
    print(f"  {'-' * 46}")

    for b in sorted(buckets, key=lambda x: x['roi_pct'], reverse=True):
        flag = " <--" if b['roi_pct'] > 5 else ""
        print(
            f"  {b['bucket']:<10} {b['count']:>6} {b['no_win_rate']:>7.1%} "
            f"{b['roi_pct']:>+7.1f}% {b['max_drawdown_pct']:>7.1f}%{flag}"
        )

print()
print("=" * 105)
print("NOTES:")
print("  Fee formula: 0.07 * no_cost * (1 - no_cost)")
print("  Drawdown: running $1000 bankroll, $1 flat size, trades sorted by created_time")
print("  Sharpe = mean(pnl) / std(pnl) per trade [higher = smoother returns]")
print("  First trade = earliest created_time trade per ticker across all parquet files")
print("  <- flags buckets with ROI > 5%")
print("=" * 105)
