"""
Exit Strategy Backtest on Historical Kalshi Game-Winner Markets
================================================================
Simulates different exit strategies for NO positions on game-winner markets
where YES price >= 60c at entry (i.e., we're fading the favorite).

Strategies:
  A: Hold to settlement
  B: Profit-take at 50%
  C: Profit-take at 100%
  D: Profit-take at 200%
  E: Trailing stop (25% from peak)
  F: Time-based exit (80% through event)
"""

import duckdb
import math
import numpy as np
import pandas as pd
from collections import defaultdict

DATA_DIR = "C:/Users/dcho0/Documents/edgerunner/data/trevorjs"

# ── Fee calculation (Kalshi) ─────────────────────────────────────────────
def kalshi_fee(price_cents: int) -> float:
    """Fee in dollars. price_cents is the YES or NO price in cents (1-99)."""
    p = price_cents / 100.0
    return math.ceil(0.07 * p * (1 - p) * 100) / 100


# ── Load data with duckdb ────────────────────────────────────────────────
def load_data():
    con = duckdb.connect()

    # Get qualifying markets: game-winner, finalized, YES price >= 60 on first trade, from 2025+
    markets = con.execute(f"""
        SELECT ticker, result, created_time as market_created, close_time
        FROM '{DATA_DIR}/markets-*.parquet'
        WHERE (ticker LIKE 'KXNBAGAME%' OR ticker LIKE 'KXNHLGAME%'
               OR ticker LIKE 'KXEPLGAME%' OR ticker LIKE 'KXNCAAMBGAME%')
          AND status = 'finalized'
          AND result IN ('yes', 'no')
          AND created_time >= '2025-01-01'
    """).fetchdf()

    # Get all trades for these tickers
    ticker_list = markets['ticker'].tolist()
    if not ticker_list:
        print("No qualifying markets found!")
        return None, None

    trades = con.execute(f"""
        SELECT t.ticker, t.yes_price, t.created_time
        FROM '{DATA_DIR}/trades-*.parquet' t
        WHERE t.ticker IN ({','.join(f"'{t}'" for t in ticker_list)})
        ORDER BY t.ticker, t.created_time ASC
    """).fetchdf()

    con.close()
    return markets, trades


# ── Simulate strategies for one market ───────────────────────────────────
def simulate_market(ticker, result, trades_df):
    """
    trades_df: DataFrame of trades for this ticker, sorted by created_time ASC.
    result: 'yes' or 'no' — the market outcome (YES wins or NO wins).

    We buy NO. If result='no', our NO position settles at $1.
    If result='yes', our NO position settles at $0.
    """
    if len(trades_df) < 50:
        return None

    yes_prices = trades_df['yes_price'].values  # cents
    timestamps = trades_df['created_time'].values

    # Entry: first trade's YES price — we buy NO at (100 - yes_price)
    entry_yes = int(yes_prices[0])
    if entry_yes < 60:
        return None  # Skip: we only buy NO when YES >= 60c (favorite)

    entry_no_cents = 100 - entry_yes  # our cost in cents
    entry_no = entry_no_cents / 100.0  # dollars

    # Settlement outcome for our NO position
    no_wins = (result == 'no')  # if YES loses, our NO wins

    # Track NO value trajectory (cents)
    no_values = 100 - yes_prices  # NO value at each trade point

    # Market lifetime
    t_start = timestamps[0]
    t_end = timestamps[-1]
    market_duration = (pd.Timestamp(t_end) - pd.Timestamp(t_start)).total_seconds()
    if market_duration <= 0:
        market_duration = 1  # avoid div by zero

    # Fees
    buy_fee = kalshi_fee(entry_no_cents)

    results = {}

    # ── Strategy A: Hold to Settlement ──
    if no_wins:
        settle_fee = kalshi_fee(100)  # settling at 100c (win)
        pnl_a = 1.0 - entry_no - buy_fee - settle_fee
    else:
        # NO settles at 0, we lose our entry cost + buy fee
        settle_fee = kalshi_fee(0)  # 0 fee on 0 or 100
        pnl_a = -entry_no - buy_fee
    results['A'] = {
        'pnl': pnl_a,
        'hold_pct': 1.0,
        'exited_early': False,
        'exit_price': 1.0 if no_wins else 0.0,
    }

    # ── Helper: early exit P&L ──
    def early_exit_pnl(exit_no_cents):
        exit_no = exit_no_cents / 100.0
        sell_fee = kalshi_fee(int(exit_no_cents))
        return exit_no - entry_no - buy_fee - sell_fee

    def hold_time_pct(exit_idx):
        t_exit = timestamps[exit_idx]
        elapsed = (pd.Timestamp(t_exit) - pd.Timestamp(t_start)).total_seconds()
        return elapsed / market_duration

    # ── Strategy B: Profit-Take at 50% ──
    target_b = entry_no_cents * 1.5
    exited_b = False
    for i in range(1, len(no_values)):
        if no_values[i] >= target_b:
            results['B'] = {
                'pnl': early_exit_pnl(no_values[i]),
                'hold_pct': hold_time_pct(i),
                'exited_early': True,
                'exit_price': no_values[i] / 100.0,
            }
            exited_b = True
            break
    if not exited_b:
        results['B'] = results['A'].copy()

    # ── Strategy C: Profit-Take at 100% ──
    target_c = entry_no_cents * 2.0
    exited_c = False
    for i in range(1, len(no_values)):
        if no_values[i] >= target_c:
            results['C'] = {
                'pnl': early_exit_pnl(no_values[i]),
                'hold_pct': hold_time_pct(i),
                'exited_early': True,
                'exit_price': no_values[i] / 100.0,
            }
            exited_c = True
            break
    if not exited_c:
        results['C'] = results['A'].copy()

    # ── Strategy D: Profit-Take at 200% ──
    target_d = entry_no_cents * 3.0
    exited_d = False
    for i in range(1, len(no_values)):
        if no_values[i] >= target_d:
            results['D'] = {
                'pnl': early_exit_pnl(no_values[i]),
                'hold_pct': hold_time_pct(i),
                'exited_early': True,
                'exit_price': no_values[i] / 100.0,
            }
            exited_d = True
            break
    if not exited_d:
        results['D'] = results['A'].copy()

    # ── Strategy E: Trailing Stop (25% from peak) ──
    peak_no = no_values[0]
    exited_e = False
    for i in range(1, len(no_values)):
        if no_values[i] > peak_no:
            peak_no = no_values[i]
        stop_level = peak_no * 0.75
        if no_values[i] <= stop_level and peak_no > entry_no_cents:
            # Only trigger if we've been above entry (otherwise just hold)
            results['E'] = {
                'pnl': early_exit_pnl(no_values[i]),
                'hold_pct': hold_time_pct(i),
                'exited_early': True,
                'exit_price': no_values[i] / 100.0,
            }
            exited_e = True
            break
    if not exited_e:
        results['E'] = results['A'].copy()

    # ── Strategy F: Time-Based Exit (80% through) ──
    target_time = pd.Timestamp(t_start) + pd.Timedelta(seconds=market_duration * 0.8)
    exited_f = False
    for i in range(1, len(no_values)):
        if pd.Timestamp(timestamps[i]) >= target_time:
            results['F'] = {
                'pnl': early_exit_pnl(no_values[i]),
                'hold_pct': hold_time_pct(i),
                'exited_early': True,
                'exit_price': no_values[i] / 100.0,
            }
            exited_f = True
            break
    if not exited_f:
        results['F'] = results['A'].copy()

    return results


# ── Main ─────────────────────────────────────────────────────────────────
def main():
    print("Loading data...")
    markets, trades = load_data()
    if markets is None:
        return

    print(f"Markets: {len(markets)}, Trade rows: {len(trades)}")

    # Group trades by ticker
    trades_grouped = {ticker: group for ticker, group in trades.groupby('ticker')}

    # Sport classification
    def get_sport(ticker):
        if 'NBAGAME' in ticker: return 'NBA'
        if 'NHLGAME' in ticker: return 'NHL'
        if 'EPLGAME' in ticker: return 'EPL'
        if 'NCAAMBGAME' in ticker: return 'NCAAMB'
        return 'OTHER'

    strategies = ['A', 'B', 'C', 'D', 'E', 'F']
    strategy_names = {
        'A': 'Hold to Settlement',
        'B': 'Profit-Take 50%',
        'C': 'Profit-Take 100%',
        'D': 'Profit-Take 200%',
        'E': 'Trailing Stop 25%',
        'F': 'Time Exit 80%',
    }

    # Collect results by sport and overall
    all_results = defaultdict(lambda: {s: [] for s in strategies})  # sport -> strategy -> list of dicts

    skipped = 0
    processed = 0

    for _, mkt in markets.iterrows():
        ticker = mkt['ticker']
        result = mkt['result']

        if ticker not in trades_grouped:
            skipped += 1
            continue

        tdf = trades_grouped[ticker].sort_values('created_time').reset_index(drop=True)
        res = simulate_market(ticker, result, tdf)

        if res is None:
            skipped += 1
            continue

        sport = get_sport(ticker)
        processed += 1
        for s in strategies:
            all_results[sport][s].append(res[s])
            all_results['ALL'][s].append(res[s])

    print(f"\nProcessed: {processed} markets, Skipped: {skipped}")
    print()

    # ── Compute and display metrics ──────────────────────────────────────
    def compute_metrics(trade_list):
        if not trade_list:
            return None
        pnls = [t['pnl'] for t in trade_list]
        n = len(pnls)
        wins = sum(1 for p in pnls if p > 0)
        avg_pnl = np.mean(pnls)
        total_pnl = np.sum(pnls)
        std_pnl = np.std(pnls) if n > 1 else 0
        sharpe = avg_pnl / std_pnl if std_pnl > 0 else 0

        # Max drawdown (cumulative P&L)
        cum = np.cumsum(pnls)
        peak = np.maximum.accumulate(cum)
        dd = peak - cum
        max_dd = np.max(dd) if len(dd) > 0 else 0

        # Average hold time %
        avg_hold = np.mean([t['hold_pct'] for t in trade_list])

        # Early exit rate
        early_exits = sum(1 for t in trade_list if t.get('exited_early', False))

        # Capital efficiency: total profit / total capital deployed
        # Each trade deploys entry_no + buy_fee, but we simplified to just entry_no
        # Since we do 1 contract per trade, capital per trade ~ varies
        # Use total P&L / n as proxy (already avg_pnl)
        # Better: total P&L / total entry costs... but we don't track entry per trade here
        # We'll use total_pnl / n = avg_pnl as capital efficiency proxy

        return {
            'n': n,
            'win_rate': wins / n * 100,
            'avg_pnl': avg_pnl,
            'total_pnl': total_pnl,
            'max_dd': max_dd,
            'sharpe': sharpe,
            'avg_hold_pct': avg_hold * 100,
            'early_exit_pct': early_exits / n * 100,
        }

    # Print tables
    for sport in ['ALL', 'NBA', 'NHL', 'EPL', 'NCAAMB']:
        data = all_results.get(sport)
        if not data or not data['A']:
            continue

        n_trades = len(data['A'])
        print(f"{'='*110}")
        print(f"  {sport} ({n_trades} qualifying markets)")
        print(f"{'='*110}")
        header = f"{'Strategy':<22} {'Trades':>6} {'Win%':>7} {'AvgP&L':>9} {'TotalP&L':>10} {'MaxDD':>8} {'Sharpe':>8} {'Hold%':>7} {'EarlyExit%':>10}"
        print(header)
        print(f"{'-'*110}")

        for s in strategies:
            m = compute_metrics(data[s])
            if m is None:
                continue
            label = f"{s}: {strategy_names[s]}"
            print(f"{label:<22} {m['n']:>6} {m['win_rate']:>6.1f}% ${m['avg_pnl']:>+7.4f} ${m['total_pnl']:>+9.2f} ${m['max_dd']:>7.2f} {m['sharpe']:>+7.3f} {m['avg_hold_pct']:>6.1f}% {m['early_exit_pct']:>9.1f}%")

        print()

    # ── Summary comparison ───────────────────────────────────────────────
    print(f"\n{'='*80}")
    print("  STRATEGY COMPARISON SUMMARY (ALL SPORTS)")
    print(f"{'='*80}")
    data = all_results['ALL']
    base_pnl = np.sum([t['pnl'] for t in data['A']])
    print(f"\n  Baseline (Hold to Settlement): ${base_pnl:+.2f} total P&L\n")
    for s in strategies[1:]:
        m = compute_metrics(data[s])
        if m is None:
            continue
        diff = m['total_pnl'] - base_pnl
        pct = (diff / abs(base_pnl) * 100) if base_pnl != 0 else 0
        label = f"{s}: {strategy_names[s]}"
        better = "BETTER" if diff > 0 else "WORSE"
        print(f"  {label:<25} ${m['total_pnl']:>+9.2f}  ({pct:>+6.1f}% vs hold, {better})")

    print()


if __name__ == '__main__':
    main()
