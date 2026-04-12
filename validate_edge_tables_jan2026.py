"""
EdgeRunner Edge Table Validation -- January 2026 Out-of-Sample Test
Compares predicted YES hit rates (from edge tables built on 2025 data)
against actual January 2026 results.
"""

import sys
import glob
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore")

# Force UTF-8 output on Windows
sys.stdout.reconfigure(encoding='utf-8')

DATA_DIR = "C:/Users/dcho0/Documents/edgerunner/data/trevorjs"

# -- Sport patterns ------------------------------------------------------------
SPORT_PREFIXES = {
    "NBA":       "KXNBAGAME",
    "NHL":       "KXNHLGAME",
    "EPL":       "KXEPLGAME",
    "UCL":       "KXUCLGAME",
    "WNBA":      "KXWNBAGAME",
    "UFC":       "KXUFCFIGHT",
    "NCAAMB":    "KXNCAAMBGAME",
    "WEATHER":   "KXHIGH",
    "NFLTD":     "KXNFLANYTD",
    "NHLSPREAD": "KXNHLSPREAD",
    "NHLFG":     "KXNHLFIRSTGOAL",
    "NBASPREAD": "KXNBASPREAD",
    "NFLSPREAD": "KXNFLSPREAD",
}

# Edge table: buckets as (lo, hi, expected_yes_rate)
EDGE_BUCKETS = {
    "NBA":      [(55,59,None),(60,64,0.50),(65,69,0.48),(70,74,0.46),(75,79,0.44),(80,84,0.42),(85,89,0.40)],
    "NHL":      [(55,59,None),(60,64,0.55),(65,69,0.535),(70,74,0.52),(75,79,0.505),(80,84,0.49),(85,89,0.475)],
    "EPL":      [(71,85,0.485)],
    "UCL":      [(66,70,0.400),(76,85,0.641)],
    "WNBA":     [(55,62,0.380),(71,77,0.550),(83,87,0.540)],
    "UFC":      [(76,85,0.622)],
    "NCAAMB":   [(66,70,0.536),(71,80,0.656),(82,90,0.770)],
    "WEATHER":  [(55,65,0.404),(66,75,0.417),(76,85,0.417),(86,95,0.419)],
    "NFLTD":    [(55,65,0.492),(66,75,0.452),(76,85,0.545),(86,95,0.286)],
    "NHLSPREAD":[(55,65,0.500),(66,75,0.450),(76,90,0.400)],
    "NHLFG":    [(55,70,0.550),(71,90,0.450)],
    "NBASPREAD":[(55,65,0.480),(66,75,0.440),(76,90,0.380)],
    "NFLSPREAD":[(55,65,0.480),(66,75,0.440),(76,90,0.380)],
}

def get_expected_yes_rate(sport, yes_price):
    """Return expected YES win rate for a sport at a given yes_price."""
    if sport == "NBA":
        return 0.50 - (yes_price - 60) * 0.004
    if sport == "NHL":
        return 0.55 - (yes_price - 60) * 0.003
    buckets = EDGE_BUCKETS.get(sport, [])
    for (lo, hi, rate) in buckets:
        if lo <= yes_price <= hi and rate is not None:
            return rate
    return None

def assign_sport(ticker):
    """Return sport key if ticker matches a known prefix, else None."""
    for sport, prefix in SPORT_PREFIXES.items():
        if ticker.startswith(prefix):
            return sport
    return None

def assign_bucket(yes_price):
    """Assign a 5-cent bucket label. Standard 5c buckets: 55-59, 60-64, ..."""
    if yes_price < 55 or yes_price > 95:
        return None
    lo = (yes_price // 5) * 5
    hi = lo + 4
    return (lo, hi)

# -- 1. Load markets: only finalized yes/no -----------------------------------
print("=" * 70)
print("STEP 1: Loading finalized markets...")
mkt_cols = ['ticker', 'result', 'created_time']
mkts_list = []
for f in sorted(glob.glob(f"{DATA_DIR}/markets-*.parquet")):
    df = pd.read_parquet(f, columns=mkt_cols)
    mkts_list.append(df)
markets = pd.concat(mkts_list, ignore_index=True)

markets['result'] = markets['result'].astype(str).str.strip().str.lower()
markets_final = markets[markets['result'].isin(['yes', 'no'])][['ticker', 'result']].copy()
# Deduplicate -- keep one result per ticker (take last non-null)
markets_final = markets_final.drop_duplicates(subset='ticker', keep='last')
print(f"  {len(markets_final):,} unique finalized tickers")

# Assign sport
markets_final['sport'] = markets_final['ticker'].apply(assign_sport)
markets_final = markets_final[markets_final['sport'].notna()]
print(f"  {len(markets_final):,} tickers matching sport patterns")
print(f"  By sport:\n{markets_final['sport'].value_counts().to_string()}")

# -- 2. Load trades: Jan 2026 only, first trade per ticker --------------------
print("\nSTEP 2: Loading trades (streaming per file, Jan 2026 filter)...")
JAN_START = pd.Timestamp("2026-01-01", tz="UTC")
JAN_END   = pd.Timestamp("2026-02-01", tz="UTC")

trade_cols = ['ticker', 'yes_price', 'created_time']
trades_jan_list = []

for f in sorted(glob.glob(f"{DATA_DIR}/trades-*.parquet")):
    df = pd.read_parquet(f, columns=trade_cols)
    # Filter to Jan 2026
    if not pd.api.types.is_datetime64_any_dtype(df['created_time']):
        df['created_time'] = pd.to_datetime(df['created_time'], utc=True, errors='coerce')
    mask = (df['created_time'] >= JAN_START) & (df['created_time'] < JAN_END)
    jan = df[mask]
    if len(jan) > 0:
        trades_jan_list.append(jan)
    del df

if not trades_jan_list:
    print("  ERROR: No January 2026 trades found!")
    exit(1)

trades_jan = pd.concat(trades_jan_list, ignore_index=True)
print(f"  {len(trades_jan):,} trades in January 2026")
print(f"  Date range: {trades_jan['created_time'].min()} to {trades_jan['created_time'].max()}")
print(f"  Unique tickers: {trades_jan['ticker'].nunique():,}")

# First trade per ticker (earliest created_time)
trades_jan_sorted = trades_jan.sort_values('created_time')
first_trades = trades_jan_sorted.drop_duplicates(subset='ticker', keep='first')[['ticker', 'yes_price']].copy()
print(f"  {len(first_trades):,} unique tickers with first trade in Jan 2026")

# -- 3. Join trades to markets -------------------------------------------------
print("\nSTEP 3: Joining trades to finalized markets...")
# Only tickers that have both a Jan 2026 first trade AND a finalized result
joined = first_trades.merge(markets_final, on='ticker', how='inner')
print(f"  {len(joined):,} tickers with Jan 2026 first trade AND finalized result")
print(f"  By sport:\n{joined['sport'].value_counts().to_string()}")

# -- 4. Assign buckets ---------------------------------------------------------
joined['bucket'] = joined['yes_price'].apply(assign_bucket)
joined = joined[joined['bucket'].notna()]
joined['is_yes'] = (joined['result'] == 'yes').astype(int)

# -- 5. Compute actual vs expected per sport + bucket -------------------------
print("\n" + "=" * 70)
print("EDGE TABLE VALIDATION -- JANUARY 2026 OUT-OF-SAMPLE")
print("=" * 70)

VERDICT_THRESHOLD = 0.05  # 5 percentage points

summary_rows = []

for sport in sorted(SPORT_PREFIXES.keys()):
    sdf = joined[joined['sport'] == sport].copy()
    if len(sdf) == 0:
        print(f"\n{'-'*60}")
        print(f"SPORT: {sport}  -- NO DATA IN JAN 2026")
        continue

    # Get relevant buckets from EDGE_BUCKETS (or generate for linear sports)
    if sport in ("NBA", "NHL"):
        # Generate 5c buckets from 55-90
        buckets = [(lo, lo+4) for lo in range(55, 90, 5)]
    else:
        buckets = [(lo, hi) for (lo, hi, rate) in EDGE_BUCKETS.get(sport, []) if rate is not None]

    print(f"\n{'-'*60}")
    print(f"SPORT: {sport}  ({len(sdf):,} tickers total in Jan 2026)")
    print(f"{'Bucket':>10} {'N':>6} {'ActualYES':>10} {'ExpectedYES':>12} {'Gap':>8} {'SimROI(NO)':>12}")
    print(f"{'':->10} {'':->6} {'':->10} {'':->12} {'':->8} {'':->12}")

    sport_rows = []
    for (lo, hi) in buckets:
        # Use the actual bucket assignment (which uses 5c standard buckets)
        # For sports with wide buckets (e.g. 55-70), match on those ranges
        mask = (sdf['yes_price'] >= lo) & (sdf['yes_price'] <= hi)
        bdf = sdf[mask]
        n = len(bdf)
        if n == 0:
            continue

        actual_yes = bdf['is_yes'].mean()
        mid_price = (lo + hi) / 2
        exp_yes = get_expected_yes_rate(sport, int(mid_price))

        if exp_yes is None:
            continue

        gap = actual_yes - exp_yes

        # Simulated ROI if we buy NO at yes_price (no_price = 100 - yes_price)
        # We pay no_price cents, collect 100 if NO wins (= YES does NOT happen)
        # ROI = (pct_no_wins * (100 - no_price) - pct_yes_wins * no_price) / no_price
        # = (actual_no * yes_price - actual_yes * no_price) / no_price
        avg_yes_price = bdf['yes_price'].mean()
        avg_no_price = 100 - avg_yes_price
        actual_no = 1 - actual_yes
        sim_roi = (actual_no * avg_yes_price - actual_yes * avg_no_price) / avg_no_price

        print(f"  {lo}-{hi}   {n:>6,}   {actual_yes:>8.3f}   {exp_yes:>10.3f}   {gap:>+7.3f}   {sim_roi:>+10.3f}")
        sport_rows.append({
            'sport': sport, 'bucket': f"{lo}-{hi}", 'n': n,
            'actual_yes': actual_yes, 'exp_yes': exp_yes, 'gap': gap, 'sim_roi': sim_roi
        })
        summary_rows.append(sport_rows[-1])

    # Overall verdict for this sport
    if sport_rows:
        buckets_with_data = [r for r in sport_rows if r['n'] >= 10]
        if not buckets_with_data:
            verdict = "INSUFFICIENT DATA"
        else:
            avg_gap = np.mean([r['gap'] for r in buckets_with_data])
            max_abs_gap = max(abs(r['gap']) for r in buckets_with_data)
            if max_abs_gap <= VERDICT_THRESHOLD:
                verdict = "CONFIRMED  (actual within 5% of expected)"
            elif avg_gap < -VERDICT_THRESHOLD:
                verdict = "BETTER THAN EXPECTED  (actual YES < predicted -- edge is BIGGER)"
            elif avg_gap > VERDICT_THRESHOLD:
                verdict = "EDGE DECAYED  (actual YES > predicted -- favorites win MORE than expected)"
            else:
                verdict = f"MIXED  (avg gap {avg_gap:+.3f}, max abs {max_abs_gap:.3f})"
        print(f"\n  >> VERDICT: {verdict}")

# -- 6. Master summary table ---------------------------------------------------
print("\n" + "=" * 70)
print("MASTER SUMMARY TABLE")
print("=" * 70)
if summary_rows:
    sdf_all = pd.DataFrame(summary_rows)
    print(sdf_all.to_string(index=False, float_format=lambda x: f"{x:+.4f}" if isinstance(x, float) else str(x)))

print("\n" + "=" * 70)
print("SPORT-LEVEL VERDICT SUMMARY")
print("=" * 70)
sport_verdicts = {}
for sport in sorted(SPORT_PREFIXES.keys()):
    sport_rows_filtered = [r for r in summary_rows if r['sport'] == sport and r['n'] >= 10]
    if not sport_rows_filtered:
        sport_verdicts[sport] = "NO DATA / INSUFFICIENT"
        continue
    avg_gap = np.mean([r['gap'] for r in sport_rows_filtered])
    max_abs_gap = max(abs(r['gap']) for r in sport_rows_filtered)
    total_n = sum(r['n'] for r in sport_rows_filtered)
    weighted_roi = np.mean([r['sim_roi'] for r in sport_rows_filtered])
    if max_abs_gap <= VERDICT_THRESHOLD:
        v = "CONFIRMED"
    elif avg_gap < -VERDICT_THRESHOLD:
        v = "BETTER THAN EXPECTED"
    elif avg_gap > VERDICT_THRESHOLD:
        v = "EDGE DECAYED"
    else:
        v = "MIXED"
    sport_verdicts[sport] = v
    print(f"  {sport:<12} N={total_n:>5,}  avgGap={avg_gap:+.4f}  avgSimROI={weighted_roi:+.4f}  => {v}")

print("\nDone.")
