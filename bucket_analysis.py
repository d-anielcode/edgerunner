"""
Per-price-bucket analysis for worst-performing sports.
Sports: NBA, NHL, NFLSPREAD, NCAAMB, NCAAWB, NBASPREAD, UCL
Buckets: 55-60, 61-65, 66-70, 71-75, 76-80, 81-85, 86-90, 91-95 (YES price)

Methodology:
  - First trade per ticker (earliest created_time), from 2025-01-01
  - Markets must be finalized: result in ('yes','no')
  - Betting NO side: no_cost = (100 - yes_price) / 100
  - Fee = 0.07 * no_cost * (1 - no_cost)
  - P&L: if NO wins -> (1 - no_cost - fee), if YES wins -> (-no_cost - fee)
"""

import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import glob
import warnings
warnings.filterwarnings("ignore")

DATA_DIR = "data/trevorjs"

SPORTS = {
    "NBA":       "KXNBAGAME",
    "NHL":       "KXNHLGAME",
    "NFLSPREAD": "KXNFLSPREAD",
    "NCAAMB":    "KXNCAAMBGAME",
    "NCAAWB":    "KXNCAAWBGAME",
    "NBASPREAD": "KXNBASPREAD",
    "UCL":       "KXUCLGAME",
}

BUCKETS = [(55,60),(61,65),(66,70),(71,75),(76,80),(81,85),(86,90),(91,95)]

# ─── 1. Load markets ─────────────────────────────────────────────────────────
print("Loading markets...")
market_files = sorted(glob.glob(f"{DATA_DIR}/markets-*.parquet"))
markets = pd.concat(
    [pq.read_table(f, columns=["ticker","result"]).to_pandas() for f in market_files],
    ignore_index=True
)
markets["result"] = markets["result"].astype(str).str.lower().str.strip()
markets = markets[markets["result"].isin(["yes","no"])].copy()
valid_ticker_set = set(markets["ticker"].unique())
ticker_result = dict(zip(markets["ticker"], markets["result"]))
del markets
print(f"  Finalized markets: {len(valid_ticker_set):,}")

# ─── 2. Load trades file by file, vectorised ──────────────────────────────────
print("Loading trades (vectorised per file)...")
trade_files = sorted(glob.glob(f"{DATA_DIR}/trades-*.parquet"))

cutoff = pd.Timestamp("2025-01-01", tz="UTC")
all_prefixes = list(SPORTS.values())  # uppercase prefixes

# We will collect the first-trade-per-ticker across files:
# After each file we merge with our running "best" dataframe
first_trade_df = None  # columns: ticker, yes_price, created_time

for fpath in trade_files:
    fname = fpath.replace("\\","/").split("/")[-1]
    print(f"  {fname}...", end=" ", flush=True)

    t = pq.read_table(fpath, columns=["ticker","yes_price","created_time"]).to_pandas()
    t["created_time"] = pd.to_datetime(t["created_time"], utc=True, errors="coerce")

    # Date filter
    t = t[t["created_time"] >= cutoff]

    # Sport prefix filter (vectorised)
    t_up = t["ticker"].str.upper()
    mask = pd.Series(False, index=t.index)
    for prefix in all_prefixes:
        mask |= t_up.str.startswith(prefix)
    t = t[mask]

    # Valid (finalized) tickers only
    t = t[t["ticker"].isin(valid_ticker_set)]

    print(f"{len(t):,} rows", end=" -> ", flush=True)

    if len(t) == 0:
        print("skip")
        continue

    # Keep earliest per ticker in this file
    t_best = t.sort_values("created_time").groupby("ticker", sort=False).first().reset_index()
    print(f"{len(t_best):,} unique tickers")

    if first_trade_df is None:
        first_trade_df = t_best
    else:
        # Merge with running best: concat then re-take earliest
        combined = pd.concat([first_trade_df, t_best], ignore_index=True)
        first_trade_df = combined.sort_values("created_time").groupby("ticker", sort=False).first().reset_index()

    del t, t_best

print(f"\nFirst trades per ticker: {len(first_trade_df):,}")

# ─── 3. Build analysis dataframe ─────────────────────────────────────────────
df = first_trade_df.copy()
df["result"] = df["ticker"].map(ticker_result)
df = df[df["result"].isin(["yes","no"])].copy()

# Assign sport label
df["ticker_upper"] = df["ticker"].str.upper()
df["sport"] = None
for sp, prefix in SPORTS.items():
    mask = df["ticker_upper"].str.startswith(prefix)
    df.loc[mask, "sport"] = sp

df = df[df["sport"].notna()].copy()
print(f"Analysis rows: {len(df):,}")
print(f"Sport breakdown:\n{df['sport'].value_counts().to_string()}")
print()

# ─── 4. Economics ─────────────────────────────────────────────────────────────
# yes_price: check scale
if df["yes_price"].max() <= 1.0:
    df["yes_price"] = (df["yes_price"] * 100).round(0)

df["no_cost"] = (100 - df["yes_price"]) / 100
df["fee"]     = 0.07 * df["no_cost"] * (1 - df["no_cost"])
df["no_won"]  = (df["result"] == "no").astype(int)
df["pnl"]     = np.where(
    df["no_won"] == 1,
    (1 - df["no_cost"]) - df["fee"],
    -df["no_cost"] - df["fee"]
)

# ─── 5. Assign buckets ────────────────────────────────────────────────────────
def assign_bucket(price):
    for lo, hi in BUCKETS:
        if lo <= price <= hi:
            return f"{lo}-{hi}"
    return "other"

df["bucket"] = df["yes_price"].apply(assign_bucket)

# ─── 6. Max consecutive losses helper ────────────────────────────────────────
def max_consec_losses(pnl_arr):
    mx = cur = 0
    for v in pnl_arr:
        if v < 0:
            cur += 1
            if cur > mx:
                mx = cur
        else:
            cur = 0
    return mx

# ─── 7. Print per-sport per-bucket table ──────────────────────────────────────
print("=" * 108)
print("PER-PRICE-BUCKET ANALYSIS — WORST SPORTS")
print("  Buying NO at (100-YES)/100 cents. Fee=7%*no_cost*(1-no_cost). From 2025-01-01.")
print("=" * 108)
print()
print(f"{'Sport':<12} {'Bucket (YES)':>14} {'Trades':>7} {'NO Win%':>9} {'ROI%':>8} {'MaxConsecLoss':>14} {'Net P/L ($)':>12} {'Status':>10}")
print("-" * 108)

ALL_RESULTS = {}

for sport in SPORTS:
    sdf = df[df["sport"] == sport].copy()
    sport_results = {}

    if len(sdf) == 0:
        print(f"{sport:<12}  *** NO DATA MATCHED ***")
        print()
        continue

    for lo, hi in BUCKETS:
        label = f"{lo}-{hi}"
        bdf = sdf[sdf["bucket"] == label]

        if len(bdf) == 0:
            continue

        n       = len(bdf)
        win_pct = bdf["no_won"].mean() * 100
        tot_pnl = bdf["pnl"].sum()
        invested= bdf["no_cost"].sum()
        roi     = (tot_pnl / invested * 100) if invested > 0 else 0
        mcl     = max_consec_losses(bdf["pnl"].values)
        status  = "PROFIT" if tot_pnl > 0 else "LOSS"

        sport_results[label] = {
            "n": n, "win_pct": win_pct, "roi": roi,
            "tot_pnl": tot_pnl, "mcl": mcl, "status": status
        }

        print(f"{sport:<12} {'YES '+label:>14} {n:>7} {win_pct:>8.1f}% {roi:>7.1f}% {mcl:>14} {tot_pnl:>12.3f} {status:>10}")

    # Sport totals
    tot  = sdf["pnl"].sum()
    inv  = sdf["no_cost"].sum()
    sroi = (tot/inv*100) if inv > 0 else 0
    smcl = max_consec_losses(sdf["pnl"].values)
    print(f"  *** {sport} TOTAL ***       {len(sdf):>7}   {'':>9} {sroi:>7.1f}%  {smcl:>13}  {tot:>12.3f}")
    print()

    ALL_RESULTS[sport] = sport_results

# ─── 8. RECOMMENDATIONS ───────────────────────────────────────────────────────
print()
print("=" * 108)
print("RECOMMENDATIONS — ACTIONABLE EDGE TABLE CHANGES")
print("=" * 108)
print()
print("Thresholds:")
print("  KEEP    = ROI > 0%  AND  MaxConsecLoss <= 5  (n >= 5)")
print("  MONITOR = ROI > 0%  AND  MaxConsecLoss >  5  -> reduce stake 50%")
print("  DROP    = ROI <= 0%                           -> remove or set edge=0")
print("  SPARSE  = n < 5                               -> insufficient data, keep small")
print()

for sport, buckets in ALL_RESULTS.items():
    keep, monitor, drop, sparse = [], [], [], []
    for bucket, s in sorted(buckets.items()):
        if s["n"] < 5:
            sparse.append((bucket, s))
        elif s["roi"] > 0 and s["mcl"] <= 5:
            keep.append((bucket, s))
        elif s["roi"] > 0 and s["mcl"] > 5:
            monitor.append((bucket, s))
        else:
            drop.append((bucket, s))

    print(f"{'─'*72}")
    print(f"  SPORT: {sport}")
    print(f"{'─'*72}")

    def fmt(items, label):
        if not items:
            return
        print(f"  {label}:")
        for b, s in items:
            print(f"    YES {b:<8}  ROI={s['roi']:+6.1f}%  MaxConsecLoss={s['mcl']:>3}  n={s['n']}")

    fmt(keep,    "KEEP   (profitable + controlled drawdown)")
    fmt(monitor, "MONITOR/TIGHTEN (profitable but high drawdown -> stake 50%)")
    fmt(drop,    "DROP   (net losing -> remove or set edge=0)")
    fmt(sparse,  "SPARSE (< 5 trades -> hold judgment)")
    print()

print()
print("=" * 108)
print("SUMMARY ACTION ITEMS (for edge table updates)")
print("=" * 108)
print()
for sport, buckets in ALL_RESULTS.items():
    drops    = sorted([b for b,s in buckets.items() if s["n"]>=5 and s["roi"]<=0])
    keeps    = sorted([b for b,s in buckets.items() if s["n"]>=5 and s["roi"]>0 and s["mcl"]<=5])
    monitors = sorted([b for b,s in buckets.items() if s["n"]>=5 and s["roi"]>0 and s["mcl"]>5])

    print(f"  {sport}:")
    if drops:
        print(f"    DISABLE buckets (YES price): {', '.join(drops)}")
    if monitors:
        print(f"    HALVE STAKE    (YES price): {', '.join(monitors)}")
    if keeps:
        print(f"    KEEP AS-IS     (YES price): {', '.join(keeps)}")
    if not drops and not monitors and not keeps:
        print(f"    -> No buckets with sufficient data")
    print()
