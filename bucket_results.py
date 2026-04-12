# -*- coding: utf-8 -*-
"""
Produces recommendations from already-computed bucket stats.
Data is hard-coded from the successful run above.
"""

# Results: (sport, bucket_label, n_trades, no_win_pct, roi_pct, max_consec_loss, net_pnl)
RAW = [
    # NBA
    ("NBA", "55-60",  160, 46.2,   4.5,  6,   3.095),
    ("NBA", "61-65",  133, 39.1,   1.3, 12,   0.642),
    ("NBA", "66-70",  125, 40.8,  22.8,  6,   9.108),
    ("NBA", "71-75",  126, 36.5,  29.9,  7,  10.192),
    ("NBA", "76-80",  132, 29.5,  29.2, 16,   8.459),
    ("NBA", "81-85",   58, 25.9,  47.3, 10,   4.631),
    ("NBA", "86-90",   70, 31.4, 131.8, 10,  12.189),
    ("NBA", "91-95",    1,  0.0,-106.4,  1,  -0.085),
    # NHL
    ("NHL", "55-60",  412, 51.2,  16.1,  6,  28.306),
    ("NHL", "61-65",  248, 48.4,  26.2, 10,  24.055),
    ("NHL", "66-70",  200, 43.0,  30.0, 11,  19.130),
    ("NHL", "71-75",   97, 43.3,  55.6,  6,  14.525),
    ("NHL", "76-80",   21, 42.9,  85.7,  5,   4.034),
    ("NHL", "81-85",    6, 16.7,  -8.7,  3,  -0.090),
    ("NHL", "86-90",   16, 62.5, 485.5,  2,   8.204),
    ("NHL", "91-95",    6, 16.7,  78.8,  4,   0.426),
    # NFLSPREAD
    ("NFLSPREAD", "55-60",  184, 50.5,  14.4,  8,  11.343),
    ("NFLSPREAD", "61-65",  100, 41.0,   6.7,  8,   2.461),
    ("NFLSPREAD", "66-70",   82, 37.8,  12.1,  7,   3.215),
    ("NFLSPREAD", "71-75",   54, 29.6,   4.3, 11,   0.624),
    ("NFLSPREAD", "76-80",   45, 33.3,  42.4,  9,   4.300),
    ("NFLSPREAD", "81-85",   29, 31.0,  75.3,  5,   3.742),
    ("NFLSPREAD", "86-90",   27, 37.0, 205.4,  5,   6.592),
    ("NFLSPREAD", "91-95",   10, 40.0, 393.6,  5,   3.149),
    # NCAAMB
    ("NCAAMB", "55-60",  321, 48.3,   9.3,  8,  12.711),
    ("NCAAMB", "61-65",  260, 37.3,  -3.0, 10,  -2.828),
    ("NCAAMB", "66-70",  347, 46.4,  41.6, 12,  45.746),
    ("NCAAMB", "71-75",  252, 44.0,  56.9,  6,  39.002),
    ("NCAAMB", "76-80",  260, 25.0,  10.0, 21,   5.647),
    ("NCAAMB", "81-85",  227, 24.2,  37.7, 14,  14.444),
    ("NCAAMB", "86-90",  300, 20.0,  64.2, 19,  22.619),
    ("NCAAMB", "91-95",  477, 13.0,  97.8, 44,  29.668),
    # NCAAWB
    ("NCAAWB", "55-60",  179, 45.3,   2.8,  7,   2.145),
    ("NCAAWB", "61-65",  106, 48.1,  25.8,  4,  10.103),
    ("NCAAWB", "66-70",  129, 40.3,  22.5, 12,   9.208),
    ("NCAAWB", "71-75",  138, 32.6,  16.5, 12,   6.096),
    ("NCAAWB", "76-80",  167, 27.5,  20.5, 18,   7.485),
    ("NCAAWB", "81-85",  154, 25.3,  43.7, 11,  11.395),
    ("NCAAWB", "86-90",  220, 11.8,  -6.0, 33,  -1.559),
    ("NCAAWB", "91-95",  262, 11.8,  60.3, 28,  11.205),
    # NBASPREAD
    ("NBASPREAD", "55-60",  224, 47.3,   7.7,  8,   7.287),
    ("NBASPREAD", "61-65",  263, 46.8,  20.8,  7,  20.466),
    ("NBASPREAD", "66-70",  128, 39.1,  17.9, 12,   7.287),
    ("NBASPREAD", "71-75",  171, 30.4,   7.1, 13,   3.297),
    ("NBASPREAD", "76-80",  110, 35.5,  54.7,  8,  13.314),
    ("NBASPREAD", "81-85",   69, 26.1,  48.4, 10,   5.652),
    ("NBASPREAD", "86-90",   77, 42.9, 256.5,  4,  23.339),
    ("NBASPREAD", "91-95",    7, 14.3,  97.6,  6,   0.478),
    # UCL
    ("UCL", "55-60",  19, 31.6, -30.1,  6,  -2.445),
    ("UCL", "61-65",  11, 36.4,  -7.5,  5,  -0.310),
    ("UCL", "66-70",  10, 60.0,  82.7,  3,   2.648),
    ("UCL", "71-75",  14, 21.4, -25.9,  5,  -0.983),
    ("UCL", "76-80",  25, 36.0,  67.9,  7,   3.522),
    ("UCL", "81-85",  12, 41.7, 113.6,  5,   2.591),
    ("UCL", "86-90",   1,  0.0,-106.0,  1,  -0.148),
]

SPORT_TOTALS = {
    "NBA":       (1622,  3.9,  6,  29.751),
    "NHL":       (1942,  7.5,  7,  65.340),
    "NFLSPREAD": (3969, -0.6, 15, -15.520),
    "NCAAMB":    (6020,  8.0, 17, 219.279),
    "NCAAWB":    (3669,  5.5, 15,  93.746),
    "NBASPREAD": (7325,  2.0,  9, 100.241),
    "UCL":       ( 390,  3.5,  4,   8.719),
}

SPORT_ORDER = ["NBA","NHL","NFLSPREAD","NCAAMB","NCAAWB","NBASPREAD","UCL"]

# --- Print table -------------------------------------------------------------
SEP = "=" * 108
sep = "-" * 108

print(SEP)
print("PER-PRICE-BUCKET ANALYSIS -- WORST SPORTS")
print("  Buying NO at (100-YES)/100. Fee=7%%*no_cost*(1-no_cost). From 2025-01-01.")
print("  First trade per ticker only.  n=24,937 total analysed.")
print(SEP)
print()
print(f"{'Sport':<12} {'Bucket (YES)':>14} {'Trades':>7} {'NO Win%':>9} {'ROI%':>8} {'MaxConsecLoss':>14} {'Net P/L':>10} {'Status':>10}")
print(sep)

prev_sport = None
for sport, bkt, n, winp, roi, mcl, pnl in RAW:
    if sport != prev_sport and prev_sport is not None:
        tot_n, tot_roi, tot_mcl, tot_pnl = SPORT_TOTALS[prev_sport]
        print(f"  *** {prev_sport} TOTAL ***       {tot_n:>7}             {tot_roi:>7.1f}%  {tot_mcl:>13}  {tot_pnl:>10.3f}")
        print()
    prev_sport = sport
    status = "PROFIT" if pnl > 0 else "LOSS"
    print(f"{sport:<12} {'YES '+bkt:>14} {n:>7} {winp:>8.1f}% {roi:>7.1f}% {mcl:>14} {pnl:>10.3f} {status:>10}")

# final sport
tot_n, tot_roi, tot_mcl, tot_pnl = SPORT_TOTALS[prev_sport]
print(f"  *** {prev_sport} TOTAL ***       {tot_n:>7}             {tot_roi:>7.1f}%  {tot_mcl:>13}  {tot_pnl:>10.3f}")
print()

# --- Recommendations ---------------------------------------------------------
MIN_N = 5  # min trades to draw a conclusion

print()
print(SEP)
print("RECOMMENDATIONS -- ACTIONABLE EDGE TABLE CHANGES")
print(SEP)
print()
print("Classification thresholds:")
print("  KEEP    = ROI > 0%  AND  MaxConsecLoss <= 5  (n >= 5)")
print("  MONITOR = ROI > 0%  AND  MaxConsecLoss >  5  -> reduce stake 50%  (n >= 5)")
print("  DROP    = ROI <= 0%                          -> remove/set edge=0  (n >= 5)")
print("  SPARSE  = n < 5                              -> hold, no action yet")
print()

buckets_by_sport = {}
for sport, bkt, n, winp, roi, mcl, pnl in RAW:
    buckets_by_sport.setdefault(sport, []).append((bkt, n, winp, roi, mcl, pnl))

for sport in SPORT_ORDER:
    items = buckets_by_sport.get(sport, [])
    keep, monitor, drop, sparse = [], [], [], []
    for bkt, n, winp, roi, mcl, pnl in items:
        if n < MIN_N:
            sparse.append((bkt, n, winp, roi, mcl, pnl))
        elif roi > 0 and mcl <= 5:
            keep.append((bkt, n, winp, roi, mcl, pnl))
        elif roi > 0 and mcl > 5:
            monitor.append((bkt, n, winp, roi, mcl, pnl))
        else:
            drop.append((bkt, n, winp, roi, mcl, pnl))

    print(f"{'-'*72}")
    print(f"  SPORT: {sport}   [Total ROI={SPORT_TOTALS[sport][1]:+.1f}%  Total P/L={SPORT_TOTALS[sport][3]:+.3f}]")
    print(f"{'-'*72}")

    def fmt(items, label, emoji=""):
        if not items:
            return
        print(f"  {label}:")
        for bkt, n, winp, roi, mcl, pnl in items:
            print(f"    YES {bkt:<8}  ROI={roi:+6.1f}%  MaxConsecLoss={mcl:>3}  NOWin={winp:4.1f}%  n={n:>4}  P/L={pnl:+7.3f}")

    fmt(keep,    "[KEEP]   Profitable + controlled drawdown")
    fmt(monitor, "[MONITOR/TIGHTEN] Profitable but high drawdown -> halve stake")
    fmt(drop,    "[DROP]   Net losing -> remove or set edge=0")
    fmt(sparse,  "[SPARSE] Under 5 trades -> hold judgment")
    print()

# --- Summary action items -----------------------------------------------------
print()
print(SEP)
print("SUMMARY ACTION ITEMS (copy into edge table update)")
print(SEP)
print()

for sport in SPORT_ORDER:
    items = buckets_by_sport.get(sport, [])
    drops    = sorted([bkt for bkt,n,winp,roi,mcl,pnl in items if n>=MIN_N and roi<=0])
    monitors = sorted([bkt for bkt,n,winp,roi,mcl,pnl in items if n>=MIN_N and roi>0 and mcl>5])
    keeps    = sorted([bkt for bkt,n,winp,roi,mcl,pnl in items if n>=MIN_N and roi>0 and mcl<=5])
    sparses  = sorted([bkt for bkt,n,winp,roi,mcl,pnl in items if n<MIN_N])

    tot_n, tot_roi, tot_mcl, tot_pnl = SPORT_TOTALS[sport]
    print(f"  {sport}  (overall ROI={tot_roi:+.1f}%  P/L={tot_pnl:+.3f}  MCL={tot_mcl}):")
    if drops:
        print(f"    DISABLE  YES price buckets: {', '.join(drops)}")
    if monitors:
        print(f"    HALVE STAKE for YES price buckets: {', '.join(monitors)}")
    if keeps:
        print(f"    KEEP AS-IS for YES price buckets:  {', '.join(keeps)}")
    if sparses:
        print(f"    SPARSE (no action): {', '.join(sparses)}")
    print()

print()
print(SEP)
print("KEY OBSERVATIONS")
print(SEP)
print("""
1. NBA (ROI +3.9%): All buckets profitable but 76-80 (MCL=16) and 81-90 (MCL=10) carry
   significant drawdown. High YES prices (81-90) produce outsized ROI (47-132%) -- keep
   but size conservatively. 91-95 has only 1 trade, ignore.

2. NHL (ROI +7.5%): Strong across the board. 81-85 is marginally losing (-8.7%, n=6) --
   sparse. 86-90 bucket (ROI 485%!) is likely sample-size noise (n=16) but positive.
   MCL on 61-70 range (10-11) warrants half-sizing.

3. NFLSPREAD (ROI -0.6% overall, total loss -$15.52): The low YES-price buckets (55-75)
   carry high MCL (8-11) and the overall book loses despite per-bucket positivity in
   dollar terms. Investigate whether the overall loss is driven by an out-of-sample
   period. Per-bucket shows all profitable in isolation -- the portfolio-level loss
   suggests adverse selection or untracked costs.

4. NCAAMB (ROI +8.0%): 61-65 is the only losing bucket (ROI -3%, n=260). All high-price
   buckets are profitable but carry extreme MCL (76-80: MCL=21; 86-90: MCL=19;
   91-95: MCL=44 on 477 trades). These are NOT safe to bet at normal sizing.

5. NCAAWB (ROI +5.5%): 86-90 loses (-6%, n=220). All others profitable but MCL
   spikes dangerously at 76-80 (MCL=18), 86-90 (MCL=33), 91-95 (MCL=28).

6. NBASPREAD (ROI +2.0%): All buckets profitable. 66-70 and 71-75 show MCL=12-13.
   86-90 is the star (ROI 256%, n=77, MCL=4) -- safe to increase sizing here.

7. UCL (ROI +3.5%): Small sample (390 trades). 55-60, 61-65, 71-75 all losing.
   66-70, 76-85 profitable. High-price buckets (86-90) has only 1 trade.
   Overall avoid YES <72 range; focus on 66-70 and 76-85.
""")
