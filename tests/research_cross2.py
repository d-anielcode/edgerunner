"""Cross-market signals: EPL draw, NHL spread, NFL total - simplified."""
import duckdb
con = duckdb.connect()
mp = "data/trevorjs/markets-*.parquet"
tp = "data/trevorjs/trades-*.parquet"

# ========== EPL DRAW ==========
print("=" * 80)
print("EPL: DRAW PRICE AS SIGNAL")
print("=" * 80)

epl = con.sql(f"""
    WITH all_mkts AS (
        SELECT ticker, event_ticker, result,
               SPLIT_PART(event_ticker, '-', 2) as game_key,
               SPLIT_PART(ticker, '-', 3) as team_code
        FROM '{mp}'
        WHERE event_ticker LIKE 'KXEPLGAME%' AND status='finalized' AND result IN ('yes','no')
    ),
    prices AS (
        SELECT t.ticker, t.yes_price,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
        FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM all_mkts) AND t.created_time >= '2025-01-01'
    ),
    full_data AS (
        SELECT am.game_key, am.team_code, am.result, p.yes_price
        FROM all_mkts am JOIN prices p ON am.ticker = p.ticker WHERE p.rn = 1
    )
    SELECT game_key, team_code, result, yes_price FROM full_data
""").fetchdf()

print(f"  Total EPL market records: {len(epl)}")

# Group by game
games = {}
for _, row in epl.iterrows():
    gk = row["game_key"]
    if gk not in games:
        games[gk] = {}
    games[gk][row["team_code"]] = {"price": int(row["yes_price"]), "result": row["result"]}

# Analyze games with draw pricing
draw_signal = []
for gk, outcomes in games.items():
    if "TIE" not in outcomes:
        continue
    tie_price = outcomes["TIE"]["price"]
    # Find favorite (highest priced non-tie)
    teams = {k: v for k, v in outcomes.items() if k != "TIE"}
    if not teams:
        continue
    fav = max(teams.items(), key=lambda x: x[1]["price"])
    fav_price = fav[1]["price"]
    fav_won = fav[1]["result"] == "yes"
    if fav_price < 40:
        continue
    draw_signal.append({"tie_price": tie_price, "fav_price": fav_price, "fav_won": fav_won})

print(f"  Games with draw + favorite (fav 40c+): {len(draw_signal)}")

if draw_signal:
    print(f"\n  {'Draw Price':>15s} | {'Games':>6s} | {'Fav Win%':>8s} | {'Upset%':>7s} | Signal")
    print("  " + "-" * 60)
    for label, lo, hi in [("Low (<18c)", 0, 18), ("Mid (18-25c)", 18, 25), ("High (25c+)", 25, 100)]:
        sub = [g for g in draw_signal if lo <= g["tie_price"] < hi]
        if len(sub) < 3:
            continue
        fw = sum(1 for g in sub if g["fav_won"])
        fp = fw / len(sub) * 100
        print(f"  {label:>15s} | {len(sub):>6d} | {fp:>7.1f}% | {100-fp:>6.1f}% | {'BET MORE' if 100-fp > 50 else 'NORMAL' if 100-fp > 35 else 'SKIP'}")

# ========== NHL SPREAD ==========
print(f"\n{'=' * 80}")
print("NHL: SPREAD DIVERGENCE")
print("=" * 80)

nhl = con.sql(f"""
    WITH ml AS (
        SELECT ticker, event_ticker, result,
               SPLIT_PART(event_ticker, '-', 2) as game_key,
               SPLIT_PART(ticker, '-', 3) as team
        FROM '{mp}'
        WHERE event_ticker LIKE 'KXNHLGAME%' AND status='finalized' AND result IN ('yes','no')
    ),
    ml_p AS (
        SELECT t.ticker, t.yes_price,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
        FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM ml) AND t.created_time >= '2025-01-01'
    ),
    ml_full AS (
        SELECT ml.game_key, ml.team, ml.result, mp.yes_price as ml_price
        FROM ml JOIN ml_p mp ON ml.ticker = mp.ticker WHERE mp.rn = 1 AND mp.yes_price BETWEEN 61 AND 95
    ),
    sp AS (
        SELECT ticker, event_ticker,
               SPLIT_PART(event_ticker, '-', 2) as game_key,
               REGEXP_EXTRACT(SPLIT_PART(ticker, '-', 3), '^[A-Z]+') as sp_team
        FROM '{mp}'
        WHERE event_ticker LIKE 'KXNHLSPREAD%' AND status='finalized'
    ),
    sp_p AS (
        SELECT t.ticker, t.yes_price as sp_price,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
        FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM sp) AND t.created_time >= '2025-01-01'
    ),
    sp_full AS (
        SELECT sp.game_key, sp.sp_team, spp.sp_price
        FROM sp JOIN sp_p spp ON sp.ticker = spp.ticker WHERE spp.rn = 1
    )
    SELECT ml_full.game_key, ml_full.team, ml_full.result, ml_full.ml_price, sp_full.sp_price
    FROM ml_full
    JOIN sp_full ON ml_full.game_key = sp_full.game_key AND ml_full.team = sp_full.sp_team
""").fetchdf()

print(f"  NHL matched (ML + spread): {len(nhl)}")

if len(nhl) > 0:
    print(f"\n  {'Spread Price':>15s} | {'Games':>6s} | {'NO Win%':>8s} | {'NO ROI':>8s}")
    print("  " + "-" * 50)
    for label, lo, hi in [("Low (<40c)", 0, 40), ("Mid (40-55c)", 40, 55), ("High (55c+)", 55, 100)]:
        sub = nhl[(nhl["sp_price"] >= lo) & (nhl["sp_price"] < hi)]
        if len(sub) < 5:
            continue
        nw = (sub["result"] == "no").sum()
        np_ = nw / len(sub) * 100
        tc = sum((100 - p) / 100 for p in sub["ml_price"])
        tp_ = sum(
            (1.0 - (100-p)/100) if r == "no" else -(100-p)/100
            for p, r in zip(sub["ml_price"], sub["result"])
        )
        roi = tp_ / tc * 100 if tc > 0 else 0
        print(f"  {label:>15s} | {len(sub):>6d} | {np_:>7.1f}% | {roi:>+7.1f}%")
else:
    print("  No overlapping games found between ML and spread data")
    # Debug: check if game keys overlap at all
    ml_keys = con.sql(f"""
        SELECT DISTINCT SPLIT_PART(event_ticker, '-', 2) as gk
        FROM '{mp}' WHERE event_ticker LIKE 'KXNHLGAME%' AND status='finalized'
    """).fetchdf()
    sp_keys = con.sql(f"""
        SELECT DISTINCT SPLIT_PART(event_ticker, '-', 2) as gk
        FROM '{mp}' WHERE event_ticker LIKE 'KXNHLSPREAD%' AND status='finalized'
    """).fetchdf()
    overlap = set(ml_keys["gk"]) & set(sp_keys["gk"])
    print(f"  ML game keys: {len(ml_keys)}, Spread game keys: {len(sp_keys)}, Overlap: {len(overlap)}")
    if overlap:
        sample = list(overlap)[:3]
        print(f"  Sample overlapping keys: {sample}")

# ========== UCL DRAW ==========
print(f"\n{'=' * 80}")
print("UCL: DRAW PRICE AS SIGNAL")
print("=" * 80)

ucl = con.sql(f"""
    WITH all_mkts AS (
        SELECT ticker, event_ticker, result,
               SPLIT_PART(event_ticker, '-', 2) as game_key,
               SPLIT_PART(ticker, '-', 3) as team_code
        FROM '{mp}'
        WHERE event_ticker LIKE 'KXUCLGAME%' AND status='finalized' AND result IN ('yes','no')
    ),
    prices AS (
        SELECT t.ticker, t.yes_price,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
        FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM all_mkts) AND t.created_time >= '2025-01-01'
    ),
    full_data AS (
        SELECT am.game_key, am.team_code, am.result, p.yes_price
        FROM all_mkts am JOIN prices p ON am.ticker = p.ticker WHERE p.rn = 1
    )
    SELECT game_key, team_code, result, yes_price FROM full_data
""").fetchdf()

ucl_games = {}
for _, row in ucl.iterrows():
    gk = row["game_key"]
    if gk not in ucl_games:
        ucl_games[gk] = {}
    ucl_games[gk][row["team_code"]] = {"price": int(row["yes_price"]), "result": row["result"]}

ucl_draw_signal = []
for gk, outcomes in ucl_games.items():
    if "TIE" not in outcomes:
        continue
    tie_price = outcomes["TIE"]["price"]
    teams = {k: v for k, v in outcomes.items() if k != "TIE"}
    if not teams:
        continue
    fav = max(teams.items(), key=lambda x: x[1]["price"])
    if fav[1]["price"] < 40:
        continue
    ucl_draw_signal.append({"tie_price": tie_price, "fav_price": fav[1]["price"], "fav_won": fav[1]["result"] == "yes"})

print(f"  UCL games with draw + favorite: {len(ucl_draw_signal)}")
if ucl_draw_signal:
    for label, lo, hi in [("Low (<20c)", 0, 20), ("Mid (20-28c)", 20, 28), ("High (28c+)", 28, 100)]:
        sub = [g for g in ucl_draw_signal if lo <= g["tie_price"] < hi]
        if len(sub) < 3:
            continue
        fw = sum(1 for g in sub if g["fav_won"])
        fp = fw / len(sub) * 100
        print(f"    {label:>15s}: {len(sub):>4d} games | Fav Win {fp:>5.1f}% | Upset {100-fp:>5.1f}%")

print(f"\n{'=' * 80}")
