"""
Research: Soccer draw price signal + NHL spread divergence.
"""
import duckdb
con = duckdb.connect()
mp = "data/trevorjs/markets-*.parquet"
tp = "data/trevorjs/trades-*.parquet"

# ================================================================
# TEST 1: SOCCER DRAW PRICE AS SIGNAL
# EPL/UCL have 3-way markets: Home Win, Draw, Away Win
# If draw price is HIGH → game expected to be close → better for fading favorite
# ================================================================
print("=" * 90)
print("TEST 1: SOCCER DRAW PRICE AS SIGNAL (EPL + UCL)")
print("High draw price = market thinks game is close = better NO bet on favorite")
print("=" * 90)

for sport, pat in [("EPL", "KXEPLGAME"), ("UCL", "KXUCLGAME")]:
    # Get all 3-way outcomes per game
    games = con.sql(f"""
        WITH markets AS (
            SELECT ticker, event_ticker, result, title, yes_bid,
                   SPLIT_PART(event_ticker, '-', 2) as game_key
            FROM '{mp}'
            WHERE event_ticker LIKE '{pat}%' AND status='finalized' AND yes_bid > 0
        ),
        game_outcomes AS (
            SELECT game_key,
                   MAX(CASE WHEN title LIKE '%Draw%' OR title LIKE '%Tie%' THEN yes_bid END) as draw_price,
                   MAX(yes_bid) as fav_price,
                   -- The favorite is the one with highest yes_bid (excluding draw)
                   MAX(CASE WHEN title NOT LIKE '%Draw%' AND title NOT LIKE '%Tie%' THEN yes_bid END) as top_team_price,
                   MIN(CASE WHEN title NOT LIKE '%Draw%' AND title NOT LIKE '%Tie%' AND yes_bid > 0 THEN yes_bid END) as underdog_price,
                   -- Did the favorite (highest priced non-draw) win?
                   MAX(CASE WHEN title NOT LIKE '%Draw%' AND title NOT LIKE '%Tie%' AND yes_bid = (
                       SELECT MAX(m2.yes_bid) FROM '{mp}' m2
                       WHERE m2.event_ticker = markets.event_ticker
                       AND m2.title NOT LIKE '%Draw%' AND m2.title NOT LIKE '%Tie%'
                   ) THEN result END) as fav_result
            FROM markets
            GROUP BY game_key
            HAVING draw_price IS NOT NULL AND top_team_price >= 40
        )
        SELECT * FROM game_outcomes
    """).fetchdf()

    if len(games) == 0:
        # Simpler approach: just get all markets per game
        games = con.sql(f"""
            WITH all_mkts AS (
                SELECT event_ticker,
                       SPLIT_PART(event_ticker, '-', 2) as game_key,
                       ticker, title, result, yes_bid
                FROM '{mp}'
                WHERE event_ticker LIKE '{pat}%' AND status='finalized'
            ),
            per_game AS (
                SELECT game_key, event_ticker,
                       -- Get draw market
                       MAX(CASE WHEN title LIKE '%Draw%' OR title LIKE '%Tie%'
                           THEN yes_bid END) as draw_price,
                       -- Get the highest non-draw price (favorite)
                       MAX(CASE WHEN title NOT LIKE '%Draw%' AND title NOT LIKE '%Tie%'
                           THEN yes_bid END) as fav_price,
                       -- Did the draw happen?
                       MAX(CASE WHEN (title LIKE '%Draw%' OR title LIKE '%Tie%') AND result='yes'
                           THEN 1 ELSE 0 END) as draw_happened,
                       -- Did the favorite win?
                       MAX(CASE WHEN title NOT LIKE '%Draw%' AND title NOT LIKE '%Tie%'
                           AND yes_bid = (SELECT MAX(m2.yes_bid) FROM all_mkts m2
                                          WHERE m2.event_ticker = all_mkts.event_ticker
                                          AND m2.title NOT LIKE '%Draw%' AND m2.title NOT LIKE '%Tie%')
                           AND result='yes' THEN 1 ELSE 0 END) as fav_won
                FROM all_mkts
                GROUP BY game_key, event_ticker
                HAVING draw_price IS NOT NULL
            )
            SELECT * FROM per_game WHERE fav_price >= 40
        """).fetchdf()

    print(f"\n  {sport}: {len(games)} games with 3-way pricing")

    if len(games) > 0:
        # Bucket by draw price
        print(f"\n  {'Draw Price':>15s} | {'Games':>6s} | {'Fav Won':>8s} | {'Fav Win%':>8s} | {'NO Win%':>8s} | Signal")
        print("  " + "-" * 70)

        for label, lo, hi in [("Low draw (<20c)", 0, 20),
                                ("Mid draw (20-28c)", 20, 28),
                                ("High draw (28c+)", 28, 100)]:
            sub = games[(games["draw_price"] >= lo) & (games["draw_price"] < hi)]
            if len(sub) < 5: continue
            fav_wins = sub["fav_won"].sum()
            fav_pct = fav_wins / len(sub) * 100
            no_pct = 100 - fav_pct
            signal = "INCREASE BET" if no_pct > 55 else ("NORMAL" if no_pct > 40 else "REDUCE")
            print(f"  {label:>15s} | {len(sub):>6d} | {fav_wins:>8d} | {fav_pct:>7.1f}% | {no_pct:>7.1f}% | {signal}")

        # Granular
        print(f"\n  Per-5c draw price:")
        for lo in range(10, 40, 5):
            sub = games[(games["draw_price"] >= lo) & (games["draw_price"] < lo + 5)]
            if len(sub) < 3: continue
            fav_wins = sub["fav_won"].sum()
            no_pct = (1 - fav_wins / len(sub)) * 100
            print(f"    Draw {lo}-{lo+4}c: {len(sub):>4d} games | NO wins {no_pct:>5.1f}%")

# ================================================================
# TEST 2: NHL SPREAD DIVERGENCE
# Same as NBA: does the spread market predict upsets?
# ================================================================
print(f"\n\n{'=' * 90}")
print("TEST 2: NHL SPREAD DIVERGENCE")
print("Does the NHL spread market predict which favorites get upset?")
print("=" * 90)

nhl_matched = con.sql(f"""
    WITH ml AS (
        SELECT ticker, event_ticker, result,
               SPLIT_PART(event_ticker, '-', 2) as game_key,
               SPLIT_PART(ticker, '-', 3) as team
        FROM '{mp}'
        WHERE event_ticker LIKE 'KXNHLGAME%' AND status='finalized' AND result IN ('yes','no')
    ),
    ml_prices AS (
        SELECT t.ticker, t.yes_price,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
        FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM ml) AND t.created_time >= '2025-01-01'
    ),
    ml_full AS (
        SELECT ml.ticker, ml.game_key, ml.team, ml.result, mp.yes_price as ml_price
        FROM ml JOIN ml_prices mp ON ml.ticker = mp.ticker
        WHERE mp.rn = 1 AND mp.yes_price BETWEEN 61 AND 95
    ),
    spreads AS (
        SELECT event_ticker, yes_bid as sp_price,
               SPLIT_PART(event_ticker, '-', 2) as game_key,
               REGEXP_EXTRACT(SPLIT_PART(ticker, '-', 3), '^[A-Z]+') as sp_team,
               CAST(NULLIF(REGEXP_EXTRACT(SPLIT_PART(ticker, '-', 3), '[0-9]+'), '') AS INT) as spread_pts
        FROM '{mp}'
        WHERE event_ticker LIKE 'KXNHLSPREAD%' AND status='finalized' AND yes_bid > 0
    )
    SELECT ml_full.ticker, ml_full.game_key, ml_full.team, ml_full.result,
           ml_full.ml_price, spreads.spread_pts, spreads.sp_price
    FROM ml_full
    JOIN spreads ON ml_full.game_key = spreads.game_key AND ml_full.team = spreads.sp_team
    WHERE spreads.spread_pts IS NOT NULL
""").fetchdf()

print(f"\n  NHL matched games (ML + Spread): {len(nhl_matched)}")

if len(nhl_matched) > 0:
    print(f"\n  By spread price (low = game expected close, high = blowout):")
    print(f"  {'Spread Price':>15s} | {'Games':>6s} | {'NO Win%':>8s} | {'NO ROI':>8s} | Signal")
    print("  " + "-" * 60)

    for label, lo, hi in [("Low (<40c)", 0, 40),
                            ("Mid (40-55c)", 40, 55),
                            ("High (55c+)", 55, 100)]:
        sub = nhl_matched[(nhl_matched["sp_price"] >= lo) & (nhl_matched["sp_price"] < hi)]
        if len(sub) < 5: continue
        no_wins = (sub["result"] == "no").sum()
        no_pct = no_wins / len(sub) * 100
        total_cost = sum((100 - p) / 100 for p in sub["ml_price"])
        total_pnl = sum(
            (1.0 - (100 - p) / 100) if r == "no" else -(100 - p) / 100
            for p, r in zip(sub["ml_price"], sub["result"])
        )
        roi = total_pnl / total_cost * 100 if total_cost > 0 else 0
        signal = "INCREASE BET" if roi > 40 else ("NORMAL" if roi > 0 else "SKIP")
        print(f"  {label:>15s} | {len(sub):>6d} | {no_pct:>7.1f}% | {roi:>+7.1f}% | {signal}")

    # Divergence
    nhl_matched["expected_sp"] = nhl_matched["ml_price"].apply(lambda x: max(25, x - 25))
    nhl_matched["divergence"] = nhl_matched["sp_price"] - nhl_matched["expected_sp"]

    print(f"\n  NHL Divergence (spread vs moneyline):")
    for lo, hi, label in [(-100, -10, "Spread says CLOSER"),
                           (-10, 10, "Agree"),
                           (10, 100, "Spread says BLOWOUT")]:
        sub = nhl_matched[(nhl_matched["divergence"] >= lo) & (nhl_matched["divergence"] < hi)]
        if len(sub) < 5: continue
        no_wins = (sub["result"] == "no").sum()
        no_pct = no_wins / len(sub) * 100
        total_cost = sum((100 - p) / 100 for p in sub["ml_price"])
        total_pnl = sum(
            (1.0 - (100 - p) / 100) if r == "no" else -(100 - p) / 100
            for p, r in zip(sub["ml_price"], sub["result"])
        )
        roi = total_pnl / total_cost * 100 if total_cost > 0 else 0
        print(f"    {label:25s}: {len(sub):>4d} games | NO wins {no_pct:>5.1f}% | ROI {roi:>+7.1f}%")

# ================================================================
# TEST 3: NFL TOTAL AS TD SIGNAL
# High total = high-scoring game = more TDs = bad for our NO TD bets?
# ================================================================
print(f"\n\n{'=' * 90}")
print("TEST 3: NFL TOTAL (Over/Under) AS TD SIGNAL")
print("Does game total predict whether TD props hit?")
print("=" * 90)

# Match NFL TD props with game totals
nfl_td = con.sql(f"""
    WITH td_mkts AS (
        SELECT ticker, event_ticker, result,
               SPLIT_PART(event_ticker, '-', 2) as game_key
        FROM '{mp}'
        WHERE event_ticker LIKE 'KXNFLANYTD%' AND status='finalized' AND result IN ('yes','no')
    ),
    td_prices AS (
        SELECT t.ticker, t.yes_price,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
        FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM td_mkts) AND t.created_time >= '2025-01-01'
    ),
    td_full AS (
        SELECT td_mkts.ticker, td_mkts.game_key, td_mkts.result, tp.yes_price as td_price
        FROM td_mkts JOIN td_prices tp ON td_mkts.ticker = tp.ticker
        WHERE tp.rn = 1 AND tp.yes_price BETWEEN 55 AND 95
    ),
    totals AS (
        SELECT event_ticker, yes_bid as total_price,
               SPLIT_PART(event_ticker, '-', 2) as game_key
        FROM '{mp}'
        WHERE event_ticker LIKE 'KXNFLTOTAL%' AND status='finalized' AND yes_bid > 30
    )
    SELECT td_full.*, totals.total_price
    FROM td_full
    JOIN totals ON td_full.game_key = totals.game_key
""").fetchdf()

print(f"\n  NFL TD props matched with game totals: {len(nfl_td)}")

if len(nfl_td) > 0:
    print(f"\n  By game total price (high = high-scoring expected):")
    print(f"  {'Total Price':>15s} | {'TD Props':>8s} | {'NO Win%':>8s} | {'NO ROI':>8s} | Signal")
    print("  " + "-" * 60)

    for label, lo, hi in [("Low total (<45c)", 0, 45),
                            ("Mid total (45-55c)", 45, 55),
                            ("High total (55c+)", 55, 100)]:
        sub = nfl_td[(nfl_td["total_price"] >= lo) & (nfl_td["total_price"] < hi)]
        if len(sub) < 10: continue
        no_wins = (sub["result"] == "no").sum()
        no_pct = no_wins / len(sub) * 100
        total_cost = sum((100 - p) / 100 for p in sub["td_price"])
        total_pnl = sum(
            (1.0 - (100 - p) / 100) if r == "no" else -(100 - p) / 100
            for p, r in zip(sub["td_price"], sub["result"])
        )
        roi = total_pnl / total_cost * 100 if total_cost > 0 else 0
        signal = "INCREASE BET" if roi > 50 else ("NORMAL" if roi > 0 else "REDUCE")
        print(f"  {label:>15s} | {len(sub):>8d} | {no_pct:>7.1f}% | {roi:>+7.1f}% | {signal}")

print("\n" + "=" * 90)
print("SUMMARY: Cross-Market Signals")
print("=" * 90)
