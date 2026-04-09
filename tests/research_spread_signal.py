"""
Research: Can spread market prices predict which moneyline bets win?

If the spread implies a closer game than the moneyline, the favorite
might be overpriced → better NO bet. If spread implies blowout,
favorite is correctly priced → worse NO bet.
"""
import duckdb
con = duckdb.connect()
mp = "data/trevorjs/markets-*.parquet"
tp = "data/trevorjs/trades-*.parquet"

print("=" * 90)
print("SPREAD AS SIGNAL: Does spread market divergence predict upsets?")
print("=" * 90)

# Match game winners with their spread markets
# Game winner: KXNBAGAME-26JAN15MEMORL-ORL (event: KXNBAGAME-26JAN15MEMORL)
# Spread: KXNBASPREAD-26JAN15MEMORL-ORL5 (event: KXNBASPREAD-26JAN15MEMORL)
# The game_key is the same: 26JAN15MEMORL

print("\n1. Matching moneyline + spread for same games...")

matched = con.sql(f"""
    WITH ml AS (
        SELECT ticker, event_ticker, result,
               SPLIT_PART(event_ticker, '-', 2) as game_key,
               SPLIT_PART(ticker, '-', 3) as team
        FROM '{mp}'
        WHERE event_ticker LIKE 'KXNBAGAME%' AND status='finalized' AND result IN ('yes','no')
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
    -- Get the LOWEST spread for the same team (closest spread = most relevant)
    spreads AS (
        SELECT ticker, event_ticker, result, title,
               SPLIT_PART(event_ticker, '-', 2) as game_key,
               -- Extract team from ticker (e.g., ORL5 -> first 2-3 alpha chars)
               REGEXP_EXTRACT(SPLIT_PART(ticker, '-', 3), '^[A-Z]+') as sp_team,
               -- Extract spread number (e.g., ORL5 -> 5)
               CAST(REGEXP_EXTRACT(SPLIT_PART(ticker, '-', 3), '[0-9]+') AS INT) as spread_pts
        FROM '{mp}'
        WHERE event_ticker LIKE 'KXNBASPREAD%' AND status='finalized'
    ),
    sp_prices AS (
        SELECT t.ticker, t.yes_price as sp_price,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
        FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM spreads) AND t.created_time >= '2025-01-01'
    ),
    sp_full AS (
        SELECT spreads.game_key, spreads.sp_team, spreads.spread_pts, spreads.result as sp_result,
               sp_prices.sp_price
        FROM spreads JOIN sp_prices ON spreads.ticker = sp_prices.ticker
        WHERE sp_prices.rn = 1
    ),
    -- Match: get the smallest spread for each game/team combo
    matched AS (
        SELECT ml_full.*, sp_full.spread_pts, sp_full.sp_price, sp_full.sp_result,
               ROW_NUMBER() OVER (PARTITION BY ml_full.ticker ORDER BY sp_full.spread_pts) as spread_rank
        FROM ml_full
        JOIN sp_full ON ml_full.game_key = sp_full.game_key AND ml_full.team = sp_full.sp_team
    )
    SELECT * FROM matched WHERE spread_rank = 1
""").fetchdf()

print(f"   Matched games: {len(matched)}")
if len(matched) == 0:
    print("   No matches found. Trying alternative matching...")
    # Try matching by game key only, get any spread
    matched = con.sql(f"""
        WITH ml AS (
            SELECT ticker, event_ticker, result,
                   SPLIT_PART(event_ticker, '-', 2) as game_key,
                   SPLIT_PART(ticker, '-', 3) as team
            FROM '{mp}'
            WHERE event_ticker LIKE 'KXNBAGAME%' AND status='finalized' AND result IN ('yes','no')
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
            SELECT event_ticker, result as sp_result, yes_bid as sp_price,
                   SPLIT_PART(event_ticker, '-', 2) as game_key,
                   REGEXP_EXTRACT(SPLIT_PART(ticker, '-', 3), '^[A-Z]+') as sp_team,
                   CAST(NULLIF(REGEXP_EXTRACT(SPLIT_PART(ticker, '-', 3), '[0-9]+'), '') AS INT) as spread_pts
            FROM '{mp}'
            WHERE event_ticker LIKE 'KXNBASPREAD%' AND status='finalized' AND sp_price > 0
        )
        SELECT ml_full.ticker, ml_full.game_key, ml_full.team, ml_full.result,
               ml_full.ml_price,
               spreads.spread_pts, spreads.sp_price, spreads.sp_result, spreads.sp_team
        FROM ml_full
        JOIN spreads ON ml_full.game_key = spreads.game_key AND ml_full.team = spreads.sp_team
        WHERE spreads.spread_pts IS NOT NULL
    """).fetchdf()
    print(f"   Alternative match: {len(matched)}")

if len(matched) > 0:
    print(f"\n   Sample matched data:")
    for _, row in matched.head(5).iterrows():
        print(f"   {row['team']} | ML={row['ml_price']}c | Spread=-{row['spread_pts']}.5 at {row['sp_price']}c | ML result={row['result']}")

    # 2. THE SIGNAL: Compare spread implied prob vs moneyline
    # If spread price is LOW (game expected to be close) -> better for NO bet
    # If spread price is HIGH (blowout expected) -> worse for NO bet
    print(f"\n\n2. DOES SPREAD PRICE PREDICT MONEYLINE OUTCOMES?")

    # Group by spread price buckets
    print(f"\n   For games where ML favorite is 65-95c:")
    print(f"   How does the SPREAD price affect the underdog's chances?")
    print(f"   (Low spread price = market thinks game is closer)")

    matched["spread_bucket"] = matched["sp_price"].apply(
        lambda x: "Low spread (<40c)" if x < 40
        else ("Mid spread (40-60c)" if x < 60
        else "High spread (60c+)")
    )

    print(f"\n   {'Spread Price':>22s} | {'Games':>6s} | {'NO Wins':>8s} | {'NO Win%':>8s} | {'NO ROI':>8s} | Signal")
    print("   " + "-" * 75)

    for bucket in ["Low spread (<40c)", "Mid spread (40-60c)", "High spread (60c+)"]:
        sub = matched[matched["spread_bucket"] == bucket]
        if len(sub) < 10: continue
        no_wins = (sub["result"] == "no").sum()
        no_pct = no_wins / len(sub) * 100
        # ROI
        total_cost = sum((100 - p) / 100 for p in sub["ml_price"])
        total_pnl = sum(
            (1.0 - (100 - p) / 100) if r == "no" else -(100 - p) / 100
            for p, r in zip(sub["ml_price"], sub["result"])
        )
        roi = total_pnl / total_cost * 100 if total_cost > 0 else 0
        signal = "INCREASE BET" if roi > 30 else ("NORMAL" if roi > 0 else "SKIP")
        print(f"   {bucket:>22s} | {len(sub):>6d} | {no_wins:>8d} | {no_pct:>7.1f}% | {roi:>+7.1f}% | {signal}")

    # 3. More granular: per spread point
    print(f"\n\n3. GRANULAR: By specific spread level")
    print(f"   {'Spread':>10s} | {'Games':>6s} | {'NO Win%':>8s} | {'NO ROI':>8s}")
    print("   " + "-" * 45)

    for pts in sorted(matched["spread_pts"].unique()):
        sub = matched[matched["spread_pts"] == pts]
        if len(sub) < 10: continue
        no_wins = (sub["result"] == "no").sum()
        no_pct = no_wins / len(sub) * 100
        total_cost = sum((100 - p) / 100 for p in sub["ml_price"])
        total_pnl = sum(
            (1.0 - (100 - p) / 100) if r == "no" else -(100 - p) / 100
            for p, r in zip(sub["ml_price"], sub["result"])
        )
        roi = total_pnl / total_cost * 100 if total_cost > 0 else 0
        print(f"   -{pts:>7.1f}.5 | {len(sub):>6d} | {no_pct:>7.1f}% | {roi:>+7.1f}%")

    # 4. THE KEY TEST: When spread DISAGREES with moneyline
    print(f"\n\n4. DIVERGENCE SIGNAL")
    print(f"   When spread suggests game is CLOSER than moneyline implies")

    # For each game, calculate: expected spread price given the moneyline
    # A 75c ML favorite "should" have a -5.5 spread at ~55c
    # If the actual spread is < 45c, the spread market thinks it's closer
    matched["ml_implied_spread_price"] = matched["ml_price"].apply(
        lambda x: max(30, x - 20)  # rough approximation
    )
    matched["divergence"] = matched["sp_price"] - matched["ml_implied_spread_price"]

    print(f"   Divergence = actual spread price - expected spread price")
    print(f"   Negative = spread thinks game is CLOSER (good for NO)")
    print(f"   Positive = spread thinks BLOWOUT (bad for NO)")

    for lo, hi, label in [(-100, -15, "Strong disagree (closer)"),
                           (-15, -5, "Slight disagree (closer)"),
                           (-5, 5, "Agree (neutral)"),
                           (5, 15, "Slight disagree (blowout)"),
                           (15, 100, "Strong disagree (blowout)")]:
        sub = matched[(matched["divergence"] >= lo) & (matched["divergence"] < hi)]
        if len(sub) < 5: continue
        no_wins = (sub["result"] == "no").sum()
        no_pct = no_wins / len(sub) * 100
        total_cost = sum((100 - p) / 100 for p in sub["ml_price"])
        total_pnl = sum(
            (1.0 - (100 - p) / 100) if r == "no" else -(100 - p) / 100
            for p, r in zip(sub["ml_price"], sub["result"])
        )
        roi = total_pnl / total_cost * 100 if total_cost > 0 else 0
        print(f"   {label:30s}: {len(sub):>4d} games | NO wins {no_pct:>5.1f}% | ROI {roi:>+7.1f}%")

else:
    print("\n   Could not match spread data to moneyline data.")
    print("   The spread analysis would need cross-market matching on the live agent.")

print("\n" + "=" * 90)
