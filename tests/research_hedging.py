"""
Research: Can we hedge our NO bets with spread bets to minimize losses?

The idea:
- Main bet: Buy NO on game winner (our edge play)
- Hedge bet: Buy YES on the same favorite's spread (pays if favorite wins big)

Scenarios:
A) Underdog wins → NO bet wins big, spread hedge loses → NET PROFIT
B) Favorite wins big (covers spread) → NO bet loses, spread wins → BREAK EVEN or small profit
C) Favorite wins close (doesn't cover) → BOTH LOSE → worst case

The question: Is scenario C rare enough that the hedge is worth it?
"""
import duckdb
con = duckdb.connect()
mp = "data/trevorjs/markets-*.parquet"
tp = "data/trevorjs/trades-*.parquet"

print("=" * 90)
print("HEDGING RESEARCH: Can spread bets offset moneyline losses?")
print("=" * 90)

# First: What spread markets exist alongside game winners?
print("\n1. SPREAD MARKETS AVAILABLE PER GAME")
rows = con.sql(f"""
    WITH game_winners AS (
        SELECT event_ticker, ticker, result
        FROM '{mp}'
        WHERE event_ticker LIKE 'KXNBAGAME%' AND status='finalized'
    ),
    spreads AS (
        SELECT event_ticker, ticker, result, title
        FROM '{mp}'
        WHERE event_ticker LIKE 'KXNBASPREAD%' AND status='finalized'
    ),
    matched AS (
        SELECT
            SPLIT_PART(game_winners.event_ticker, '-', 2) as game_key,
            COUNT(DISTINCT spreads.ticker) as spread_markets
        FROM game_winners
        JOIN spreads ON SPLIT_PART(game_winners.event_ticker, '-', 2) = SPLIT_PART(spreads.event_ticker, '-', 2)
        GROUP BY game_key
    )
    SELECT AVG(spread_markets) as avg_spreads, MIN(spread_markets) as min_sp, MAX(spread_markets) as max_sp, COUNT(*) as games
    FROM matched
""").fetchone()
print(f"   Games with both ML + Spread: {rows[3]}")
print(f"   Spread markets per game: avg={rows[0]:.1f}, min={rows[1]}, max={rows[2]}")

# Look at actual spread titles to understand what's available
print("\n   Sample spread markets:")
rows2 = con.sql(f"""
    SELECT ticker, title, result, yes_bid, volume
    FROM '{mp}'
    WHERE event_ticker LIKE 'KXNBASPREAD%' AND status='finalized' AND volume > 1000
    ORDER BY volume DESC
    LIMIT 10
""").fetchall()
for r in rows2:
    print(f"   {r[0][:45]:45s} | {r[1][:50]:50s} | res={r[2]} | vol={r[4]:,}")

# 2. THE KEY QUESTION: When favorites win, how often do they cover various spreads?
print("\n\n2. WHEN NBA FAVORITES WIN, BY HOW MUCH?")
print("   (This determines if a spread hedge would have paid off)")

rows3 = con.sql(f"""
    WITH gw AS (
        SELECT ticker, event_ticker, result,
               SPLIT_PART(ticker, '-', 3) as team,
               SPLIT_PART(event_ticker, '-', 2) as game_key
        FROM '{mp}'
        WHERE event_ticker LIKE 'KXNBAGAME%' AND status='finalized' AND result='yes'
    ),
    ft AS (
        SELECT t.ticker, t.yes_price,
               ROW_NUMBER() OVER (PARTITION BY t.ticker ORDER BY t.created_time) as rn
        FROM '{tp}' t WHERE t.ticker IN (SELECT ticker FROM gw) AND t.created_time >= '2025-01-01'
    ),
    with_price AS (
        SELECT gw.game_key, gw.team, ft.yes_price
        FROM gw JOIN ft ON gw.ticker = ft.ticker
        WHERE ft.rn = 1 AND ft.yes_price BETWEEN 61 AND 95
    ),
    -- Now get the spread results for the same game
    spreads AS (
        SELECT ticker, event_ticker, result, title,
               SPLIT_PART(event_ticker, '-', 2) as game_key
        FROM '{mp}'
        WHERE event_ticker LIKE 'KXNBASPREAD%' AND status='finalized'
    )
    SELECT
        wp.yes_price as fav_price,
        sp.title,
        sp.result as spread_result,
        COUNT(*) as cnt
    FROM with_price wp
    JOIN spreads sp ON wp.game_key = sp.game_key
    WHERE sp.title LIKE '%wins by over%' OR sp.title LIKE '%wins by%'
    GROUP BY wp.yes_price, sp.title, sp.result
    HAVING COUNT(*) >= 3
    ORDER BY wp.yes_price, sp.title
    LIMIT 30
""").fetchall()

if rows3:
    print("   When favorites win, how often do they cover spreads?")
    for r in rows3:
        print(f"   Price {r[0]}c | {r[1][:40]:40s} | result={r[2]} | count={r[3]}")
else:
    print("   (Need different query structure for spread analysis)")

# Alternative: Use final scores to calculate margins
print("\n\n3. MARGIN OF VICTORY WHEN FAVORITES WIN (from sportsbook data)")
print("   Using SBR 10-year NBA dataset")

import json
with open("data/sportsbook/nba_archive_10Y.json") as f:
    nba = json.load(f)

# For games where the moneyline favorite won
margins = []
for game in nba:
    hml = game.get("home_close_ml")
    aml = game.get("away_close_ml")
    hf = game.get("home_final")
    af = game.get("away_final")
    if hml is None or aml is None or hf is None or af is None: continue
    try:
        hs = int(hf); as_ = int(af); hml = int(hml); aml = int(aml)
    except: continue

    # Determine favorite
    if hml < 0 and aml > 0:  # Home is favorite
        fav_ml = hml
        fav_won = hs > as_
        margin = hs - as_ if fav_won else as_ - hs
        fav_cents = int(abs(hml) / (abs(hml) + 100) * 100)
    elif aml < 0 and hml > 0:  # Away is favorite
        fav_ml = aml
        fav_won = as_ > hs
        margin = as_ - hs if fav_won else hs - as_
        fav_cents = int(abs(aml) / (abs(aml) + 100) * 100)
    else:
        continue

    if not fav_won: continue  # Only looking at games where favorite DID win
    if fav_cents < 61 or fav_cents > 90: continue

    margins.append({"cents": fav_cents, "margin": margin})

print(f"   Games where favorite won (61-90c): {len(margins)}")
print()
print(f"   {'Price':>8s} | {'Games':>6s} | {'Avg Margin':>10s} | {'Win by 1-3':>10s} | {'Win by 4-7':>10s} | {'Win by 8+':>9s}")
print("   " + "-" * 65)

for price_lo in range(60, 91, 5):
    bucket = [m for m in margins if price_lo <= m["cents"] < price_lo + 5]
    if len(bucket) < 20: continue
    avg_margin = sum(m["margin"] for m in bucket) / len(bucket)
    close = sum(1 for m in bucket if m["margin"] <= 3) / len(bucket) * 100
    medium = sum(1 for m in bucket if 4 <= m["margin"] <= 7) / len(bucket) * 100
    blowout = sum(1 for m in bucket if m["margin"] >= 8) / len(bucket) * 100
    print(f"   {price_lo}-{price_lo+4}c | {len(bucket):>6d} | {avg_margin:>9.1f} pts | {close:>9.1f}% | {medium:>9.1f}% | {blowout:>8.1f}%")

# 4. HEDGE MATH: What would a hedged position look like?
print("\n\n4. HEDGE SIMULATION")
print("   Main bet: Buy NO on 75c favorite (cost $0.25)")
print("   Hedge: Buy YES on favorite -5.5 spread (cost ~$0.55)")
print()

# At 75c, favorite wins ~70% of the time
# When favorite wins, they win by 5+ about 55% of the time (from data above)
# So:
fav_price = 75
no_cost = (100 - fav_price) / 100  # 0.25
fav_win_rate = 0.70  # From sportsbook data at 75c
spread_cost = 0.55  # Typical -5.5 spread price

# Scenario probabilities
p_underdog_wins = 1 - fav_win_rate  # 30%
p_fav_blowout = fav_win_rate * 0.55  # 70% * 55% = 38.5% (wins by 6+)
p_fav_close = fav_win_rate * 0.45  # 70% * 45% = 31.5% (wins by 1-5)

print(f"   Scenario A: Underdog wins (30%)")
pnl_a = (1.0 - no_cost) - spread_cost  # NO wins, spread loses
print(f"     NO wins +${1.0-no_cost:.2f}, spread loses -${spread_cost:.2f} = ${pnl_a:+.2f}")

print(f"   Scenario B: Favorite blowout, covers spread (38.5%)")
pnl_b = -no_cost + (1.0 - spread_cost)  # NO loses, spread wins
print(f"     NO loses -${no_cost:.2f}, spread wins +${1.0-spread_cost:.2f} = ${pnl_b:+.2f}")

print(f"   Scenario C: Favorite wins close, NO cover (31.5%) — THE DANGER")
pnl_c = -no_cost - spread_cost  # Both lose
print(f"     NO loses -${no_cost:.2f}, spread loses -${spread_cost:.2f} = ${pnl_c:+.2f}")

ev_hedged = p_underdog_wins * pnl_a + p_fav_blowout * pnl_b + p_fav_close * pnl_c
ev_unhedged = p_underdog_wins * (1.0 - no_cost) + (1 - p_underdog_wins) * (-no_cost)

print(f"\n   Expected Value:")
print(f"     Unhedged (NO only): ${ev_unhedged:+.4f} per trade")
print(f"     Hedged (NO + spread): ${ev_hedged:+.4f} per trade")
print(f"     Difference: ${ev_hedged - ev_unhedged:+.4f}")

print(f"\n   Worst case:")
print(f"     Unhedged: lose ${no_cost:.2f} (100% of bet)")
print(f"     Hedged: lose ${no_cost + spread_cost:.2f} ({(no_cost+spread_cost)/no_cost*100:.0f}% of original bet!)")

# 5. BETTER APPROACH: Partial hedge
print("\n\n5. PARTIAL HEDGE (bet only 25-50% of NO cost on spread)")

for hedge_pct in [0, 0.25, 0.50, 0.75, 1.0]:
    hedge_size = no_cost * hedge_pct
    spread_contracts = hedge_size / spread_cost if spread_cost > 0 else 0

    pnl_a = (1.0 - no_cost) - hedge_size  # Underdog wins
    pnl_b = -no_cost + spread_contracts * (1.0 - spread_cost)  # Fav blowout
    pnl_c = -no_cost - hedge_size  # Fav close win (gray zone)

    ev = p_underdog_wins * pnl_a + p_fav_blowout * pnl_b + p_fav_close * pnl_c

    print(f"   Hedge {hedge_pct*100:>3.0f}% of NO cost (${hedge_size:.2f} on spread):")
    print(f"     A) Underdog wins: ${pnl_a:+.3f} | B) Blowout: ${pnl_b:+.3f} | C) Close win: ${pnl_c:+.3f}")
    print(f"     EV: ${ev:+.4f} | Worst case: ${pnl_c:+.3f}")
    print()

# 6. ALTERNATIVE: Buy the opposite game winner at a low price
print("\n6. ALTERNATIVE HEDGE: Buy YES on the underdog at a discount")
print("   If favorite is at 75c YES, underdog is at ~25c YES")
print("   We buy NO on favorite ($0.25 cost)")
print("   We also buy YES on underdog ($0.25 cost)")
print("   These are the SAME BET (NO on favorite = YES on underdog)")
print("   So this doesn't hedge — it just doubles the position!")

print("\n7. REAL ALTERNATIVE: Don't hedge — just reduce bet size")
print("   Instead of $20 NO bet + $10 spread hedge = $30 total risk")
print("   Just bet $15 NO with no hedge = $15 total risk")
print("   Same risk reduction, higher EV (no hedge drag)")

print("\n" + "=" * 90)
print("CONCLUSION")
print("=" * 90)
