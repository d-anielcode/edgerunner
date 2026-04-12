"""
EdgeRunner Edge Table Validation -- Feb 1 - Apr 9, 2026
Fetches settled market data + first-trade opening prices from the Kalshi public API.

Two-phase approach:
  Phase 1: Fetch all settled markets for target series tickers (paginated)
  Phase 2: For each market, fetch the FIRST trade to get the opening YES price
           (previous_price_dollars snaps to 1c/99c after settlement)
  Phase 3: Per-sport, per-bucket analysis comparing actual vs expected YES rates

Usage:
    python validate_edge_tables_feb_apr_2026.py
"""

import asyncio
import sys
import time
from datetime import datetime, timezone

import httpx
import numpy as np

# Force UTF-8 output on Windows
sys.stdout.reconfigure(encoding="utf-8")

# ── Config ────────────────────────────────────────────────────────────────────

BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

SERIES_TICKERS = [
    "KXNBAGAME",
    "KXNHLGAME",
    "KXNFLANYTD",
    "KXNHLSPREAD",
    "KXNBASPREAD",
    "KXNFLSPREAD",
    "KXHIGHNY",
    "KXHIGHCHI",
    "KXHIGHMIA",
    "KXHIGHLA",
    "KXEPLGAME",
    "KXUCLGAME",
    "KXUFCFIGHT",
]

DATE_START = datetime(2026, 2, 1, tzinfo=timezone.utc)
DATE_END = datetime(2026, 4, 10, tzinfo=timezone.utc)

# Concurrency for trade fetching
MAX_CONCURRENT = 15  # Stay under 20 reads/sec basic tier
RATE_LIMIT_DELAY = 0.07  # seconds between request dispatches

# ── Sport mapping ─────────────────────────────────────────────────────────────

SPORT_PREFIXES = {
    "NBA": "KXNBAGAME",
    "NHL": "KXNHLGAME",
    "EPL": "KXEPLGAME",
    "UCL": "KXUCLGAME",
    "UFC": "KXUFCFIGHT",
    "WEATHER": "KXHIGH",
    "NFLTD": "KXNFLANYTD",
    "NHLSPREAD": "KXNHLSPREAD",
    "NBASPREAD": "KXNBASPREAD",
    "NFLSPREAD": "KXNFLSPREAD",
}

EDGE_BUCKETS = {
    "NBA": [(55, 59, None), (60, 64, 0.50), (65, 69, 0.48), (70, 74, 0.46),
            (75, 79, 0.44), (80, 84, 0.42), (85, 89, 0.40)],
    "NHL": [(55, 59, None), (60, 64, 0.55), (65, 69, 0.535), (70, 74, 0.52),
            (75, 79, 0.505), (80, 84, 0.49), (85, 89, 0.475)],
    "EPL": [(71, 85, 0.485)],
    "UCL": [(66, 70, 0.400), (76, 85, 0.641)],
    "UFC": [(76, 85, 0.622)],
    "WEATHER": [(55, 65, 0.404), (66, 75, 0.417), (76, 85, 0.417), (86, 95, 0.419)],
    "NFLTD": [(55, 65, 0.492), (66, 75, 0.452), (76, 85, 0.545), (86, 95, 0.286)],
    "NHLSPREAD": [(55, 65, 0.500), (66, 75, 0.450), (76, 90, 0.400)],
    "NBASPREAD": [(55, 65, 0.480), (66, 75, 0.440), (76, 90, 0.380)],
    "NFLSPREAD": [(55, 65, 0.480), (66, 75, 0.440), (76, 90, 0.380)],
}


def get_expected_yes_rate(sport: str, yes_price: int) -> float | None:
    if sport == "NBA":
        if yes_price < 60 or yes_price > 89:
            return None
        return 0.50 - (yes_price - 60) * 0.004
    if sport == "NHL":
        if yes_price < 60 or yes_price > 89:
            return None
        return 0.55 - (yes_price - 60) * 0.003
    buckets = EDGE_BUCKETS.get(sport, [])
    for lo, hi, rate in buckets:
        if lo <= yes_price <= hi and rate is not None:
            return rate
    return None


def assign_sport(ticker: str) -> str | None:
    for sport, prefix in SPORT_PREFIXES.items():
        if ticker.startswith(prefix):
            return sport
    return None


# ── Phase 1: Fetch all settled markets ────────────────────────────────────────

def fetch_settled_markets_sync(series_ticker: str, client: httpx.Client) -> list[dict]:
    all_markets = []
    cursor = None
    while True:
        params = {"series_ticker": series_ticker, "status": "settled", "limit": 200}
        if cursor:
            params["cursor"] = cursor
        resp = client.get(f"{BASE_URL}/markets", params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        markets = data.get("markets", [])
        if not markets:
            break
        all_markets.extend(markets)
        cursor = data.get("cursor")
        if not cursor:
            break
        time.sleep(0.15)
    return all_markets


# ── Phase 2: Fetch first trade per ticker (async with concurrency) ────────────

async def fetch_first_trade(
    ticker: str,
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    rate_limiter: asyncio.Lock,
) -> tuple[str, float | None]:
    """Fetch the opening YES price for a ticker by getting trades oldest-first."""
    async with semaphore:
        # Rate limit: small delay between dispatches
        async with rate_limiter:
            await asyncio.sleep(RATE_LIMIT_DELAY)

        try:
            # Get trades for this ticker. The API returns newest first.
            # We need to paginate to the end to find the oldest trade, which is slow.
            # Instead, use a trick: fetch with limit=1000 and take the last one.
            # For most markets, there are <1000 trades in the first few hours.
            # But some have 6000+ trades. Let's use a different approach:
            # Fetch trades and paginate until we get to the oldest.
            # Actually, let's just get the last page.

            # Strategy: get first batch. If there's a cursor, keep paginating.
            # For efficiency, we want the OLDEST trade. Unfortunately, the API
            # returns newest-first. We must paginate to the end.
            #
            # Optimization: for most markets, we just need ANY early trade.
            # The first batch's last entry (oldest in batch) is often close enough.
            # Let's use limit=1000 and take the oldest from that batch.

            resp = await client.get(
                f"{BASE_URL}/markets/trades",
                params={"ticker": ticker, "limit": 1000},
                timeout=30,
            )

            if resp.status_code == 429:
                # Rate limited - wait and retry once
                await asyncio.sleep(2)
                resp = await client.get(
                    f"{BASE_URL}/markets/trades",
                    params={"ticker": ticker, "limit": 1000},
                    timeout=30,
                )

            if resp.status_code != 200:
                return (ticker, None)

            data = resp.json()
            trades = data.get("trades", [])
            if not trades:
                return (ticker, None)

            cursor = data.get("cursor")

            # If there are more pages, keep going to find the oldest trade
            while cursor:
                resp2 = await client.get(
                    f"{BASE_URL}/markets/trades",
                    params={"ticker": ticker, "limit": 1000, "cursor": cursor},
                    timeout=30,
                )
                if resp2.status_code == 429:
                    await asyncio.sleep(2)
                    resp2 = await client.get(
                        f"{BASE_URL}/markets/trades",
                        params={"ticker": ticker, "limit": 1000, "cursor": cursor},
                        timeout=30,
                    )
                if resp2.status_code != 200:
                    break
                data2 = resp2.json()
                more = data2.get("trades", [])
                if not more:
                    break
                trades.extend(more)
                cursor = data2.get("cursor")

            # Oldest trade is last in the list (newest-first ordering)
            oldest = trades[-1]
            yes_price = float(oldest.get("yes_price_dollars", "0"))
            return (ticker, yes_price)

        except Exception as e:
            return (ticker, None)


async def fetch_all_first_trades(tickers: list[str]) -> dict[str, float]:
    """Fetch opening YES price for all tickers concurrently."""
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    rate_limiter = asyncio.Lock()

    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as client:
        tasks = [
            fetch_first_trade(ticker, client, semaphore, rate_limiter)
            for ticker in tickers
        ]

        results = {}
        done = 0
        total = len(tasks)
        start = time.time()

        for coro in asyncio.as_completed(tasks):
            ticker, price = await coro
            if price is not None:
                results[ticker] = price
            done += 1
            if done % 100 == 0 or done == total:
                elapsed = time.time() - start
                rate = done / elapsed if elapsed > 0 else 0
                print(f"  [{done}/{total}] {rate:.1f} req/s, {len(results)} with price", flush=True)

    return results


# ── Phase 3: Analysis ─────────────────────────────────────────────────────────

def run_analysis(records: list[dict]):
    sport_records = [r for r in records if r["sport"] is not None]
    print(f"\nRecords with known sport + opening price: {len(sport_records)}")

    sport_counts = {}
    for r in sport_records:
        sport_counts[r["sport"]] = sport_counts.get(r["sport"], 0) + 1
    print("\nBy sport:")
    for sport in sorted(sport_counts.keys()):
        print(f"  {sport:<12} {sport_counts[sport]:>5}")

    # Price distribution check
    print("\nPrice distribution (opening YES price in cents):")
    prices = [r["yes_price"] for r in sport_records]
    for lo in range(0, 100, 10):
        hi = lo + 9
        n = sum(1 for p in prices if lo <= p <= hi)
        if n > 0:
            print(f"  {lo:>2}-{hi:<2}c: {n:>5} markets")

    # Bucket analysis
    print(f"\n{'=' * 70}")
    print("EDGE TABLE VALIDATION -- FEB 1 to APR 9, 2026 (Kalshi API)")
    print("=" * 70)

    VERDICT_THRESHOLD = 0.05
    summary_rows = []

    for sport in sorted(SPORT_PREFIXES.keys()):
        sdata = [r for r in sport_records if r["sport"] == sport]
        if not sdata:
            print(f"\n{'-' * 60}")
            print(f"SPORT: {sport}  -- NO DATA")
            continue

        if sport in ("NBA", "NHL"):
            buckets = [(lo, lo + 4) for lo in range(55, 90, 5)]
        else:
            buckets = [(lo, hi) for lo, hi, rate in EDGE_BUCKETS.get(sport, []) if rate is not None]

        print(f"\n{'-' * 60}")
        print(f"SPORT: {sport}  ({len(sdata)} tickers in Feb-Apr 2026)")
        print(f"  {'Bucket':>10} {'N':>6} {'ActualYES':>10} {'ExpectedYES':>12} {'Gap':>8} {'SimROI(NO)':>12}")
        print(f"  {'':->10} {'':->6} {'':->10} {'':->12} {'':->8} {'':->12}")

        sport_rows = []
        for lo, hi in buckets:
            bdata = [r for r in sdata if lo <= r["yes_price"] <= hi]
            n = len(bdata)
            if n == 0:
                continue

            actual_yes = sum(1 for r in bdata if r["result"] == "yes") / n
            mid_price = (lo + hi) / 2
            exp_yes = get_expected_yes_rate(sport, int(mid_price))
            if exp_yes is None:
                continue

            gap = actual_yes - exp_yes
            avg_yes_price = sum(r["yes_price"] for r in bdata) / n
            avg_no_price = 100 - avg_yes_price
            actual_no = 1 - actual_yes
            sim_roi = (actual_no * avg_yes_price - actual_yes * avg_no_price) / avg_no_price if avg_no_price > 0 else 0.0

            print(f"  {lo:>3}-{hi:<3}    {n:>6,}   {actual_yes:>8.3f}   {exp_yes:>10.3f}   {gap:>+7.3f}   {sim_roi:>+10.3f}")
            row = {"sport": sport, "bucket": f"{lo}-{hi}", "n": n,
                   "actual_yes": actual_yes, "exp_yes": exp_yes, "gap": gap, "sim_roi": sim_roi}
            sport_rows.append(row)
            summary_rows.append(row)

        if sport_rows:
            buckets_with_data = [r for r in sport_rows if r["n"] >= 10]
            if not buckets_with_data:
                verdict = "INSUFFICIENT DATA (all buckets < 10 markets)"
            else:
                avg_gap = np.mean([r["gap"] for r in buckets_with_data])
                max_abs_gap = max(abs(r["gap"]) for r in buckets_with_data)
                if max_abs_gap <= VERDICT_THRESHOLD:
                    verdict = "CONFIRMED  (actual within 5pp of expected)"
                elif avg_gap < -VERDICT_THRESHOLD:
                    verdict = "BETTER THAN EXPECTED  (YES < predicted => edge is BIGGER)"
                elif avg_gap > VERDICT_THRESHOLD:
                    verdict = "EDGE DECAYED  (YES > predicted => favorites win MORE)"
                else:
                    verdict = f"MIXED  (avg gap {avg_gap:+.3f}, max abs {max_abs_gap:.3f})"
            print(f"\n  >> VERDICT: {verdict}")

    # Master summary
    print(f"\n{'=' * 70}")
    print("MASTER SUMMARY TABLE")
    print("=" * 70)
    if summary_rows:
        print(f"{'Sport':<12} {'Bucket':>8} {'N':>6} {'ActYES':>8} {'ExpYES':>8} {'Gap':>8} {'SimROI':>8}")
        print(f"{'':->12} {'':->8} {'':->6} {'':->8} {'':->8} {'':->8} {'':->8}")
        for r in summary_rows:
            print(f"{r['sport']:<12} {r['bucket']:>8} {r['n']:>6} "
                  f"{r['actual_yes']:>+7.3f} {r['exp_yes']:>+7.3f} "
                  f"{r['gap']:>+7.3f} {r['sim_roi']:>+7.3f}")

    # Sport-level verdict
    print(f"\n{'=' * 70}")
    print("SPORT-LEVEL VERDICT SUMMARY")
    print("=" * 70)
    for sport in sorted(SPORT_PREFIXES.keys()):
        rows = [r for r in summary_rows if r["sport"] == sport and r["n"] >= 10]
        if not rows:
            print(f"  {sport:<12} NO DATA / INSUFFICIENT")
            continue
        avg_gap = np.mean([r["gap"] for r in rows])
        max_abs_gap = max(abs(r["gap"]) for r in rows)
        total_n = sum(r["n"] for r in rows)
        avg_roi = np.mean([r["sim_roi"] for r in rows])
        if max_abs_gap <= VERDICT_THRESHOLD:
            v = "CONFIRMED"
        elif avg_gap < -VERDICT_THRESHOLD:
            v = "BETTER THAN EXPECTED"
        elif avg_gap > VERDICT_THRESHOLD:
            v = "EDGE DECAYED"
        else:
            v = "MIXED"
        print(f"  {sport:<12} N={total_n:>5,}  avgGap={avg_gap:+.4f}  avgSimROI={avg_roi:+.4f}  => {v}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("KALSHI EDGE TABLE VALIDATION -- Feb 1 to Apr 9, 2026")
    print("Phase 1: Fetching settled markets...")
    print("=" * 70)

    sync_client = httpx.Client()
    all_market_info = []  # list of (ticker, result, close_time, sport)

    for series in SERIES_TICKERS:
        print(f"\n  Fetching {series}...", end=" ", flush=True)
        markets = fetch_settled_markets_sync(series, sync_client)
        print(f"{len(markets)} total", end="")

        in_range = []
        for m in markets:
            close_str = m.get("close_time", "")
            if not close_str:
                continue
            try:
                close_dt = datetime.fromisoformat(close_str.replace("Z", "+00:00"))
            except ValueError:
                continue
            if DATE_START <= close_dt < DATE_END:
                in_range.append(m)

        print(f" -> {len(in_range)} in Feb-Apr")

        for m in in_range:
            ticker = m["ticker"]
            result = m.get("result", "").lower().strip()
            if result not in ("yes", "no"):
                continue
            sport = assign_sport(ticker)
            all_market_info.append({
                "ticker": ticker,
                "result": result,
                "close_time": m.get("close_time", ""),
                "sport": sport,
            })

    sync_client.close()

    tickers_to_fetch = [m["ticker"] for m in all_market_info]
    print(f"\n{'=' * 70}")
    print(f"Phase 2: Fetching first-trade opening prices for {len(tickers_to_fetch)} tickers...")
    print(f"  Using {MAX_CONCURRENT} concurrent connections")
    print("=" * 70)

    t0 = time.time()
    opening_prices = asyncio.run(fetch_all_first_trades(tickers_to_fetch))
    elapsed = time.time() - t0
    print(f"\n  Fetched opening prices for {len(opening_prices)}/{len(tickers_to_fetch)} tickers in {elapsed:.1f}s")

    # Build final records with opening price
    records = []
    no_price_count = 0
    for m in all_market_info:
        ticker = m["ticker"]
        price_dollars = opening_prices.get(ticker)
        if price_dollars is None or price_dollars <= 0:
            no_price_count += 1
            continue
        yes_price_cents = round(price_dollars * 100)
        records.append({
            "ticker": ticker,
            "yes_price": yes_price_cents,
            "result": m["result"],
            "close_time": m["close_time"],
            "sport": m["sport"],
        })

    print(f"  Records with opening price: {len(records)} ({no_price_count} missing)")

    # Sample
    print(f"\n  Sample records (first 10):")
    for r in records[:10]:
        print(f"    {r['ticker']:<50} open={r['yes_price']:>3}c  result={r['result']:<3}  sport={r['sport']}")

    # Phase 3
    print(f"\n{'=' * 70}")
    print("Phase 3: Edge table validation analysis")
    print("=" * 70)
    run_analysis(records)
    print(f"\nDone. Total analyzed: {len(records)}")


if __name__ == "__main__":
    main()
