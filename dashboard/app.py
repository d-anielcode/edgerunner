"""
EdgeRunner Dashboard — Flask backend.

Serves a single-page trading dashboard with live data from the Kalshi API.
Run: python dashboard/app.py
"""

import json
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from flask import Flask, jsonify, render_template, request

try:
    from dashboard.kalshi_api import KalshiClient, TRADING_MODE
except ImportError:
    from kalshi_api import KalshiClient, TRADING_MODE

app = Flask(__name__)
client = KalshiClient()

# Total amount deposited into Kalshi (used for accurate P&L)
TOTAL_DEPOSITS = 410.00

# Agent start date — fills before this are personal trades, not agent trades
# Agent first started automated trading on April 4, 2026
AGENT_START_DATE = "2026-04-04"

# Project root (one level up from dashboard/)
PROJECT_ROOT = Path(__file__).parent.parent

# ---------------------------------------------------------------------------
# Sport detection from ticker prefix
# ---------------------------------------------------------------------------
SPORT_FROM_PREFIX = {
    "KXNBAGAME": "NBA",
    "KXNHLGAME": "NHL",
    "KXNBASPREAD": "NBASPREAD",
    "KXNHLSPREAD": "NHLSPREAD",
    "KXNFLANYTD": "NFLTD",
    "KXNFLSPREAD": "NFLSPREAD",
    "KXNFLTEAMTOTAL": "NFLTT",
    "KXMLBGAME": "MLB",
    "KXMLBTOTAL": "MLBTOTAL",
    "KXUCLGAME": "UCL",
    "KXWNBAGAME": "WNBA",
    "KXATPMATCH": "ATP",
    "KXCFBGAME": "CFB",
    "KXNBAPTS": "NBA_PTS",
    "KXNBA3PT": "NBA_3PT",
    "KXNBAREB": "NBA_REB",
    "KXNBAAST": "NBA_AST",
    "KXWTAMATCH": "WTA",
    "KXCBAGAME": "CBA",
    "KXLOLMAP": "LOL",
    "KXEPLGAME": "EPL",
    "KXLALIGAGAME": "LALIGA",
    "KXUFCFIGHT": "UFC",
}


def _parse_fill(fill: dict) -> tuple:
    """Parse a fill using v2 API field names. Returns (action, side, count, yes_price, no_price, fee)."""
    action = fill.get("action", "")
    side = fill.get("side", "")
    count = float(fill.get("count_fp", fill.get("count", 0)))
    yes_price = float(fill.get("yes_price_dollars", 0))
    no_price = float(fill.get("no_price_dollars", 0))
    fee = float(fill.get("fee_cost", 0))
    return action, side, count, yes_price, no_price, fee


def _compute_ticker_pnl(fills: list[dict], settlement: dict | None) -> tuple:
    """
    Compute total cost, revenue, buy_count for a list of fills on one ticker.
    Uses settlement data (no_total_cost_dollars, no_count_fp) for accurate P&L
    when available, fills for early exits.
    """
    total_cost = 0.0
    total_revenue = 0.0
    buy_count = 0.0

    for fill in fills:
        action, side, count, yes_price, no_price, fee = _parse_fill(fill)
        if action == "buy":
            cost = (no_price if side == "no" else yes_price) * count + fee
            total_cost += cost
            buy_count += count
        elif action == "sell":
            # Sell YES (closing NO position) = receive NO value
            # Sell NO (closing YES position) = receive YES value
            if side == "yes":
                rev = no_price * count - fee
            else:
                rev = yes_price * count - fee
            total_revenue += rev

    # Settlement revenue — use the 'revenue' field (in cents)
    if settlement:
        revenue_cents = settlement.get("revenue", 0)
        if revenue_cents > 0:
            total_revenue += revenue_cents / 100.0

        # If we had cost data from settlement but no fills (paginated away)
        settle_cost = float(settlement.get("no_total_cost_dollars", 0))
        if settle_cost > 0 and total_cost == 0:
            total_cost = settle_cost + float(settlement.get("fee_cost", 0))
            buy_count = float(settlement.get("no_count_fp", 0))

    return total_cost, total_revenue, buy_count


def _parse_ticker(ticker: str) -> dict:
    """
    Parse a Kalshi ticker into human-readable components.

    Examples:
        KXNBAGAME-26APR12ORLBOS-ORL → {sport: NBA, date: Apr 12, matchup: ORL vs BOS, pick: ORL}
        KXNBASPREAD-26APR12ORLBOS-ORL6 → {sport: NBA Spread, date: Apr 12, matchup: ORL vs BOS, pick: ORL +6}
        KXMLBTOTAL-26APR121335AZPHI-5 → {sport: MLB Total, date: Apr 12, matchup: AZ vs PHI, line: Over 5}
        KXNBAPTS-26APR12ORLBOS-ORLJTATUM0-25 → {sport: NBA PTS, date: Apr 12, player: J TATUM, line: 25+}
    """
    import re

    result = {"sport": _detect_sport(ticker), "date": "", "matchup": "", "pick": "", "raw": ticker}

    # Extract date: pattern is 2-digit year + 3-letter month + 2-digit day
    date_match = re.search(r'-(\d{2})([A-Z]{3})(\d{2})', ticker)
    if date_match:
        months = {'JAN': '1', 'FEB': '2', 'MAR': '3', 'APR': '4', 'MAY': '5', 'JUN': '6',
                  'JUL': '7', 'AUG': '8', 'SEP': '9', 'OCT': '10', 'NOV': '11', 'DEC': '12'}
        month_str = date_match.group(2)
        day = date_match.group(3)
        result["date"] = f"{month_str.title()} {int(day)}"

    # Extract teams/matchup: 3-letter codes after the date
    # Pattern: date followed by optional time (4 digits) then TEAMATEAMB
    teams_match = re.search(r'\d{2}[A-Z]{3}\d{2}(\d{4})?([A-Z]{2,4})([A-Z]{2,4})-', ticker)
    if teams_match:
        team_a = teams_match.group(2)
        team_b = teams_match.group(3)
        result["matchup"] = f"{team_a} vs {team_b}"

    # Extract pick: everything after the last dash
    parts = ticker.split('-')
    if len(parts) >= 3:
        pick_raw = parts[-1]
        # For spreads: ORL6 → ORL +6
        spread_match = re.match(r'([A-Z]{2,4})(\d+)$', pick_raw)
        if spread_match and result["sport"] in ("NBASPREAD", "NHLSPREAD", "NFLSPREAD"):
            result["pick"] = f"{spread_match.group(1)} +{spread_match.group(2)}"
        # For totals: just a number → Over N
        elif pick_raw.isdigit() and "TOTAL" in result["sport"]:
            result["pick"] = f"Over {pick_raw}"
        # For player props: ORLJTATUM0-25 format
        elif "PTS" in result["sport"] or "REB" in result["sport"] or "AST" in result["sport"] or "3PT" in result["sport"]:
            # Try to extract player name from the pick
            player_match = re.match(r'([A-Z]{2,4})([A-Z][A-Za-z]+\d*)-?(\d+)?', pick_raw)
            if player_match:
                result["pick"] = pick_raw
            else:
                result["pick"] = pick_raw
        else:
            result["pick"] = pick_raw

    # Better matchup parsing for game winners
    # KXNBAGAME-26APR12ORLBOS-ORL → teams are in the middle segment
    mid_match = re.search(r'\d{2}[A-Z]{3}\d{2}(\d{4})?([A-Z]{3,8})-', ticker)
    if mid_match:
        teams_str = mid_match.group(2)
        # Split 6-letter combo into two 3-letter teams
        if len(teams_str) == 6:
            result["matchup"] = f"{teams_str[:3]} vs {teams_str[3:]}"
        elif len(teams_str) == 8:
            result["matchup"] = f"{teams_str[:4]} vs {teams_str[4:]}"
        elif len(teams_str) == 7:
            # Try 3+4 or 4+3
            result["matchup"] = f"{teams_str[:3]} vs {teams_str[3:]}"

    return result


def _compute_fill_cash_change(fill: dict) -> float:
    """Compute the exact cash change from a fill (buy or sell)."""
    action = fill.get("action", "")
    side = fill.get("side", "")
    count = float(fill.get("count_fp", 0))
    yes_p = float(fill.get("yes_price_dollars", 0))
    no_p = float(fill.get("no_price_dollars", 0))
    fee = float(fill.get("fee_cost", 0))

    if action == "buy":
        price = no_p if side == "no" else yes_p
        return -(price * count + fee)
    elif action == "sell":
        if side == "yes":
            return no_p * count - fee  # Sell YES closing NO = get NO value
        else:
            return yes_p * count - fee  # Sell NO closing YES = get YES value
    return 0.0


def _build_balance_timeline(fills: list, settlements: list, current_balance: float) -> list[dict]:
    """
    Build a timeline of balance snapshots by backtracking from current balance.
    Returns list of {ts, balance, cash_change, type, ticker} sorted chronologically.
    """
    events = []

    for f in fills:
        cash = _compute_fill_cash_change(f)
        events.append({
            "ts": f.get("created_time", ""),
            "cash": cash,
            "type": "fill",
            "ticker": f.get("ticker", ""),
            "desc": f"{f.get('action', '')} {f.get('side', '')} {float(f.get('count_fp', 0)):.0f}x",
        })

    for s in settlements:
        revenue_cents = s.get("revenue", 0)
        if revenue_cents > 0:
            events.append({
                "ts": s.get("settled_time", s.get("created_time", "")),
                "cash": revenue_cents / 100.0,
                "type": "settlement",
                "ticker": s.get("ticker", ""),
                "desc": f"settle {s.get('market_result', '')}",
            })

    events.sort(key=lambda e: e["ts"])

    # Backtrack from current balance to find initial
    balance = current_balance
    for e in reversed(events):
        balance -= e["cash"]

    # Replay forward, recording balance after each event
    timeline = []
    for e in events:
        balance += e["cash"]
        timeline.append({
            "ts": e["ts"],
            "balance": round(balance, 2),
            "cash_change": round(e["cash"], 2),
            "type": e["type"],
            "ticker": e["ticker"],
            "desc": e["desc"],
        })

    return timeline


def _filter_agent_fills(fills: list[dict]) -> list[dict]:
    """Filter out fills from before the agent started (personal trades)."""
    return [f for f in fills if f.get("created_time", "") >= AGENT_START_DATE]


def _filter_agent_settlements(settlements: list[dict]) -> list[dict]:
    """Filter out settlements from before the agent started."""
    return [s for s in settlements
            if s.get("settled_time", s.get("created_time", "")) >= AGENT_START_DATE]


def _detect_sport(ticker: str) -> str:
    """Match ticker to sport using longest-prefix match."""
    for prefix, sport in sorted(SPORT_FROM_PREFIX.items(), key=lambda x: -len(x[0])):
        if ticker.startswith(prefix):
            return sport
    return "OTHER"


def _parse_ts(ts_str: str) -> datetime:
    """Parse an ISO timestamp from Kalshi into a datetime."""
    if not ts_str:
        return datetime.min.replace(tzinfo=timezone.utc)
    # Handle both Z and +00:00 suffixes
    ts_str = ts_str.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(ts_str)
    except Exception:
        return datetime.min.replace(tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@app.route("/")
def index():
    """Serve the dashboard HTML."""
    return render_template("index.html")


@app.route("/api/summary")
def api_summary():
    """Account summary: cash, portfolio value, NAV, drawdown, mode."""
    try:
        cash = client.get_balance()
    except Exception:
        cash = 0.0

    try:
        positions = client.get_positions()
    except Exception:
        positions = []

    # Estimate portfolio value from positions
    portfolio_value = cash
    for pos in positions:
        exposure = float(pos.get("market_exposure_dollars", 0))
        portfolio_value += exposure

    # Read NAV from risk_state.json if it exists
    nav = 1.0
    hwm_nav = 1.0
    risk_path = PROJECT_ROOT / "data" / "risk_state.json"
    if risk_path.is_file():
        try:
            with open(risk_path) as f:
                risk_data = json.load(f)
            nav = risk_data.get("nav", 1.0)
            hwm_nav = risk_data.get("hwm_nav", nav)
        except Exception:
            pass

    drawdown_pct = round((1.0 - nav / hwm_nav) * 100, 2) if hwm_nav > 0 else 0.0

    # Save portfolio snapshot for equity curve (append once per hour max)
    try:
        snap_path = PROJECT_ROOT / "data" / "portfolio_snapshots.jsonl"
        now = datetime.now(timezone.utc)
        should_save = True
        if snap_path.is_file():
            # Check last line timestamp
            with open(snap_path, "rb") as f:
                f.seek(0, 2)
                fsize = f.tell()
                if fsize > 2:
                    f.seek(max(0, fsize - 200))
                    last_line = f.readlines()[-1].decode()
                    last_snap = json.loads(last_line)
                    last_time = _parse_ts(last_snap.get("time", ""))
                    if (now - last_time).total_seconds() < 3600:
                        should_save = False
        if should_save:
            snap = {"time": now.isoformat(), "portfolio": round(portfolio_value, 2), "cash": round(cash, 2)}
            with open(snap_path, "a") as f:
                f.write(json.dumps(snap) + "\n")
    except Exception:
        pass

    # True P&L = current portfolio - total deposits
    net_pnl = round(portfolio_value - TOTAL_DEPOSITS, 2)
    net_pnl_pct = round(net_pnl / TOTAL_DEPOSITS * 100, 1) if TOTAL_DEPOSITS > 0 else 0.0

    return jsonify(
        {
            "cash": round(cash, 2),
            "portfolio_value": round(portfolio_value, 2),
            "total_deposits": TOTAL_DEPOSITS,
            "net_pnl": net_pnl,
            "net_pnl_pct": net_pnl_pct,
            "open_positions": len(positions),
            "nav": round(nav, 4),
            "hwm_nav": round(hwm_nav, 4),
            "drawdown_pct": drawdown_pct,
            "mode": TRADING_MODE,
        }
    )


@app.route("/api/positions")
def api_positions():
    """Open positions with ticker, side, qty, exposure, entry price."""
    try:
        positions = client.get_positions()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    result = []
    for pos in positions:
        ticker = pos.get("ticker", "")
        # v2 API: position_fp (string), market_exposure_dollars (string)
        position_qty = float(pos.get("position_fp", pos.get("position", 0)))
        side = "YES" if position_qty > 0 else "NO"
        qty = abs(position_qty)
        exposure = float(pos.get("market_exposure_dollars", 0))
        total_traded = float(pos.get("total_traded_dollars", 0))
        entry_price = round(total_traded / qty, 4) if qty > 0 else 0.0

        result.append(
            {
                "ticker": ticker,
                "sport": _detect_sport(ticker),
                "side": side,
                "qty": qty,
                "exposure": round(exposure, 2),
                "entry_price": entry_price,
            }
        )

    return jsonify(result)


@app.route("/api/fills")
def api_fills():
    """
    Trade history with per-ticker P&L.

    Groups fills by ticker, computes buy cost / sell revenue,
    checks settlements, and returns net P&L per trade.
    """
    days = int(request.args.get("days", 7))
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    try:
        fills = _filter_agent_fills(client.get_fills(paginate_all=True))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    try:
        settlements = _filter_agent_settlements(client.get_settlements(paginate_all=True))
    except Exception:
        settlements = []

    # Build settlement lookup: ticker -> full settlement dict
    settlement_map: dict[str, dict] = {}
    for s in settlements:
        sticker = s.get("ticker", "")
        if sticker:
            settlement_map[sticker] = s

    # Also build P&L from settlements directly (for trades where fills were paginated away)
    # This catches positions where we have no fills but DO have settlement data
    all_tickers_with_data: set[str] = set()

    # Group fills by ticker
    grouped: dict[str, list[dict]] = defaultdict(list)
    for fill in fills:
        created = fill.get("created_time", "")
        fill_dt = _parse_ts(created)
        if fill_dt < cutoff:
            continue
        ticker = fill.get("ticker", "")
        grouped[ticker].append(fill)
        all_tickers_with_data.add(ticker)

    # Add settlement-only tickers (positions where fills were paginated away)
    for sticker, settle_data in settlement_map.items():
        settle_time = settle_data.get("settled_time", settle_data.get("created_time", ""))
        if _parse_ts(settle_time) >= cutoff and sticker not in all_tickers_with_data:
            grouped[sticker] = []  # No fills, but has settlement

    result = []
    for ticker, ticker_fills in grouped.items():
        buys = []
        sells = []

        # Parse fills into buy/sell lists
        for fill in ticker_fills:
            action, side, count, yes_price, no_price, fee = _parse_fill(fill)
            created = fill.get("created_time", "")
            fill_time = _parse_ts(created).strftime("%H:%M:%S") if created else ""

            if action == "buy":
                cost = (no_price if side == "no" else yes_price) * count + fee
                buys.append({"count": count, "no_price": round(no_price, 4),
                             "yes_price": round(yes_price, 4), "cost": round(cost, 2), "time": fill_time})
            elif action == "sell":
                # Sell YES (closing NO) = receive NO value, Sell NO (closing YES) = receive YES value
                rev = (no_price if side == "yes" else yes_price) * count - fee
                sells.append({"count": count, "yes_price": round(yes_price, 4),
                              "no_price": round(no_price, 4), "revenue": round(rev, 2), "time": fill_time})

        # Compute P&L using settlement-aware helper
        settle = settlement_map.get(ticker)
        total_cost, total_revenue, buy_count = _compute_ticker_pnl(ticker_fills, settle)

        net_pnl = round(total_revenue - total_cost, 2)

        # Determine status
        settle_result = ""
        if settle:
            settle_result = settle.get("market_result", settle.get("result", ""))
        if settle_result:
            status = "WIN" if net_pnl > 0 else "LOSS"
        elif not ticker_fills:
            status = "SETTLED"
        else:
            status = "OPEN" if net_pnl == 0 and not settle_result else ("WIN" if net_pnl > 0 else ("LOSS" if net_pnl < 0 else "FLAT"))

        # Determine earliest fill time for sorting
        all_times = [_parse_ts(f.get("created_time", "")) for f in ticker_fills]
        earliest = min(all_times) if all_times else datetime.min.replace(tzinfo=timezone.utc)

        parsed = _parse_ticker(ticker)
        result.append(
            {
                "ticker": ticker,
                "sport": parsed["sport"],
                "date": parsed["date"],
                "matchup": parsed["matchup"],
                "pick": parsed["pick"],
                "buys": buys,
                "sells": sells,
                "total_cost": round(total_cost, 2),
                "total_revenue": round(total_revenue, 2),
                "net_pnl": net_pnl,
                "status": status,
                "settlement": settle_result,
                "_sort_time": earliest.isoformat(),
            }
        )

    # Sort by time descending (most recent first)
    result.sort(key=lambda x: x["_sort_time"], reverse=True)
    # Remove internal sort key
    for r in result:
        r.pop("_sort_time", None)

    return jsonify(result)


@app.route("/api/settlements")
def api_settlements():
    """Recent settlement results."""
    days = int(request.args.get("days", 7))
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    try:
        settlements = client.get_settlements(paginate_all=True)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    result = []
    for s in settlements:
        settled_time = s.get("settled_time", s.get("created_time", ""))
        if _parse_ts(settled_time) < cutoff:
            continue
        ticker = s.get("ticker", "")
        result.append(
            {
                "ticker": ticker,
                "sport": _detect_sport(ticker),
                "result": s.get("market_result", s.get("result", "")),
                "revenue": float(s.get("revenue_dollars", s.get("revenue", 0))),
                "settled_time": settled_time,
            }
        )

    return jsonify(result)


@app.route("/api/daily-pnl")
def api_daily_pnl():
    """Accurate daily balance via backtracking from current known balance."""
    try:
        fills = client.get_fills(paginate_all=True)
        settlements = client.get_settlements(paginate_all=True)
        current_balance = client.get_balance()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    timeline = _build_balance_timeline(fills, settlements, current_balance)

    # Aggregate to daily (end of day balance)
    daily: dict[str, dict] = {}
    daily_trades: dict[str, int] = defaultdict(int)
    for t in timeline:
        date = t["ts"][:10]
        daily[date] = t["balance"]
        daily_trades[date] += 1

    result_list = []
    prev_balance = TOTAL_DEPOSITS
    for d in sorted(daily.keys()):
        if d < AGENT_START_DATE:
            prev_balance = daily[d]
            continue
        bal = round(daily[d], 2)
        day_pnl = round(bal - prev_balance, 2)
        result_list.append({
            "date": d,
            "pnl": day_pnl,
            "cumulative": round(bal - TOTAL_DEPOSITS, 2),
            "portfolio_value": bal,
            "trades": daily_trades.get(d, 0),
        })
        prev_balance = bal

    return jsonify(result_list)


@app.route("/api/trade-timeline")
def api_trade_timeline():
    """
    Full trade-level timeline with balance after each event.
    Shows every fill and settlement with the exact cash change and running balance.
    """
    try:
        fills = client.get_fills(paginate_all=True)
        settlements = client.get_settlements(paginate_all=True)
        current_balance = client.get_balance()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    timeline = _build_balance_timeline(fills, settlements, current_balance)

    # Filter to agent era and add sport
    result = []
    for t in timeline:
        if t["ts"][:10] < AGENT_START_DATE:
            continue
        parsed = _parse_ticker(t["ticker"])
        result.append({
            "time": t["ts"][:19],
            "ticker": t["ticker"],
            "sport": parsed["sport"],
            "date": parsed["date"],
            "matchup": parsed["matchup"],
            "pick": parsed["pick"],
            "type": t["type"],
            "desc": t["desc"],
            "cash_change": t["cash_change"],
            "balance": t["balance"],
        })

    # Most recent first
    result.reverse()
    return jsonify(result)


@app.route("/api/open-positions")
def api_open_positions():
    """
    Currently held positions with entry cost, current value, and unrealized P&L.
    """
    try:
        positions = client.get_positions()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    result = []
    for pos in positions:
        position_qty = float(pos.get("position_fp", 0))
        if position_qty == 0:
            continue

        ticker = pos.get("ticker", "")
        side = "NO" if position_qty < 0 else "YES"
        qty = abs(position_qty)
        exposure = float(pos.get("market_exposure_dollars", 0))
        total_traded = float(pos.get("total_traded_dollars", 0))
        fees = float(pos.get("fees_paid_dollars", 0))
        entry_cost = total_traded + fees

        # Unrealized P&L = current value - entry cost
        # For NO positions: if we sold now at current no_price, we'd get exposure
        unrealized = exposure - entry_cost if entry_cost > 0 else 0

        parsed = _parse_ticker(ticker)
        result.append({
            "ticker": ticker,
            "sport": parsed["sport"],
            "date": parsed["date"],
            "matchup": parsed["matchup"],
            "pick": parsed["pick"],
            "side": side,
            "qty": qty,
            "entry_cost": round(entry_cost, 2),
            "current_value": round(exposure, 2),
            "unrealized_pnl": round(unrealized, 2),
            "fees": round(fees, 2),
        })

    return jsonify(result)


@app.route("/api/sport-breakdown")
def api_sport_breakdown():
    """Per-sport aggregate stats."""
    try:
        fills = _filter_agent_fills(client.get_fills(paginate_all=True))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    try:
        settlements = _filter_agent_settlements(client.get_settlements(paginate_all=True))
    except Exception:
        settlements = []

    settlement_map: dict[str, dict] = {}
    for s in settlements:
        sticker = s.get("ticker", "")
        if sticker:
            settlement_map[sticker] = s

    # Group fills by ticker
    ticker_fills: dict[str, list[dict]] = defaultdict(list)
    for fill in fills:
        ticker_fills[fill.get("ticker", "")].append(fill)

    # Add settlement-only tickers
    for sticker in settlement_map:
        if sticker not in ticker_fills:
            ticker_fills[sticker] = []

    # Compute per-ticker P&L, then aggregate by sport
    sport_stats: dict[str, dict] = defaultdict(
        lambda: {"trades": 0, "wins": 0, "losses": 0, "pnl": 0.0, "total_cost": 0.0}
    )

    for ticker, tfills in ticker_fills.items():
        sport = _detect_sport(ticker)
        total_cost, total_revenue, buy_count = _compute_ticker_pnl(tfills, settlement_map.get(ticker))
        net_pnl = total_revenue - total_cost
        sport_stats[sport]["trades"] += 1
        sport_stats[sport]["pnl"] += net_pnl
        sport_stats[sport]["total_cost"] += total_cost
        if net_pnl > 0:
            sport_stats[sport]["wins"] += 1
        elif net_pnl < 0:
            sport_stats[sport]["losses"] += 1

    result = []
    for sport, stats in sorted(sport_stats.items(), key=lambda x: -x[1]["pnl"]):
        roi = (
            round(stats["pnl"] / stats["total_cost"] * 100, 1)
            if stats["total_cost"] > 0
            else 0.0
        )
        result.append(
            {
                "sport": sport,
                "trades": stats["trades"],
                "wins": stats["wins"],
                "losses": stats["losses"],
                "pnl": round(stats["pnl"], 2),
                "roi": roi,
            }
        )

    return jsonify(result)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5050, debug=True)
