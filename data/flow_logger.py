"""
Order flow data logger — passive data collection for future analysis.

Records market state at evaluation time so we can later analyze:
- Does orderbook imbalance predict outcomes?
- Does volume level correlate with edge?
- Do specific price levels have different hit rates in live trading?

Data is appended to data/flow_log.jsonl (one JSON object per line).
This is READ-ONLY from the agent's perspective — no trading decisions use this data.
"""
import json
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

LOG_FILE = Path(__file__).parent / "flow_log.jsonl"


def log_market_flow(
    ticker: str,
    sport: str,
    yes_price: Decimal,
    bid_volume: Decimal,
    ask_volume: Decimal,
    ofi: float,
    volume: float,
    edge: float,
    kelly: float,
    action: str,
) -> None:
    """
    Log market state at evaluation time.

    Appends one line to flow_log.jsonl. Never blocks, never crashes.
    """
    try:
        entry = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "ticker": ticker,
            "sport": sport,
            "yes_price": float(yes_price),
            "bid_vol": float(bid_volume),
            "ask_vol": float(ask_volume),
            "ofi": round(ofi, 3),
            "volume": volume,
            "edge": round(edge, 4),
            "kelly": round(kelly, 4),
            "action": action,
        }
        with open(LOG_FILE, "a") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception:
        pass  # Never crash the agent for logging
