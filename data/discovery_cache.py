"""
Persist discovery prices to disk so they survive agent restarts.
If a price was first seen at 41c, we know 84c is mid-game even after restart.
"""
import json
from decimal import Decimal
from pathlib import Path

CACHE_FILE = Path(__file__).parent / "discovery_prices.json"


def load_discovery_prices() -> dict[str, Decimal]:
    """Load discovery prices from disk."""
    if not CACHE_FILE.exists():
        return {}
    try:
        with open(CACHE_FILE) as f:
            data = json.load(f)
        return {k: Decimal(str(v)) for k, v in data.items()}
    except (json.JSONDecodeError, ValueError):
        return {}


def save_discovery_prices(prices: dict[str, Decimal]) -> None:
    """Save discovery prices to disk."""
    try:
        data = {k: str(v) for k, v in prices.items()}
        with open(CACHE_FILE, "w") as f:
            json.dump(data, f)
    except Exception:
        pass


def clear_old_prices(prices: dict[str, Decimal], current_tickers: list[str]) -> dict[str, Decimal]:
    """Remove discovery prices for tickers no longer being tracked (old games)."""
    current_set = set(current_tickers)
    return {k: v for k, v in prices.items() if k in current_set}
