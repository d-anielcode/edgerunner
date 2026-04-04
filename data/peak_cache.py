"""
Persistent peak price cache for EdgeRunner.

Saves peak prices to a local JSON file so they survive agent restarts.
Without this, restarting the agent resets all trailing stop high-water marks,
potentially causing the agent to hold losing positions that should have been sold.
"""

import json
from decimal import Decimal
from pathlib import Path

from rich.console import Console

console = Console()

PEAK_CACHE_FILE = Path("data/peak_prices.json")


def load_peak_prices() -> dict[str, Decimal]:
    """Load peak prices from disk. Returns empty dict if file doesn't exist."""
    if not PEAK_CACHE_FILE.exists():
        return {}
    try:
        with open(PEAK_CACHE_FILE, "r") as f:
            data = json.load(f)
        return {k: Decimal(str(v)) for k, v in data.items()}
    except Exception as e:
        console.print(f"[yellow]Peak cache load error: {e}[/yellow]")
        return {}


def save_peak_prices(peaks: dict[str, Decimal]) -> None:
    """Save peak prices to disk."""
    try:
        PEAK_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        data = {k: str(v) for k, v in peaks.items()}
        with open(PEAK_CACHE_FILE, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        console.print(f"[yellow]Peak cache save error: {e}[/yellow]")
