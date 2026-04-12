"""
Persist high-water mark (HWM) to disk so it survives agent restarts.
Follows the same pattern as discovery_cache.py and peak_cache.py.
"""
import json
from decimal import Decimal
from pathlib import Path

CACHE_FILE = Path(__file__).parent / "hwm_state.json"


def load_hwm() -> Decimal | None:
    """Load persistent high-water mark from disk. Returns None if no file."""
    if not CACHE_FILE.exists():
        return None
    try:
        with open(CACHE_FILE) as f:
            data = json.load(f)
        return Decimal(str(data["all_time_high"]))
    except (json.JSONDecodeError, ValueError, KeyError):
        return None


def save_hwm(value: Decimal) -> None:
    """Save high-water mark to disk."""
    try:
        from datetime import datetime, timezone
        data = {
            "all_time_high": str(value),
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }
        with open(CACHE_FILE, "w") as f:
            json.dump(data, f)
    except Exception:
        pass
