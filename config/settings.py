"""
Configuration loader for EdgeRunner.

This is the SINGLE SOURCE OF TRUTH for all configuration.
Every module imports settings from here — no module reads .env directly.

How it works:
- Loads .env file via python-dotenv at import time
- Validates all required env vars exist immediately (fail fast)
- Switches Kalshi URLs based on TRADING_MODE (paper vs live)
- Prints a startup banner so you always know which mode you're in
"""

import os
from pathlib import Path

from dotenv import load_dotenv
from rich.console import Console
from rich.panel import Panel

# Load .env file at import time.
# override=True ensures .env values take precedence over any existing
# system environment variables (e.g., empty vars set by shell profiles).
load_dotenv(override=True)

console = Console()


def _require_env(key: str) -> str:
    """
    Read a required environment variable or crash immediately.

    Why crash at startup? Because a missing API key discovered 20 minutes
    into a trading session (mid-trade) is far worse than failing on launch.
    """
    value = os.getenv(key)
    if not value or value.strip() == "":
        raise EnvironmentError(
            f"Required environment variable '{key}' is missing or empty. "
            f"Check your .env file. See .env.example for the template."
        )
    return value.strip()


def _mask_key(key: str, visible_chars: int = 8) -> str:
    """Show first N characters of a key, mask the rest."""
    if len(key) <= visible_chars:
        return "***"
    return key[:visible_chars] + "..." + "*" * 4


# =============================================================================
# TRADING MODE — The critical safety switch
# =============================================================================

TRADING_MODE: str = os.getenv("TRADING_MODE", "paper").lower()

if TRADING_MODE == "paper":
    KALSHI_BASE_URL: str = os.getenv(
        "KALSHI_BASE_URL", "https://demo-api.kalshi.co/trade-api/v2"
    )
    KALSHI_WS_URL: str = os.getenv(
        "KALSHI_WS_URL", "wss://demo-api.kalshi.co/trade-api/ws/v2"
    )
elif TRADING_MODE == "live":
    KALSHI_BASE_URL = os.getenv(
        "KALSHI_BASE_URL", "https://api.elections.kalshi.com/trade-api/v2"
    )
    KALSHI_WS_URL = os.getenv(
        "KALSHI_WS_URL", "wss://api.elections.kalshi.com/trade-api/ws/v2"
    )
else:
    raise ValueError(
        f"Invalid TRADING_MODE: '{TRADING_MODE}'. Must be 'paper' or 'live'. "
        f"Check your .env file."
    )

# =============================================================================
# API CREDENTIALS — All required, no defaults
# =============================================================================

ANTHROPIC_API_KEY: str = _require_env("ANTHROPIC_API_KEY")

KALSHI_API_KEY_ID: str = _require_env("KALSHI_API_KEY_ID")

_key_path_str: str = _require_env("KALSHI_PRIVATE_KEY_PATH")
KALSHI_PRIVATE_KEY_PATH: Path = Path(_key_path_str)
# Note: We validate the key file exists but NEVER read or log its contents here.
# The Kalshi client module handles reading the key when it needs to sign requests.
if not KALSHI_PRIVATE_KEY_PATH.is_file():
    console.print(
        f"[yellow]WARNING:[/yellow] Kalshi private key not found at "
        f"'{KALSHI_PRIVATE_KEY_PATH}'. RSA signing will fail. "
        f"Generate a key pair in your Kalshi dashboard.",
    )

SUPABASE_URL: str = _require_env("SUPABASE_URL")
SUPABASE_ANON_KEY: str = _require_env("SUPABASE_ANON_KEY")

DISCORD_WEBHOOK_URL: str = _require_env("DISCORD_WEBHOOK_URL")

# =============================================================================
# RISK PARAMETERS — Safe defaults, operator can override in .env
# =============================================================================

MAX_POSITION_PCT: float = float(os.getenv("MAX_POSITION_PCT", "0.05"))
"""Maximum fraction of bankroll per single trade (default: 5%)."""

FRACTIONAL_KELLY: float = float(os.getenv("FRACTIONAL_KELLY", "0.20"))
"""Multiply full Kelly recommendation by this (default: 0.20 = 20% Kelly)."""

MAX_CONCURRENT_POSITIONS: int = int(os.getenv("MAX_CONCURRENT_POSITIONS", "25"))
"""Maximum number of open positions at once (spread-thin strategy: many small bets)."""

MIN_EDGE_THRESHOLD: float = float(os.getenv("MIN_EDGE_THRESHOLD", "0.05"))
"""Minimum edge (|true_prob - market_prob|) required to trade (default: 5%)."""

MAX_SPREAD_CENTS: float = float(os.getenv("MAX_SPREAD_CENTS", "0.03"))
"""Maximum bid-ask spread in dollars to accept a trade (default: $0.03)."""

MAX_BET_DOLLARS: float = float(os.getenv("MAX_BET_DOLLARS", "200.0"))
"""Maximum dollar amount per trade (default: $200). Limits compounding risk."""

# =============================================================================
# DATA FEEDS
# =============================================================================

BALLDONTLIE_API_KEY: str | None = os.getenv("BALLDONTLIE_API_KEY")
"""Optional BallDontLie API key. Free tier may work without it for basic endpoints."""

ENABLE_NBA_POLLER: bool = os.getenv("ENABLE_NBA_POLLER", "false").lower() in ("true", "1", "yes")
"""Enable NBA data poller (BallDontLie + nba_api). Default: false."""

NBA_POLL_INTERVAL: float = float(os.getenv("NBA_POLL_INTERVAL", "45.0"))
"""Seconds between NBA data polling cycles (default: 45s)."""

ORDERBOOK_STALE_THRESHOLD: float = float(os.getenv("ORDERBOOK_STALE_THRESHOLD", "30.0"))
"""Seconds before orderbook data is considered stale (default: 30s)."""

# =============================================================================
# SMART MONEY (Polymarket leaderboard tracking)
# =============================================================================

SMART_MONEY_POLL_INTERVAL: float = float(os.getenv("SMART_MONEY_POLL_INTERVAL", "600.0"))
"""Seconds between Polymarket smart money polling cycles (default: 600s = 10 min)."""

SMART_MONEY_MIN_TRADERS: int = int(os.getenv("SMART_MONEY_MIN_TRADERS", "3"))
"""Minimum number of top traders on the same side to generate a smart money signal."""

POLYMARKET_DATA_API: str = "https://data-api.polymarket.com"
"""Polymarket Data API base URL. Public, no auth required, read-only from US."""

# =============================================================================
# BUDGET GUARD
# =============================================================================

CLAUDE_MONTHLY_BUDGET_LIMIT: float = float(
    os.getenv("CLAUDE_MONTHLY_BUDGET_LIMIT", "45.00")
)
"""Monthly Claude API spend limit in USD. Agent PASSes all trades if exceeded."""

# =============================================================================
# DEBUG
# =============================================================================

DEBUG_MODE: bool = os.getenv("DEBUG_MODE", "false").lower() == "true"

DRY_RUN: bool = os.getenv("DRY_RUN", "false").lower() == "true"
"""Dry-run mode: evaluate signals, log decisions, but skip placing orders on Kalshi."""

# =============================================================================
# STARTUP BANNER — So you always know which mode you're in
# =============================================================================

_mode_color = "red bold" if TRADING_MODE == "live" else "blue"
_mode_emoji = "LIVE" if TRADING_MODE == "live" else "PAPER"

console.print(
    Panel(
        f"[{_mode_color}]Mode: {_mode_emoji}[/{_mode_color}]\n"
        f"Kalshi: {KALSHI_BASE_URL}\n"
        f"Anthropic: {_mask_key(ANTHROPIC_API_KEY)}\n"
        f"Kalshi Key: {_mask_key(KALSHI_API_KEY_ID)}\n"
        f"Supabase: {SUPABASE_URL}\n"
        f"Kelly: {FRACTIONAL_KELLY}x | Max Position: {MAX_POSITION_PCT * 100}%\n"
        f"Dry Run: {DRY_RUN}\n"
        f"Debug: {DEBUG_MODE}",
        title="EdgeRunner",
        border_style=_mode_color,
    )
)

# =============================================================================
# EXPORTS
# =============================================================================

__all__ = [
    "TRADING_MODE",
    "KALSHI_BASE_URL",
    "KALSHI_WS_URL",
    "ANTHROPIC_API_KEY",
    "KALSHI_API_KEY_ID",
    "KALSHI_PRIVATE_KEY_PATH",
    "SUPABASE_URL",
    "SUPABASE_ANON_KEY",
    "DISCORD_WEBHOOK_URL",
    "MAX_POSITION_PCT",
    "FRACTIONAL_KELLY",
    "MAX_CONCURRENT_POSITIONS",
    "MIN_EDGE_THRESHOLD",
    "MAX_SPREAD_CENTS",
    "CLAUDE_MONTHLY_BUDGET_LIMIT",
    "BALLDONTLIE_API_KEY",
    "NBA_POLL_INTERVAL",
    "ORDERBOOK_STALE_THRESHOLD",
    "SMART_MONEY_POLL_INTERVAL",
    "SMART_MONEY_MIN_TRADERS",
    "POLYMARKET_DATA_API",
    "DEBUG_MODE",
    "DRY_RUN",
    "console",
]
