"""
Decision logger for EdgeRunner.

Logs EVERY trade decision (accepted AND rejected) to Supabase
for post-session analysis. This is the foundation for understanding
what the agent does right and wrong.

Inspired by Lumibot's decision caching — by logging every decision
with full context, we can replay and analyze without re-calling Claude.
"""

import json
from datetime import datetime, timezone
from decimal import Decimal

from rich.console import Console

from storage.supabase_client import insert_row

console = Console()
UTC = timezone.utc

TABLE_DECISIONS = "decisions"


async def log_decision(
    ticker: str,
    title: str,
    action: str,
    edge: float,
    kelly_fraction: float,
    confidence: float,
    rationale: str,
    market_prob: float,
    agent_prob: float,
    gate_results: str,
    accepted: bool,
    rejection_reason: str = "",
    bet_amount: float = 0.0,
    market_type: str = "",
) -> None:
    """
    Log a trade decision to Supabase.

    Logs both accepted and rejected decisions so we can analyze:
    - What trades are we taking vs skipping?
    - Which gates block the most trades?
    - Is the agent's edge real? (compare predictions to outcomes)
    - What's our Brier score by market type?
    """
    try:
        data = {
            "ticker": ticker,
            "title": title[:100],
            "action": action,
            "edge": round(edge, 4),
            "kelly_fraction": round(kelly_fraction, 4),
            "confidence": round(confidence, 4),
            "rationale": rationale[:500],
            "market_prob": round(market_prob, 4),
            "agent_prob": round(agent_prob, 4),
            "gate_results": gate_results[:500],
            "accepted": accepted,
            "rejection_reason": rejection_reason[:200],
            "bet_amount": round(bet_amount, 4),
            "market_type": market_type,
            "created_at": datetime.now(UTC).isoformat(),
        }
        await insert_row(TABLE_DECISIONS, data)
    except Exception as e:
        # Never crash the trading loop for logging
        console.print(f"[yellow]Decision log failed: {e}[/yellow]")


def classify_market_type(ticker: str) -> str:
    """Classify a ticker into market type for analysis."""
    ticker_upper = ticker.upper()
    if "KXNBAGAME" in ticker_upper:
        return "GAME_WINNER"
    elif "KXNBASPREAD" in ticker_upper:
        return "SPREAD"
    elif "KXNBAPTS" in ticker_upper:
        return "PLAYER_PTS"
    elif "KXNBAREB" in ticker_upper:
        return "PLAYER_REB"
    elif "KXNBAAST" in ticker_upper:
        return "PLAYER_AST"
    elif "KXNBA3PT" in ticker_upper:
        return "PLAYER_3PT"
    elif "KXNBA" in ticker_upper:
        return "NBA_OTHER"
    return "OTHER"
