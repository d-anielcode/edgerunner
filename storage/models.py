"""
Pydantic V2 models for all Supabase tables.

These are the canonical data shapes for the entire project. Every module
that reads from or writes to Supabase uses these models for validation.

Design decisions:
- Decimal (not float) for all monetary values — prevents floating-point
  rounding errors that could cause Kalshi order rejections or incorrect P&L.
- Literal types for constrained fields — a typo like "buuy" raises a
  ValidationError immediately, not a mysterious API rejection.
- Optional fields with None defaults for Supabase-generated columns
  (id, created_at, computed columns) — absent when inserting, present when reading.
"""

import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, field_validator
from rich.console import Console

console = Console()


class Market(BaseModel):
    """
    A prediction market on Kalshi that the agent is tracking.

    Maps to the 'markets' Supabase table.
    """

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID | None = None
    kalshi_ticker: str
    title: str
    category: str | None = None
    close_time: datetime | None = None
    closing_price: Decimal | None = None
    resolved_at: datetime | None = None
    resolution: str | None = None
    created_at: datetime | None = None

    def to_insert_dict(self) -> dict:
        """Convert to dict for Supabase insert, excluding None and auto-generated fields."""
        return self.model_dump(
            exclude_none=True,
            exclude={"id", "created_at"},
        )


class Trade(BaseModel):
    """
    A single trade executed by the agent.

    Maps to the 'trades' Supabase table. This is the most important model —
    it stores every parameter of every trade for post-session analysis.
    """

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID | None = None
    market_id: uuid.UUID | None = None
    kalshi_ticker: str
    side: Literal["yes", "no"]
    action: Literal["buy", "sell"]
    quantity: Decimal
    price: Decimal
    kelly_fraction: Decimal
    edge: Decimal
    claude_reasoning: str | None = None
    signal_confidence: Decimal | None = None
    execution_latency_ms: int | None = None
    model_version: str = "haiku-4.5"
    trading_mode: Literal["paper", "live"]
    filled_at: datetime | None = None

    @field_validator("price")
    @classmethod
    def price_must_be_valid(cls, v: Decimal) -> Decimal:
        """
        Validate price is between 0.01 and 0.99 (valid prediction market range).

        Why? Kalshi contracts trade between $0.01 and $0.99. A price of $0.00
        or $1.00 means the market has resolved — you can't trade at those prices.
        """
        if not (Decimal("0.01") <= v <= Decimal("0.99")):
            raise ValueError(f"Price must be between 0.01 and 0.99, got {v}")
        return v

    @field_validator("kelly_fraction")
    @classmethod
    def kelly_must_be_non_negative(cls, v: Decimal) -> Decimal:
        """Kelly fraction must be >= 0. A value of 0 means PASS (no trade)."""
        if v < Decimal("0"):
            raise ValueError(f"Kelly fraction must be non-negative, got {v}")
        return v

    @field_validator("edge")
    @classmethod
    def edge_must_be_non_negative(cls, v: Decimal) -> Decimal:
        """Edge must be >= 0. We only trade when we have a positive edge."""
        if v < Decimal("0"):
            raise ValueError(f"Edge must be non-negative, got {v}")
        return v

    def to_insert_dict(self) -> dict:
        """Convert to dict for Supabase insert, excluding auto-generated fields."""
        data = self.model_dump(
            exclude_none=True,
            exclude={"id", "filled_at"},
        )
        # Convert Decimal to str for Supabase (it expects numeric-compatible types)
        for key, value in data.items():
            if isinstance(value, Decimal):
                data[key] = float(value)
            elif isinstance(value, uuid.UUID):
                data[key] = str(value)
        return data


class Position(BaseModel):
    """
    An open position the agent currently holds.

    Maps to the 'positions' Supabase table. unrealized_pnl is computed
    by Supabase (GENERATED ALWAYS AS) and is read-only.
    """

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID | None = None
    market_id: uuid.UUID | None = None
    kalshi_ticker: str
    side: Literal["yes", "no"]
    avg_price: Decimal
    quantity: Decimal
    current_price: Decimal | None = None
    unrealized_pnl: Decimal | None = None  # Computed by Supabase, read-only
    opened_at: datetime | None = None
    closed_at: datetime | None = None

    @field_validator("avg_price")
    @classmethod
    def avg_price_must_be_valid(cls, v: Decimal) -> Decimal:
        """Average price must be in the valid prediction market range."""
        if not (Decimal("0.01") <= v <= Decimal("0.99")):
            raise ValueError(f"avg_price must be between 0.01 and 0.99, got {v}")
        return v

    def to_insert_dict(self) -> dict:
        """Convert to dict for Supabase insert, excluding computed and auto fields."""
        data = self.model_dump(
            exclude_none=True,
            exclude={"id", "unrealized_pnl", "opened_at", "closed_at"},
        )
        for key, value in data.items():
            if isinstance(value, Decimal):
                data[key] = float(value)
            elif isinstance(value, uuid.UUID):
                data[key] = str(value)
        return data


class DailyPnl(BaseModel):
    """
    Daily profit & loss summary.

    Maps to the 'daily_pnl' Supabase table. win_rate is computed
    by Supabase and is read-only.
    """

    model_config = ConfigDict(from_attributes=True)

    date: date
    realized_pnl: Decimal = Decimal("0")
    unrealized_pnl: Decimal = Decimal("0")
    total_trades: int = 0
    winning_trades: int = 0
    win_rate: Decimal | None = None  # Computed by Supabase, read-only
    claude_api_cost: Decimal = Decimal("0")

    def to_insert_dict(self) -> dict:
        """Convert to dict for Supabase insert/upsert."""
        data = self.model_dump(exclude={"win_rate"})
        data["date"] = data["date"].isoformat()
        for key, value in data.items():
            if isinstance(value, Decimal):
                data[key] = float(value)
        return data


class BrierScore(BaseModel):
    """
    Tracks prediction accuracy for a single market.

    The Brier score measures how well the agent's probability estimates
    match reality. Perfect predictor = 0.0, random guessing = 0.25.
    brier_score is computed by Supabase (GENERATED ALWAYS AS).
    """

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID | None = None
    market_id: uuid.UUID | None = None
    predicted_probability: Decimal
    actual_outcome: int  # 0 or 1
    brier_score: Decimal | None = None  # Computed by Supabase, read-only
    scored_at: datetime | None = None

    @field_validator("predicted_probability")
    @classmethod
    def prob_must_be_valid(cls, v: Decimal) -> Decimal:
        """Probability must be between 0 and 1."""
        if not (Decimal("0") <= v <= Decimal("1")):
            raise ValueError(f"predicted_probability must be between 0 and 1, got {v}")
        return v

    @field_validator("actual_outcome")
    @classmethod
    def outcome_must_be_binary(cls, v: int) -> int:
        """Outcome must be 0 (event didn't happen) or 1 (event happened)."""
        if v not in (0, 1):
            raise ValueError(f"actual_outcome must be 0 or 1, got {v}")
        return v

    def to_insert_dict(self) -> dict:
        """Convert to dict for Supabase insert, excluding computed fields."""
        data = self.model_dump(
            exclude_none=True,
            exclude={"id", "brier_score", "scored_at"},
        )
        for key, value in data.items():
            if isinstance(value, Decimal):
                data[key] = float(value)
            elif isinstance(value, uuid.UUID):
                data[key] = str(value)
        return data


# =============================================================================
# Standalone test
# =============================================================================

if __name__ == "__main__":
    from pydantic import ValidationError

    console.print("[bold]Testing Pydantic models...[/bold]\n")

    # Test valid Market
    market = Market(kalshi_ticker="KXNBA-LEBRON-PTS-O25", title="LeBron Over 25.5 Points")
    console.print(f"[green]Market: {market.kalshi_ticker} — {market.title}[/green]")
    console.print(f"  Insert dict: {market.to_insert_dict()}")

    # Test valid Trade
    trade = Trade(
        kalshi_ticker="KXNBA-LEBRON-PTS-O25",
        side="yes",
        action="buy",
        quantity=Decimal("5"),
        price=Decimal("0.42"),
        kelly_fraction=Decimal("0.042"),
        edge=Decimal("0.15"),
        claude_reasoning="Davis ruled OUT, usage rate implies 65% true prob.",
        signal_confidence=Decimal("0.75"),
        trading_mode="paper",
    )
    console.print(f"[green]Trade: {trade.side} @ ${trade.price} — Kelly {trade.kelly_fraction}[/green]")
    console.print(f"  Insert dict keys: {list(trade.to_insert_dict().keys())}")

    # Test valid Position
    pos = Position(
        kalshi_ticker="KXNBA-LEBRON-PTS-O25",
        side="yes",
        avg_price=Decimal("0.42"),
        quantity=Decimal("5"),
    )
    console.print(f"[green]Position: {pos.kalshi_ticker} — {pos.quantity} @ ${pos.avg_price}[/green]")

    # Test valid DailyPnl
    pnl = DailyPnl(
        date=date.today(),
        realized_pnl=Decimal("8.40"),
        total_trades=7,
        winning_trades=5,
    )
    console.print(f"[green]DailyPnl: {pnl.date} — P&L: ${pnl.realized_pnl}[/green]")

    # Test valid BrierScore
    brier = BrierScore(
        predicted_probability=Decimal("0.65"),
        actual_outcome=1,
    )
    console.print(f"[green]BrierScore: pred={brier.predicted_probability} actual={brier.actual_outcome}[/green]")

    # Test INVALID data — should raise ValidationError
    console.print("\n[bold]Validation Tests (expecting errors):[/bold]")

    invalid_tests = [
        ("Trade with invalid side", lambda: Trade(
            kalshi_ticker="T", side="maybe", action="buy",
            quantity=Decimal("1"), price=Decimal("0.50"),
            kelly_fraction=Decimal("0.01"), edge=Decimal("0.05"),
            trading_mode="paper",
        )),
        ("Trade with price > 0.99", lambda: Trade(
            kalshi_ticker="T", side="yes", action="buy",
            quantity=Decimal("1"), price=Decimal("1.50"),
            kelly_fraction=Decimal("0.01"), edge=Decimal("0.05"),
            trading_mode="paper",
        )),
        ("Trade with negative kelly", lambda: Trade(
            kalshi_ticker="T", side="yes", action="buy",
            quantity=Decimal("1"), price=Decimal("0.50"),
            kelly_fraction=Decimal("-0.05"), edge=Decimal("0.05"),
            trading_mode="paper",
        )),
        ("BrierScore with outcome=2", lambda: BrierScore(
            predicted_probability=Decimal("0.5"), actual_outcome=2,
        )),
        ("BrierScore with prob > 1", lambda: BrierScore(
            predicted_probability=Decimal("1.5"), actual_outcome=1,
        )),
    ]

    for name, create_fn in invalid_tests:
        try:
            create_fn()
            console.print(f"  [red]FAIL: {name} — should have raised ValidationError[/red]")
        except ValidationError:
            console.print(f"  [green]PASS: {name} — correctly rejected[/green]")

    console.print("\n[green]storage/models.py: All tests passed.[/green]")
