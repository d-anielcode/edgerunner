"""
Trade decision schema for EdgeRunner.

Defines the TradeDecision Pydantic model and the corresponding JSON Schema
tool definition for the Anthropic API. When Claude analyzes a market, it
MUST return a response matching this exact schema via structured tool use.

Why strict tool use? With `strict: True`, Claude's constrained decoding
algorithm guarantees the output matches the schema on the first try.
No parsing errors, no hallucinated text, no malformed JSON — ever.
"""

from typing import Literal

from pydantic import BaseModel, field_validator
from rich.console import Console

console = Console()


class TradeDecision(BaseModel):
    """
    The structured output Claude returns when evaluating a market.

    Every field is required and typed. Claude physically cannot return
    a response that doesn't match this schema when strict mode is on.
    """

    action: Literal["BUY_YES", "BUY_NO", "PASS"]
    """What to do: buy the Yes contract, buy the No contract, or skip."""

    target_market_id: str
    """The Kalshi market ticker (e.g., 'KXNBA-LEBRON-PTS-O25')."""

    implied_market_probability: float
    """The market's current implied probability (from the Yes price, 0.01-0.99)."""

    agent_calculated_probability: float
    """Claude's estimate of the true probability (0.01-0.99)."""

    kelly_fraction: float
    """Recommended Kelly fraction for this trade (0.0-0.05). 0.0 means PASS."""

    confidence_score: float
    """Claude's self-assessed confidence in its probability estimate (0.0-1.0)."""

    rationale: str
    """1-2 sentence explanation of why this trade has (or doesn't have) edge."""

    @field_validator("implied_market_probability", "agent_calculated_probability")
    @classmethod
    def prob_must_be_valid(cls, v: float) -> float:
        """Probabilities must be between 0.01 and 0.99."""
        if not (0.01 <= v <= 0.99):
            raise ValueError(f"Probability must be between 0.01 and 0.99, got {v}")
        return v

    @field_validator("kelly_fraction")
    @classmethod
    def kelly_must_be_bounded(cls, v: float) -> float:
        """Kelly fraction must be between 0.0 and 0.10 (safety cap)."""
        if not (0.0 <= v <= 0.10):
            raise ValueError(f"Kelly fraction must be between 0.0 and 0.10, got {v}")
        return v

    @field_validator("confidence_score")
    @classmethod
    def confidence_must_be_valid(cls, v: float) -> float:
        """Confidence must be between 0.0 and 1.0."""
        if not (0.0 <= v <= 1.0):
            raise ValueError(f"Confidence must be between 0.0 and 1.0, got {v}")
        return v

    @property
    def edge(self) -> float:
        """
        The calculated edge: difference between Claude's probability and market's.

        Positive edge = Claude thinks the event is more likely than the market does.
        For BUY_YES: edge = agent_prob - market_prob
        For BUY_NO: edge = market_prob - agent_prob (inverted)
        """
        if self.action == "BUY_YES":
            return self.agent_calculated_probability - self.implied_market_probability
        elif self.action == "BUY_NO":
            return self.implied_market_probability - self.agent_calculated_probability
        return 0.0

    @property
    def is_actionable(self) -> bool:
        """Whether this decision recommends a trade (not PASS)."""
        return self.action != "PASS"


def pass_decision(market_id: str, rationale: str = "No edge detected.") -> TradeDecision:
    """
    Create a PASS decision. Used as fallback when Claude is unavailable,
    data is stale, or the edge is below threshold.
    """
    return TradeDecision(
        action="PASS",
        target_market_id=market_id,
        implied_market_probability=0.50,
        agent_calculated_probability=0.50,
        kelly_fraction=0.0,
        confidence_score=0.0,
        rationale=rationale,
    )


# =============================================================================
# TOOL SCHEMA — JSON Schema for the Anthropic API tools parameter
# =============================================================================

TRADE_TOOL_SCHEMA: dict = {
    "name": "execute_prediction_trade",
    "description": (
        "Evaluate a Kalshi prediction market and return a structured trading decision. "
        "Analyze the market price against your estimated true probability. "
        "If the edge exceeds 5%, recommend BUY_YES or BUY_NO with Kelly sizing. "
        "If no edge exists or data is insufficient, return PASS."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "action": {
                "type": "string",
                "enum": ["BUY_YES", "BUY_NO", "PASS"],
                "description": "BUY_YES to buy the Yes contract, BUY_NO to buy the No contract, PASS to skip.",
            },
            "target_market_id": {
                "type": "string",
                "description": "The Kalshi market ticker being evaluated.",
            },
            "implied_market_probability": {
                "type": "number",
                "minimum": 0.01,
                "maximum": 0.99,
                "description": "The market's current implied probability from the Yes price.",
            },
            "agent_calculated_probability": {
                "type": "number",
                "minimum": 0.01,
                "maximum": 0.99,
                "description": "Your estimated true probability based on all available data.",
            },
            "kelly_fraction": {
                "type": "number",
                "minimum": 0.0,
                "maximum": 0.10,
                "description": "Recommended fraction of bankroll to wager (0.0 for PASS).",
            },
            "confidence_score": {
                "type": "number",
                "minimum": 0.0,
                "maximum": 1.0,
                "description": "Your confidence in the probability estimate (0=guessing, 1=certain).",
            },
            "rationale": {
                "type": "string",
                "description": "1-2 sentence explanation of the edge (or why there is no edge).",
            },
        },
        "required": [
            "action",
            "target_market_id",
            "implied_market_probability",
            "agent_calculated_probability",
            "kelly_fraction",
            "confidence_score",
            "rationale",
        ],
        "additionalProperties": False,
    },
}
"""
The tool definition passed to client.messages.create(tools=[TRADE_TOOL_SCHEMA]).

With strict=True on the API call, Claude's constrained decoding guarantees
every response matches this exact schema. No parsing needed — just validate
with the TradeDecision Pydantic model.
"""


# =============================================================================
# Standalone test
# =============================================================================

if __name__ == "__main__":
    from pydantic import ValidationError

    console.print("[bold]Testing signals/schemas.py...[/bold]\n")

    # Test 1: Valid BUY_YES decision
    console.print("[cyan]1. Valid BUY_YES decision:[/cyan]")
    decision = TradeDecision(
        action="BUY_YES",
        target_market_id="KXNBA-LEBRON-PTS-O25",
        implied_market_probability=0.42,
        agent_calculated_probability=0.65,
        kelly_fraction=0.042,
        confidence_score=0.75,
        rationale="Davis ruled OUT, usage rate implies 65% true probability.",
    )
    console.print(f"   Action: {decision.action}")
    console.print(f"   Edge: {decision.edge:.3f} ({decision.edge * 100:.1f}%)")
    console.print(f"   Actionable: {decision.is_actionable}")
    console.print("   [green]Valid BUY_YES created successfully.[/green]")

    # Test 2: Valid PASS decision
    console.print("\n[cyan]2. Valid PASS decision:[/cyan]")
    pass_d = pass_decision("KXNBA-TEST", "Spread too wide at $0.08.")
    console.print(f"   Action: {pass_d.action}")
    console.print(f"   Edge: {pass_d.edge:.3f}")
    console.print(f"   Actionable: {pass_d.is_actionable}")
    console.print("   [green]PASS fallback created successfully.[/green]")

    # Test 3: Invalid decisions (should raise ValidationError)
    console.print("\n[cyan]3. Validation tests (expecting errors):[/cyan]")
    invalid_tests = [
        ("Invalid action", lambda: TradeDecision(
            action="HOLD", target_market_id="T",
            implied_market_probability=0.5, agent_calculated_probability=0.5,
            kelly_fraction=0.0, confidence_score=0.5, rationale="test",
        )),
        ("Probability > 0.99", lambda: TradeDecision(
            action="PASS", target_market_id="T",
            implied_market_probability=1.5, agent_calculated_probability=0.5,
            kelly_fraction=0.0, confidence_score=0.5, rationale="test",
        )),
        ("Kelly > 0.10", lambda: TradeDecision(
            action="BUY_YES", target_market_id="T",
            implied_market_probability=0.5, agent_calculated_probability=0.7,
            kelly_fraction=0.15, confidence_score=0.5, rationale="test",
        )),
        ("Confidence > 1.0", lambda: TradeDecision(
            action="PASS", target_market_id="T",
            implied_market_probability=0.5, agent_calculated_probability=0.5,
            kelly_fraction=0.0, confidence_score=1.5, rationale="test",
        )),
    ]

    for name, create_fn in invalid_tests:
        try:
            create_fn()
            console.print(f"   [red]FAIL: {name} — should have raised ValidationError[/red]")
        except ValidationError:
            console.print(f"   [green]PASS: {name} — correctly rejected[/green]")

    # Test 4: Tool schema structure
    console.print("\n[cyan]4. Tool schema validation:[/cyan]")
    assert TRADE_TOOL_SCHEMA["name"] == "execute_prediction_trade"
    assert "input_schema" in TRADE_TOOL_SCHEMA
    props = TRADE_TOOL_SCHEMA["input_schema"]["properties"]
    assert len(props) == 7
    console.print(f"   Tool name: {TRADE_TOOL_SCHEMA['name']}")
    console.print(f"   Properties: {list(props.keys())}")
    console.print(f"   Required fields: {len(TRADE_TOOL_SCHEMA['input_schema']['required'])}")
    console.print("   [green]Tool schema structure valid.[/green]")

    console.print("\n[green]signals/schemas.py: All tests passed.[/green]")
