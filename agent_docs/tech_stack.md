# Tech Stack & Tools

- **Runtime:** Python 3.10+ with `asyncio` (hard requirement — zero blocking calls)
- **AI Reasoning Engine:** Anthropic Claude Haiku via `anthropic` SDK (structured outputs + prompt caching)
- **Exchange API:** Kalshi (`kalshi_python_async` SDK, RSA-PSS authentication)
- **Database:** Supabase PostgreSQL via `supabase-py` async client (`acreate_client()`)
- **Data Feeds:** `nba_api` (historical NBA stats), BallDontLie REST API (live game data, polling only)
- **Alerts:** Telegram Bot API via `python-telegram-bot`
- **Terminal UI:** `rich` library (color-coded output)
- **Schema Validation:** `pydantic` (all external data + Claude outputs)
- **Resilience:** `aiobreaker` (circuit breaker), `tenacity` (retry with exponential backoff)
- **HTTP Client:** `aiohttp` or `httpx` (async only — never `requests`)
- **WebSocket:** `websockets` library
- **Config:** `python-dotenv` (loads `.env`)

## Setup Commands
```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# .venv\Scripts\activate   # Windows

# Install all dependencies
pip install anthropic kalshi_python_async aiohttp websockets supabase pydantic python-telegram-bot rich tenacity aiobreaker nba_api python-dotenv httpx

# Freeze versions
pip freeze > requirements.txt

# Verify installation
python -c "import anthropic; print('Anthropic OK')"
python -c "from supabase import create_client; print('Supabase OK')"
python -c "import kalshi_python_async; print('Kalshi OK')"
python -c "from rich.console import Console; Console().print('[green]Rich OK[/green]')"
```

## Error Handling Pattern
```python
from rich.console import Console

console = Console()

async def safe_api_call(func, *args, **kwargs):
    """
    Wraps any async API call with structured error handling.
    Logs errors to terminal via rich. Never crashes the main loop.
    """
    try:
        return await func(*args, **kwargs)
    except asyncio.TimeoutError:
        console.print("[red]TIMEOUT:[/red] API call timed out. Retrying...", style="bold red")
        return None
    except Exception as e:
        console.print(f"[red]ERROR:[/red] {type(e).__name__}: {e}", style="bold red")
        # Log to Supabase asynchronously (non-blocking)
        await log_error_to_supabase(func.__name__, str(e))
        return None
```

## Async Pattern (canonical example)
```python
import asyncio

async def main():
    """
    Main entry point. Runs all tasks concurrently.
    If any task crashes, others continue running.
    """
    await asyncio.gather(
        kalshi_ws_feed(),       # WebSocket: orderbook updates
        nba_data_poller(),      # Poll NBA stats every 30-60s
        signal_evaluator(),     # Check for trading opportunities
        position_monitor(),     # Track open positions
        watchdog(),             # Health checks every 10s
        return_exceptions=True  # Don't kill all tasks if one fails
    )

if __name__ == "__main__":
    asyncio.run(main())
```

## Claude API Pattern (structured tool use)
```python
import anthropic

client = anthropic.Anthropic()

tools = [{
    "name": "execute_prediction_trade",
    "description": "Execute a trade decision based on detected edge.",
    "strict": True,  # Guarantees schema-compliant JSON
    "input_schema": {
        "type": "object",
        "properties": {
            "action": {"type": "string", "enum": ["BUY_YES", "BUY_NO", "PASS"]},
            "target_market_id": {"type": "string"},
            "implied_market_probability": {"type": "number"},
            "agent_calculated_probability": {"type": "number"},
            "kelly_fraction": {"type": "number"},
            "confidence_score": {"type": "number"},
            "rationale": {"type": "string"}
        },
        "required": ["action", "target_market_id", "implied_market_probability",
                      "agent_calculated_probability", "kelly_fraction", "rationale"],
        "additionalProperties": False
    }
}]

# System prompt must exceed 4,096 tokens for caching to activate
response = client.messages.create(
    model="claude-haiku-4-5-20250415",
    max_tokens=1024,
    system=[{
        "type": "text",
        "text": SYSTEM_PROMPT,  # From signals/prompts.py
        "cache_control": {"type": "ephemeral"}  # 5-minute cache
    }],
    tools=tools,
    messages=[{"role": "user", "content": market_state_payload}]
)

# Verify caching is working
print(f"Cache hits: {response.usage.cache_read_input_tokens}")
```

## Kalshi API Pattern (RSA-PSS auth)
```python
# Prices are FIXED-POINT DOLLAR STRINGS, not floats
order = {
    "ticker": "KXNBA-SOMEMARKET",
    "side": "yes",
    "action": "buy",
    "count_fp": "10.00",
    "yes_price_dollars": "0.6500",  # NOT 0.65 or 65
    "client_order_id": str(uuid.uuid4())
}
```

## Naming Conventions
- **Files:** `snake_case.py` (e.g., `kalshi_client.py`, `nba_poller.py`)
- **Functions:** `snake_case` (e.g., `calculate_kelly_fraction()`)
- **Classes:** `PascalCase` (e.g., `TradeDecision`, `MarketState`)
- **Constants:** `UPPER_SNAKE_CASE` (e.g., `MAX_POSITION_PCT`, `FRACTIONAL_KELLY`)
- **Pydantic Models:** `PascalCase` with descriptive names (e.g., `KalshiOrderPayload`)
