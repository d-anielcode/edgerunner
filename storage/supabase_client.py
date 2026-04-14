"""
Async Supabase client for EdgeRunner.

This module provides a lazy-initialized singleton async Supabase client.
All database operations across the entire project go through here.

Why lazy singleton? Because acreate_client() is async and can't run at
module import time. The first caller pays the connection cost, and all
subsequent callers get the cached client.

Why helper functions? They wrap Supabase operations with error handling
so that a database failure NEVER crashes the trading loop.
"""

import asyncio
from typing import Any

from rich.console import Console
from supabase import acreate_client
from supabase._async.client import AsyncClient

from config.settings import SUPABASE_ANON_KEY, SUPABASE_URL

console = Console()

# Lazy singleton — initialized on first get_client() call
_client: AsyncClient | None = None

# Table name constants — every query uses these, never raw strings
TABLE_MARKETS: str = "markets"
TABLE_TRADES: str = "trades"
TABLE_POSITIONS: str = "positions"
TABLE_DAILY_PNL: str = "daily_pnl"
TABLE_BRIER_SCORES: str = "brier_scores"
TABLE_FILLS: str = "fills"
TABLE_PORTFOLIO_SNAPSHOTS: str = "portfolio_snapshots"


async def get_client() -> AsyncClient:
    """
    Get the async Supabase client, initializing on first call.

    Returns the cached client on subsequent calls. This is the ONLY
    way to get a database connection in the entire project.
    """
    global _client
    if _client is not None:
        return _client

    try:
        _client = await acreate_client(SUPABASE_URL, SUPABASE_ANON_KEY)
        console.print("[blue]Supabase: Connected successfully.[/blue]")
        return _client
    except Exception as e:
        console.print(f"[red]Supabase: Connection failed — {type(e).__name__}: {e}[/red]")
        raise


async def close_client() -> None:
    """
    Gracefully close the Supabase client.

    Called from main.py during shutdown to clean up resources.
    """
    global _client
    if _client is not None:
        # supabase-py async client doesn't have an explicit close method,
        # but we clear the reference so a new one can be created if needed.
        _client = None
        console.print("[blue]Supabase: Client closed.[/blue]")


async def insert_row(table: str, data: dict[str, Any]) -> dict[str, Any] | None:
    """
    Insert a single row into a Supabase table.

    Returns the inserted row dict on success, None on failure.
    Failures are logged but NEVER propagated — the trading loop must not crash
    because of a database write failure.
    """
    try:
        client = await get_client()
        result = await client.table(table).insert(data).execute()
        if result.data:
            return result.data[0]
        return None
    except Exception as e:
        console.print(
            f"[red]Supabase INSERT failed on '{table}': "
            f"{type(e).__name__}: {e}[/red]"
        )
        return None


async def upsert_row(table: str, data: dict[str, Any]) -> dict[str, Any] | None:
    """
    Insert or update a row in a Supabase table.

    Uses Supabase's upsert which inserts if the row doesn't exist,
    or updates if it does (based on primary key).
    Returns the upserted row dict on success, None on failure.
    """
    try:
        client = await get_client()
        result = await client.table(table).upsert(data).execute()
        if result.data:
            return result.data[0]
        return None
    except Exception as e:
        console.print(
            f"[red]Supabase UPSERT failed on '{table}': "
            f"{type(e).__name__}: {e}[/red]"
        )
        return None


async def fetch_rows(
    table: str,
    filters: dict[str, Any] | None = None,
    limit: int = 100,
    order_by: str | None = None,
    descending: bool = True,
) -> list[dict[str, Any]]:
    """
    Fetch rows from a Supabase table with optional filtering and ordering.

    Returns a list of row dicts. Returns empty list on failure.
    Never crashes the trading loop.
    """
    try:
        client = await get_client()
        query = client.table(table).select("*")

        if filters:
            for key, value in filters.items():
                query = query.eq(key, value)

        if order_by:
            query = query.order(order_by, desc=descending)

        query = query.limit(limit)
        result = await query.execute()
        return result.data if result.data else []
    except Exception as e:
        console.print(
            f"[red]Supabase SELECT failed on '{table}': "
            f"{type(e).__name__}: {e}[/red]"
        )
        return []


# =============================================================================
# Standalone test
# =============================================================================

if __name__ == "__main__":

    async def _test() -> None:
        """Test Supabase connection and basic operations."""
        console.print("[bold]Testing Supabase client...[/bold]")

        try:
            client = await get_client()
            console.print("[green]Connection: OK[/green]")

            # Try a simple query to verify the connection works
            result = await client.table(TABLE_MARKETS).select("*").limit(1).execute()
            console.print(
                f"[green]Query test: OK (returned {len(result.data)} rows)[/green]"
            )
        except Exception as e:
            console.print(
                f"[yellow]Connection test failed: {type(e).__name__}: {e}[/yellow]\n"
                f"[yellow]This is expected if Supabase tables haven't been "
                f"created yet or credentials are placeholders.[/yellow]"
            )
        finally:
            await close_client()
            console.print("[green]storage/supabase_client.py: Test complete.[/green]")

    asyncio.run(_test())
