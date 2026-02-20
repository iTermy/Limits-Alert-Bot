"""
Database connection manager using asyncpg for Supabase (PostgreSQL)
"""
import asyncpg
import os
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager
from utils.logger import get_logger

logger = get_logger("database")


class DatabaseManager:
    """Manages database connections and base operations via asyncpg connection pool"""

    def __init__(self, db_url: str = None):
        """
        Initialize database manager.

        Args:
            db_url: PostgreSQL connection string. Falls back to SUPABASE_DB_URL env var.
        """
        self.db_url = db_url or os.environ.get("SUPABASE_DB_URL")
        if not self.db_url:
            raise ValueError(
                "No database URL provided. Set SUPABASE_DB_URL environment variable "
                "or pass db_url to DatabaseManager."
            )
        self._pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        """Create the connection pool. Must be called before any queries."""
        if self._pool is None:
            self._pool = await asyncpg.create_pool(
                dsn=self.db_url,
                min_size=2,
                max_size=10,
                command_timeout=30,
            )
            logger.info("Database connection pool created")

    @asynccontextmanager
    async def get_connection(self):
        """
        Acquire a connection from the pool within a transaction.

        Yields:
            asyncpg.Connection
        """
        if self._pool is None:
            await self.connect()
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                yield conn

    async def execute(self, query: str, params: tuple = ()) -> int:
        """
        Execute a query that modifies data.

        For INSERT ... RETURNING id queries, returns the new row id.
        For UPDATE/DELETE, returns the number of affected rows.

        Args:
            query: SQL query using $1, $2, ... placeholders
            params: Query parameters

        Returns:
            New row ID (for INSERT RETURNING id) or affected row count
        """
        if self._pool is None:
            await self.connect()

        # If query returns a value (RETURNING clause), use fetchval
        if "RETURNING" in query.upper():
            async with self._pool.acquire() as conn:
                result = await conn.fetchval(query, *params)
                return result or 0

        # Otherwise execute and return affected row count
        async with self._pool.acquire() as conn:
            status = await conn.execute(query, *params)
            # asyncpg returns status like "UPDATE 3" or "DELETE 1"
            try:
                return int(status.split()[-1])
            except (ValueError, IndexError):
                return 0

    async def execute_many(self, query: str, params_list: List[tuple]) -> int:
        """
        Execute the same query multiple times with different parameters.

        Args:
            query: SQL query using $1, $2, ... placeholders
            params_list: List of parameter tuples

        Returns:
            Number of rows processed
        """
        if self._pool is None:
            await self.connect()
        async with self._pool.acquire() as conn:
            await conn.executemany(query, params_list)
            return len(params_list)

    async def fetch_one(self, query: str, params: tuple = ()) -> Optional[Dict[str, Any]]:
        """
        Fetch a single row.

        Args:
            query: SQL query using $1, $2, ... placeholders
            params: Query parameters

        Returns:
            Row as dictionary or None
        """
        if self._pool is None:
            await self.connect()
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(query, *params)
            return dict(row) if row else None

    async def fetch_all(self, query: str, params: tuple = ()) -> List[Dict[str, Any]]:
        """
        Fetch all matching rows.

        Args:
            query: SQL query using $1, $2, ... placeholders
            params: Query parameters

        Returns:
            List of rows as dictionaries
        """
        if self._pool is None:
            await self.connect()
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(query, *params)
            return [dict(row) for row in rows]

    async def close(self):
        """Close the connection pool."""
        if self._pool:
            await self._pool.close()
            self._pool = None
            logger.info("Database connection pool closed")