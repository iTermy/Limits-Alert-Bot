"""
Database connection manager with base operations
"""
import aiosqlite
import asyncio
from pathlib import Path
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager
from utils.logger import get_logger

logger = get_logger("database")


class DatabaseManager:
    """Manages database connections and base operations"""

    def __init__(self, db_path: str = "data/trading_bot.db"):
        """
        Initialize database manager

        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        self._connection: Optional[aiosqlite.Connection] = None
        self._lock = asyncio.Lock()

        # Ensure data directory exists
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    @asynccontextmanager
    async def get_connection(self):
        """
        Get database connection with context manager

        Yields:
            Database connection
        """
        async with self._lock:
            conn = await aiosqlite.connect(self.db_path)
            conn.row_factory = aiosqlite.Row  # Return rows as dictionaries
            await conn.execute("PRAGMA foreign_keys = ON")  # Ensure foreign keys are enabled
            try:
                yield conn
            finally:
                await conn.close()

    async def execute(self, query: str, params: tuple = ()) -> int:
        """
        Execute a query that modifies data

        Args:
            query: SQL query
            params: Query parameters

        Returns:
            Last row ID for inserts, or number of affected rows
        """
        async with self.get_connection() as conn:
            cursor = await conn.execute(query, params)
            await conn.commit()
            return cursor.lastrowid if cursor.lastrowid else cursor.rowcount

    async def execute_many(self, query: str, params_list: List[tuple]) -> int:
        """
        Execute multiple queries with different parameters

        Args:
            query: SQL query
            params_list: List of parameter tuples

        Returns:
            Number of affected rows
        """
        async with self.get_connection() as conn:
            await conn.executemany(query, params_list)
            await conn.commit()
            return len(params_list)

    async def fetch_one(self, query: str, params: tuple = ()) -> Optional[Dict[str, Any]]:
        """
        Fetch a single row

        Args:
            query: SQL query
            params: Query parameters

        Returns:
            Row as dictionary or None
        """
        async with self.get_connection() as conn:
            cursor = await conn.execute(query, params)
            row = await cursor.fetchone()
            return dict(row) if row else None

    async def fetch_all(self, query: str, params: tuple = ()) -> List[Dict[str, Any]]:
        """
        Fetch all rows

        Args:
            query: SQL query
            params: Query parameters

        Returns:
            List of rows as dictionaries
        """
        async with self.get_connection() as conn:
            cursor = await conn.execute(query, params)
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]

    async def close(self):
        """Close database connection"""
        if self._connection:
            await self._connection.close()
            self._connection = None