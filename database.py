import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import aiosqlite
from tabulate import tabulate

logger = logging.getLogger("WebScraper")


class DatabaseManager:
    """
    An asynchronous context manager for handling SQLite database operations.

    Manages a single, persistent connection for the lifetime of the 'async with'
    block.

    Usage:
        async with DatabaseManager("my_database.db") as db_manager:
            await db_manager.initialize(...)
            await db_manager.insert(...)
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._connection: Optional[aiosqlite.Connection] = None

    async def __aenter__(self):
        """Establishes the database connection."""
        logger.debug(f"Connecting to database at {self.db_path}...")
        self._connection = await aiosqlite.connect(self.db_path)
        self._connection.row_factory = aiosqlite.Row
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Closes the database connection."""
        if self._connection:
            logger.debug("Closing database connection.")
            await self._connection.close()

    @property
    def connection(self) -> aiosqlite.Connection:
        """
        Provides access to the connection, ensuring it has been initialized.
        """
        if self._connection is None:
            raise TypeError("DatabaseManager must be used with 'async with'.")
        return self._connection

    async def is_initialized(self) -> bool:
        """
        Checks if the 'records' table exists in the database.
        """
        cursor = await self.connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='records'"
        )
        table_exists = await cursor.fetchone()
        return table_exists is not None

    async def initialize(self, sample: Dict):
        """
        Initializes the database schema based on a sample dictionary.
        """
        await self.connection.execute("DROP TABLE IF EXISTS records")
        columns = ", ".join([f'"{k}" TEXT' for k in sample.keys()])
        await self.connection.execute(f"CREATE TABLE records ({columns})")
        await self.connection.commit()
        logger.info(
            f"Database schema initialized at '{Path(self.db_path).stem}' with columns={set(sample.keys())}"
        )

    async def insert(self, records: List[Dict]):
        """
        Inserts a list of records into the 'records' table.
        """
        if not records:
            return

        keys = records[0].keys()
        cols = ", ".join([f'"{k}"' for k in keys])
        placeholders = ", ".join(["?" for _ in keys])

        await self.connection.executemany(
            f"INSERT INTO records ({cols}) VALUES ({placeholders})",
            [tuple(str(rec.get(k, "")) for k in keys) for rec in records],
        )
        await self.connection.commit()
        logger.info(
            f"Inserted {len(records)} record(s) into '{Path(self.db_path).stem}'."
        )

    async def select_all(self):
        """
        Selects all records and prints them in a formatted table.
        """
        cursor = await self.connection.execute("SELECT * FROM records")
        rows = await cursor.fetchall()

        if not rows:
            logger.info("No records found in the database.")
            return

        headers = [description[0] for description in cursor.description]
        data = [list(row) for row in rows[:5]]  # Limiting to 5 rows for display

        print(f"\n--- Records from '{os.path.basename(self.db_path)}' (first 5) ---\n")
        print(tabulate(data, headers=headers, tablefmt="grid", showindex=True))
        print("\n-------------------\n")
