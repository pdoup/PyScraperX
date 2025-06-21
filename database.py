import logging
import os
from contextlib import asynccontextmanager
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Dict, List

import aiosqlite
from tabulate import tabulate

logger = logging.getLogger("WebScraper")


class DatabaseManager:
    def __init__(self, db_path: str):
        self.db_path = db_path

    @asynccontextmanager
    async def _get_connection(self):
        """Helper method to establish and yield a database connection."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            yield db

    def db_connection_required(func: Callable[..., Any]):
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            try:
                async with self._get_connection() as db:
                    return await func(self, db, *args, **kwargs)
            except Exception as e:
                logger.error(f"Database operation failed in {func.__name__}: {e}")
                raise

        return wrapper

    @db_connection_required
    async def is_initialized(self, db: aiosqlite.Connection) -> bool:
        """
        Checks if the 'records' table exists in the database.
        """
        cursor = await db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='records'"
        )
        table_exists = await cursor.fetchone()
        return table_exists is not None

    @db_connection_required
    async def initialize(self, db: aiosqlite.Connection, sample: Dict):
        """
        Initializes the database schema by dropping and recreating the 'records' table
        based on the keys of a sample dictionary.
        """
        await db.execute("DROP TABLE IF EXISTS records")
        columns = ", ".join([f"{k} TEXT" for k in sample.keys()])
        await db.execute(f"CREATE TABLE records ({columns})")
        await db.commit()
        logger.info("Database schema initialized")

    @db_connection_required
    async def insert(self, db: aiosqlite.Connection, records: List[Dict]):
        """
        Inserts a list of records into the 'records' table.
        """
        if not records:
            return

        keys = records[0].keys()
        cols = ", ".join(keys)
        placeholders = ", ".join(["?" for _ in keys])

        await db.executemany(
            f"INSERT INTO records ({cols}) VALUES ({placeholders})",
            [tuple(str(rec[k]) for k in keys) for rec in records],
        )
        await db.commit()
        logger.info(
            f"Inserted {len(records)} records into '{Path(self.db_path).stem}'."
        )

    @db_connection_required
    async def select_all(self, db: aiosqlite.Connection):
        """
        Runs a SELECT * query on the 'records' table and pretty-prints the result
        using the tabulate library.
        """
        cursor = await db.execute("SELECT * FROM records")
        rows = await cursor.fetchall()

        if not rows:
            logger.info("No records found in the database.")
            return

        headers = [description[0] for description in cursor.description]
        data = [list(row) for row in rows[:5]]

        print(f"\n--- All Records '{os.path.basename(self.db_path)}' ---\n")
        print(tabulate(data, headers=headers, tablefmt="grid", showindex=True))
        print("\n-------------------\n")
