import asyncio
import hashlib
import logging
import os
import os.path as osp
from typing import Optional
from urllib.parse import urlparse

import aiohttp
from aiohttp import ClientTimeout
from dotenv import load_dotenv
from pydantic import HttpUrl

from config import load_urls, settings
from models import JobStatus
from report.state_manager import state_manager
from scraper import WebScraper

load_dotenv(
    dotenv_path=settings.model_config.get("env_file"), override=True, verbose=True
)

logger = logging.getLogger("WebScraper")


class ScraperEngine:
    db_path_base = osp.join(osp.dirname(__file__), "dbs")

    def __init__(self) -> None:
        self.config = load_urls(settings.urls_path)
        self.scrapers: list[WebScraper] = []
        self.http_session: Optional[aiohttp.ClientSession] = None
        self._initialized: bool = False
        logger.info("ScraperEngine initialized (HTTP session not yet created).")

    async def initialize_http_session(self):
        """
        Initializes the aiohttp.ClientSession asynchronously and then
        initializes the WebScraper instances and their job statuses.
        """
        if self.http_session is None or self.http_session.closed:
            self.http_session = aiohttp.ClientSession(
                timeout=ClientTimeout(total=settings.http_client_timeout_seconds),
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    "Accept": "application/json",
                },
                trust_env=True,  # Accept proxies from environment variables
            )
            logger.info(
                "ScraperEngine: aiohttp.ClientSession initialized asynchronously."
            )
            await self._initialize_scrapers()
            self._initialized = True
        else:
            logger.debug("ScraperEngine: http_session already initialized.")

    def _get_db_file_path(self, url_str: HttpUrl) -> str:
        """
        Constructs the full database file path for a given URL.
        The filename is composed of a sanitized hostname and a short hash of the full URL,
        ensuring uniqueness and descriptiveness for different endpoints on the same host.

        Args:
            url_str: The Pydantic HttpUrl object for which to generate the DB path.

        Returns:
            The complete file path for the SQLite database.
        """
        parsed_url = urlparse(url_str.unicode_string())
        hostname = parsed_url.hostname if parsed_url.hostname else "unknown_host"

        sanitized_hostname = "".join(
            c for c in hostname if c.isalnum() or c == "."
        ).replace(".", "_")

        url_hash = hashlib.sha256(url_str.unicode_string().encode("utf-8")).hexdigest()[
            :8
        ]

        db_filename = f"db_{sanitized_hostname}_{url_hash}.sqlite3"
        full_db_path = osp.join(self.db_path_base, db_filename)
        return full_db_path

    async def _initialize_scrapers(self):
        """
        Initializes WebScraper instances for each URL in the config and
        registers/initializes their job statuses in the state manager.
        """
        if self.http_session is None:
            raise RuntimeError(
                "HTTP session must be initialized before initializing scrapers."
            )

        os.makedirs(self.db_path_base, exist_ok=True)

        initialization_tasks = []
        for url_str in self.config.urls:
            full_db_path = self._get_db_file_path(url_str)

            logger.info(
                "Setting up WebScraper for URL: %s (DB: %s)",
                url_str.unicode_string(),
                osp.basename(full_db_path),
            )
            scraper = WebScraper(url_str, full_db_path, self.http_session)
            self.scrapers.append(scraper)
            initialization_tasks.append(scraper.initialize_job_status())

        if initialization_tasks:
            await asyncio.gather(*initialization_tasks)
            logger.info(
                f"Initialized job statuses for {len(initialization_tasks)} scrapers."
            )

    def is_initialized(self) -> bool:
        """
        Returns True if the ScraperEngine's HTTP session and scrapers have been initialized, False otherwise.
        """
        return (
            self._initialized
            and self.http_session is not None
            and not self.http_session.closed
        )

    async def run_all(self):
        if not self.is_initialized():
            logger.error(
                "ScraperEngine is not initialized. Call initialize_http_session() first."
            )
            return

        logger.info("ScraperEngine: Starting all configured scrapers...")

        tasks = []
        for scraper in self.scrapers:
            job_state = await state_manager.get_job_status(scraper.job_id)

            if job_state and job_state.status == JobStatus.PERMANENTLY_FAILED:
                logger.warning(f"Skipping permanently failed scraper: {scraper.job_id}")
                continue
            tasks.append(scraper(show=False))

        if tasks:
            await asyncio.gather(*tasks)
            logger.info(
                f"ScraperEngine: {len(tasks)}/{len(self.scrapers)} scrapers finished their run."
            )
        else:
            logger.info(
                "No active scrapers to run (all permanently failed or no scrapers configured)."
            )

    async def close(self):
        """
        Closes the shared aiohttp.ClientSession gracefully.
        """
        if self.http_session and not self.http_session.closed:
            await self.http_session.close()
            logger.info("ScraperEngine: aiohttp.ClientSession closed.")
            self.http_session = None
        self._initialized = False
