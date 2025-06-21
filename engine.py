import asyncio
import logging
import os
from typing import Optional
from urllib.parse import urlparse

import aiohttp
from aiohttp import ClientTimeout

from config import load_urls, settings
from models import JobStatus
from report.state_manager import job_status_data
from scraper import WebScraper

logger = logging.getLogger("WebScraper")


class ScraperEngine:
    db_path_base = os.path.join(os.path.dirname(__file__), "dbs")

    def __init__(self) -> None:
        self.config = load_urls(settings.urls_path)
        self.scrapers: list[WebScraper] = []
        self.http_session: Optional[aiohttp.ClientSession] = None
        logger.info("ScraperEngine initialized (HTTP session not yet created).")

    async def initialize_http_session(self):
        """
        Initializes the aiohttp.ClientSession asynchronously.
        This must be called AFTER the asyncio event loop has started.
        """
        if self.http_session is None or self.http_session.closed:
            self.http_session = aiohttp.ClientSession(
                timeout=ClientTimeout(total=10),
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    "Accept": "application/json",
                },
            )
            logger.info(
                "ScraperEngine: aiohttp.ClientSession initialized asynchronously."
            )
            self._initialize_scrapers()
        else:
            logger.debug("ScraperEngine: http_session already initialized.")

    def _initialize_scrapers(self):
        """
        Initializes WebScraper instances for each URL in the config.
        This method now relies on self.http_session being already initialized.
        """
        if self.http_session is None:
            raise RuntimeError(
                "HTTP session must be initialized before initializing scrapers."
            )

        os.makedirs(self.db_path_base, exist_ok=True)

        for i, url_str in enumerate(self.config.urls):
            parsed_url = urlparse(url_str.unicode_string())
            hostname = (
                parsed_url.hostname if parsed_url.hostname else f"unknown_host_{i}"
            )
            db_filename = f"db_{hostname}_{i}.sqlite3"
            full_db_path = os.path.join(self.db_path_base, db_filename)

            logger.info(
                "Setting up WebScraper for URL: %s (DB: %s)",
                url_str.unicode_string(),
                db_filename,
            )
            self.scrapers.append(WebScraper(url_str, full_db_path, self.http_session))

    async def run_all(self):
        if not self.http_session or self.http_session.closed:
            logger.error(
                "HTTP session is not initialized or closed. Cannot run scrapers."
            )
            return

        logger.info("ScraperEngine: Starting all configured scrapers...")

        tasks = []
        for scraper in self.scrapers:
            job_status = job_status_data.get(scraper.job_id, {})
            if job_status.get("status") == JobStatus.PERMANENTLY_FAILED:
                logger.warning(f"Skipping permanently failed scraper: {scraper.job_id}")
                continue
            tasks.append(scraper(show=False))

        if tasks:
            await asyncio.gather(*tasks)
            logger.info(
                f"ScraperEngine: {len(tasks)}/{len(self.scrapers)} scrapers finished their run."
            )
        else:
            logger.info("No active scrapers to run (exceeded max fail retries)")

    async def close(self):
        """
        Closes the shared aiohttp.ClientSession gracefully.
        This method should be called during application shutdown.
        """
        if self.http_session and not self.http_session.closed:
            await self.http_session.close()
            logger.info("ScraperEngine: aiohttp.ClientSession closed.")
            self.http_session = None  # Clear reference
