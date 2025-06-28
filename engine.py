import asyncio
import hashlib
import logging
import os
import os.path as osp
from typing import List, Optional
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
        self.scrapers: List[WebScraper] = []
        self._http_session: Optional[aiohttp.ClientSession] = None
        self._semaphore = asyncio.Semaphore(value=settings.concurrent_scrapers)
        logger.info(
            f"ScraperEngine initialized. Concurrency limit: {settings.concurrent_scrapers}."
        )

    async def __aenter__(self):
        """Initializes the engine, making it ready to run."""
        self._http_session = aiohttp.ClientSession(
            timeout=ClientTimeout(total=settings.http_client_timeout_seconds),
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json",
            },
            trust_env=True,
        )
        await self._initialize_scrapers()
        logger.info(
            "ScraperEngine entered context: HTTP session and scrapers are ready."
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Closes the HTTP session gracefully."""
        if self._http_session and not self._http_session.closed:
            await self._http_session.close()
            logger.info("ScraperEngine exited context: HTTP session closed.")

    def _get_db_file_path(self, url_str: HttpUrl) -> str:
        parsed_url = urlparse(url_str.unicode_string())
        hostname = parsed_url.hostname if parsed_url.hostname else "unknown_host"
        sanitized_hostname = "".join(
            c for c in hostname if c.isalnum() or c == "."
        ).replace(".", "_")
        url_hash = hashlib.sha256(url_str.unicode_string().encode("utf-8")).hexdigest()[
            :8
        ]
        db_filename = f"db_{sanitized_hostname}_{url_hash}.sqlite3"
        return osp.join(self.db_path_base, db_filename)

    async def _initialize_scrapers(self):
        """Initializes scrapers and their job statuses concurrently."""
        if self._http_session is None:
            raise RuntimeError(
                "HTTP session must be initialized before initializing scrapers."
            )

        os.makedirs(self.db_path_base, exist_ok=True)

        self.scrapers = [
            WebScraper(url, self._get_db_file_path(url), self._http_session)
            for url in self.config.urls
        ]

        init_tasks = [s.initialize_job_status() for s in self.scrapers]
        if init_tasks:
            await asyncio.gather(*init_tasks)
            logger.info(f"Initialized job statuses for {len(self.scrapers)} scrapers.")

    async def _run_scraper_safely(self, scraper: WebScraper):
        """A wrapper to run a single scraper within the semaphore."""
        async with self._semaphore:
            logger.debug(f"Acquired semaphore for {scraper.job_id}")
            try:
                return await scraper(show=False)
            finally:
                logger.debug(f"Released semaphore for {scraper.job_id}")

    async def run_all(self):
        """
        Fetches job statuses concurrently, filters out failed jobs,
        and runs the active scrapers with a concurrency limit.
        """
        if not self._http_session or self._http_session.closed:
            logger.error(
                "ScraperEngine is not ready. Use 'async with ScraperEngine()'."
            )
            return

        logger.info("ScraperEngine: Starting all configured scrapers...")

        # 1. Fetch all job statuses concurrently
        status_tasks = [state_manager.get_job_status(s.job_id) for s in self.scrapers]
        job_statuses = await asyncio.gather(*status_tasks)

        # 2. Create a list of scraper tasks to run, filtering out failed ones
        tasks_to_run = []
        for scraper, status in zip(self.scrapers, job_statuses):
            if status and status.status == JobStatus.PERMANENTLY_FAILED:
                logger.warning(f"Skipping permanently failed scraper: {scraper.job_id}")
                continue
            tasks_to_run.append(self._run_scraper_safely(scraper))

        # 3. Run the active scrapers concurrently with robust error handling
        if tasks_to_run:
            results = await asyncio.gather(*tasks_to_run, return_exceptions=True)

            success_count = sum(1 for r in results if not isinstance(r, Exception))
            failure_count = len(results) - success_count

            logger.info(
                f"ScraperEngine run finished. Completed: {success_count}/{failure_count + success_count}. Active Scrapers: ({failure_count + success_count}/{len(self.scrapers)})"
            )
            for result in results:
                if isinstance(result, Exception):
                    logger.error(f"A scraper task failed: {result}", exc_info=False)
        else:
            logger.info("No active scrapers to run.")
