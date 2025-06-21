import asyncio
import datetime
import logging
import os
import time
from urllib.parse import urlparse

import aiohttp
from aiohttp import ClientResponseError
from pydantic import HttpUrl

from config import settings
from database import DatabaseManager
from fetcher import DataFetcher
from models import JobStatus
from report.state_manager import job_status_data, update_job_status

logger = logging.getLogger("WebScraper")


class WebScraper:
    def __init__(self, endpoint: HttpUrl, db_path: str, session: aiohttp.ClientSession):
        self.endpoint = endpoint
        self.db_path = db_path
        self.fetcher = DataFetcher(endpoint, session)
        self.db = DatabaseManager(db_path)
        self.lock = asyncio.Lock()

        # Create a unique job ID for this scraper instance
        hostname = urlparse(endpoint.unicode_string()).hostname
        # Sanitize hostname for use in file names or unique IDs
        sanitized_hostname = "".join(
            c for c in hostname if c.isalnum() or c == "."
        ).replace(".", "_")
        # Use a part of the db_path to make it more unique if multiple endpoints from same host
        db_name_part = os.path.basename(db_path).split(".")[0]
        self.job_id = f"{sanitized_hostname}_{db_name_part}"

        # Initialize status for this specific scraper job
        update_job_status(
            self.job_id,
            name=endpoint,
            status=JobStatus.INITIALIZED,
            total_runs=0,
            success_count=0,
            fail_count=0,
            next_run=None,
        )

    async def __call__(self, show: bool = False):
        async with self.lock:
            start_time = time.monotonic()

            current_status = job_status_data.get(self.job_id, {})
            current_retry_count = current_status.get("retry_count", 0)
            job_max_retries = current_status.get("max_retries", settings.max_retries)

            if current_status.get("status") == JobStatus.PERMANENTLY_FAILED:
                logger.warning(
                    f"Skipping permanently failed scraper: {self.job_id} ({current_status.get('error_message', 'No error message')})"
                )
                return

            main_dispatcher_status = job_status_data.get("main_scheduler_job", {})
            next_run_time = main_dispatcher_status.get("next_run")

            update_job_status(
                self.job_id,
                status=JobStatus.RUNNING,
                last_run=datetime.datetime.now(),
                next_run=next_run_time,
                error_message=None,
            )

            logger.info(f"WebScraper for {self.fetcher.endpoint} running...")

            try:
                records = await self.fetcher.fetch()
                if records:
                    if not await self.db.is_initialized():
                        await self.db.initialize(records[0])
                    await self.db.insert(records)

                    if show:
                        await self.db.select_all()

                end_time = time.monotonic()
                duration_ms = (end_time - start_time) * 1000

                update_job_status(
                    self.job_id,
                    status=JobStatus.SUCCESS,
                    duration_ms=duration_ms,
                    success_count=current_status.get("success_count", 0) + 1,
                    total_runs=current_status.get("total_runs", 0) + 1,
                    next_run=next_run_time,
                    retry_count=0,
                )
                logger.info(
                    f"WebScraper for {self.fetcher.endpoint} finished in {duration_ms:.2f} ms."
                )

            except Exception as e:
                end_time = time.monotonic()
                duration_ms = (end_time - start_time) * 1000

                current_retry_count += 1
                final_status = JobStatus.FAILED

                if isinstance(e, ClientResponseError):
                    error_message = f"ClientResponseError {e.status} {e.message} for URL: {e.request_info.real_url}"
                else:
                    error_message = str(e)

                if current_retry_count >= job_max_retries:
                    final_status = JobStatus.PERMANENTLY_FAILED
                    next_run_time = None  # No more runs for this job
                    error_message = f"Permanently failed after {current_retry_count}/{job_max_retries} retries: {error_message}"
                    logger.critical(
                        f"Scraper {self.job_id} has permanently failed: {error_message}"
                    )
                else:
                    logger.error(
                        f"Scraper {self.job_id} failed ({current_retry_count}/{job_max_retries} attempts): {e}"
                    )

                update_job_status(
                    self.job_id,
                    status=final_status,
                    error_message=error_message,
                    duration_ms=duration_ms,
                    fail_count=current_status.get("fail_count", 0) + 1,
                    total_runs=current_status.get("total_runs", 0) + 1,
                    next_run=next_run_time,
                    retry_count=current_retry_count,
                )
