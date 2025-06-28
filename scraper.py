import asyncio
import datetime
import hashlib
import logging
import time
from urllib.parse import urlparse

import aiohttp
from aiohttp import ClientResponseError
from pydantic import HttpUrl

from config import settings
from database import DatabaseManager
from fetcher import DataFetcher
from models import JobStatus
from report.state_manager import state_manager

logger = logging.getLogger("WebScraper")


class WebScraper:
    def __init__(self, endpoint: HttpUrl, db_path: str, session: aiohttp.ClientSession):
        self.endpoint = endpoint
        self.db_path = db_path
        self.fetcher = DataFetcher(endpoint, session)
        self.db_manager = DatabaseManager(db_path)
        self.lock = asyncio.Lock()

        # Create a unique job ID for this scraper instance
        self.job_id = self._generate_job_id(endpoint)

    @staticmethod
    def _generate_job_id(endpoint: HttpUrl) -> str:
        """
        Generates a unique job ID based on the endpoint URL.
        It uses a sanitized hostname and a short hash of the full URL for uniqueness.

        Args:
            endpoint: The HTTP URL endpoint for the scraper.

        Returns:
            A unique string identifier for the job.
        """
        hostname = urlparse(endpoint.unicode_string()).hostname
        # Sanitize hostname by replacing dots with underscores for consistency
        sanitized_hostname = "".join(
            c for c in hostname if c.isalnum() or c == "."
        ).replace(".", "_")

        # Generate a short, stable hash of the full URL string
        url_hash = hashlib.sha256(
            endpoint.unicode_string().encode("utf-8")
        ).hexdigest()[:8]

        return f"{sanitized_hostname}_{url_hash}"

    async def initialize_job_status(self):
        """
        Registers and initializes the job status for this scraper instance.
        """
        await state_manager.register_job(self.job_id, name=str(self.endpoint))
        await state_manager.update_job_status(
            self.job_id,
            status=JobStatus.INITIALIZED,
            total_runs=0,
            success_count=0,
            fail_count=0,
            next_run=None,
        )

    async def __call__(self, show: bool = False):
        async with self.lock:
            start_time = time.monotonic()

            current_job_state = await state_manager.get_job_status(self.job_id)
            if not current_job_state:
                logger.error(
                    f"Job state for '{self.job_id}' not found. Cannot proceed."
                )
                return

            current_retry_count = current_job_state.retry_count
            job_max_retries = current_job_state.max_retries or settings.max_retries

            if current_job_state.status == JobStatus.PERMANENTLY_FAILED:
                logger.warning(
                    f"Skipping permanently failed scraper: {self.job_id} ({current_job_state.error_message or 'No error message'})"
                )
                return

            main_dispatcher_status = await state_manager.get_job_status(
                settings.master_scheduler_id
            )
            next_run_time = (
                main_dispatcher_status.next_run if main_dispatcher_status else None
            )

            await state_manager.update_job_status(
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
                    async with self.db_manager as db_manager:
                        if not await db_manager.is_initialized():
                            await db_manager.initialize(records[0])
                        await db_manager.insert(records)

                        if show:
                            await db_manager.select_all()

                end_time = time.monotonic()
                duration_ms = (end_time - start_time) * 1000

                await state_manager.update_job_status(
                    self.job_id,
                    status=JobStatus.SUCCESS,
                    duration_ms=duration_ms,
                    success_count=current_job_state.success_count + 1,
                    total_runs=current_job_state.total_runs + 1,
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

                await state_manager.update_job_status(
                    self.job_id,
                    status=final_status,
                    error_message=error_message,
                    duration_ms=duration_ms,
                    fail_count=current_job_state.fail_count + 1,
                    total_runs=current_job_state.total_runs + 1,
                    next_run=next_run_time,
                    retry_count=current_retry_count,
                )
