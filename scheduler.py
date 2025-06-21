import asyncio
import logging
import time
from datetime import datetime
from threading import Thread

from engine import ScraperEngine
from models import JobStatus
from report.state_manager import update_job_status

logger = logging.getLogger("WebScraper")

_loop = None
_scraper_engine = None
_main_scheduler_job_ref = None


def start_async_loop():
    """Starts and runs the asyncio event loop in a new thread."""
    global _loop
    _loop = asyncio.new_event_loop()
    asyncio.set_event_loop(_loop)
    logger.info("Asyncio event loop started in a new thread.")
    _loop.run_forever()  # Keep the loop running


def scrape_job():
    """
    This function is scheduled by the 'schedule' library.
    It dispatches the asynchronous scraper tasks to the background asyncio loop.
    """
    global _loop, _scraper_engine, _main_scheduler_job_ref

    if _loop is None or _scraper_engine is None:
        logger.error("Asyncio loop or ScraperEngine not initialized. Cannot run job.")
        update_job_status(
            "main_scheduler_job",
            status=JobStatus.FAILED,
            error_message="Asyncio loop or ScraperEngine not initialized.",
        )
        return

    update_job_status(
        "main_scheduler_job",
        status=JobStatus.RUNNING,
        last_run=datetime.now(),
        error_message=None,  # Clear any previous error message
    )
    logger.info("Running all scrapers (scheduled job)...")

    try:
        _ = asyncio.run_coroutine_threadsafe(_scraper_engine.run_all(), _loop)

        update_job_status(
            "main_scheduler_job", status=JobStatus.IDLE, error_message=None
        )
        logger.info("Async scraper task submitted to background loop.")
    except Exception as e:
        logger.error(f"Failed to submit async task: {e}", exc_info=True)
        update_job_status(
            "main_scheduler_job",
            status=JobStatus.FAILED,
            error_message=f"Failed to dispatch: {e}",
        )


async def _async_init_services_task():
    """An async task to perform asynchronous initialization within the event loop."""
    global _scraper_engine
    await _scraper_engine.initialize_http_session()
    logger.info("Asyncio HTTP services initialized within the event loop.")


# Function to initialize background async services
def init_async_services():
    """Initializes the ScraperEngine and starts the async event loop in a thread."""
    global _scraper_engine

    # Initialize ScraperEngine once
    _scraper_engine = ScraperEngine()

    # Start the asyncio loop in a separate daemon thread
    Thread(target=start_async_loop, daemon=True).start()
    logger.info("Background asyncio loop thread started.")

    # Give the loop a moment to start up
    time.sleep(0.5)
    try:
        asyncio.run_coroutine_threadsafe(_async_init_services_task(), _loop).result(
            60.0
        )  # Wait for the async session creation to finish
        logger.info("All async services (including HTTP session) initialized.")
    except Exception as e:
        logger.critical(
            f"FATAL: Failed to initialize async HTTP services: {e}", exc_info=True
        )
        raise

    update_job_status(
        "main_scheduler_job",
        name="Main Scraper Dispatcher",
        status=JobStatus.INITIALIZED,
    )


async def _close_engine_session():
    """Helper coroutine to close the ScraperEngine's session."""
    global _scraper_engine
    if _scraper_engine:
        await _scraper_engine.close()


def stop_async_loop():
    """Stops the asyncio event loop gracefully."""
    global _loop
    if _loop and _loop.is_running():
        asyncio.run_coroutine_threadsafe(_close_engine_session(), _loop)
        _loop.call_soon_threadsafe(_loop.stop)
        logger.info("Asyncio event loop stop requested.")
