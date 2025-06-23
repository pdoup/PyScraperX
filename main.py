import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone

from config import ResolveTime, settings
from engine import ScraperEngine
from job_scheduler import JobScheduler
from logger import setup_logger, stop_queue_listener
from models import JobStatus
from report.server import start_web_server
from report.state_manager import state_manager

logger = setup_logger(log_level=settings.log_level.to_logging_level())

# Ensure the database directory exists
os.makedirs("dbs", exist_ok=True)


async def run_all_scrapers_job(engine: ScraperEngine):
    """
    Defines the main scraping task. This coroutine will be scheduled to run periodically.
    """
    job_id = settings.master_scheduler_id
    logger.info("Master scrape job triggered.")

    await state_manager.update_job_status(
        job_id,
        status=JobStatus.RUNNING,
        last_run=datetime.now(timezone.utc),
        next_run=datetime.now(timezone.utc)
        + timedelta(seconds=settings.interval_in_seconds),
        error_message=None,
    )

    try:
        await engine.run_all()

        await state_manager.update_job_status(
            job_id, status=JobStatus.IDLE, error_message=None
        )
        logger.info("Master scrape job completed successfully.")

    except Exception as e:
        logger.error(f"Master scrape job failed: {e}", exc_info=True)
        await state_manager.update_job_status(
            job_id,
            status=JobStatus.FAILED,
            error_message=f"Execution failed: {e}",
        )


async def main():
    """
    Initializes all services, schedules jobs, and runs the application.
    """
    logger.info("Application starting up...")

    # 1. Initialization
    engine = ScraperEngine()
    scheduler = JobScheduler()
    run_interval: ResolveTime = settings.run_interval
    try:
        # Initialize services
        await engine.initialize_http_session()
        logger.info("ScraperEngine HTTP session initialized.")

        # Register the main scheduler job with the state manager
        await state_manager.register_job(
            settings.master_scheduler_id, name="Master Scraper Dispatcher"
        )
        # Update initial status for the main scheduler job
        await state_manager.update_job_status(
            settings.master_scheduler_id,
            status=JobStatus.IDLE,
            last_run=None,
            next_run=None,
            error_message=None,
            total_runs=0,
        )

        # 2. Schedule Jobs
        if run_interval.unit == "seconds":
            scheduler.every_seconds(run_interval.interval).do(
                run_all_scrapers_job, engine
            )
        elif run_interval.unit == "minutes":
            scheduler.every_minutes(run_interval.interval).do(
                run_all_scrapers_job, engine
            )
        elif run_interval.unit == "hours":
            scheduler.every_hours(run_interval.interval).do(
                run_all_scrapers_job, engine
            )
        else:
            logger.error(f"Unsupported run_interval unit: {run_interval.unit}")
            return

        logger.info("Performing immediate first scrape job run...")
        await run_all_scrapers_job(engine)

        # 3. Run Forever
        logger.info("WebScraper is now running. Press Ctrl+C to exit.")
        await asyncio.Future()

    except asyncio.CancelledError:
        logger.info("Shutdown signal received (asyncio.CancelledError).")
    except Exception as e:
        logger.critical(
            f"An unhandled error occurred in the async main loop: {e}", exc_info=True
        )

    finally:
        logger.info("Shutting down services gracefully...")
        if engine.is_initialized():
            await engine.close()
            logger.info("ScraperEngine HTTP session closed.")

        # Cancel all tasks started by the scheduler
        for task in scheduler.get_tasks():
            task.cancel()

        # Await their completion to ensure cleanup and avoid RuntimeError on exit
        await asyncio.gather(*scheduler.get_tasks(), return_exceptions=True)
        logger.info("All background tasks cancelled. Shutdown complete.")


if __name__ == "__main__":
    logger.info(
        f"WebScraper service started with options: {settings.model_dump(exclude_unset=True, exclude_none=True, serialize_as_any=True)}"
    )

    try:
        # Start the FastAPI web server in a separate thread.
        start_web_server()
        # Run the main asynchronous application
        asyncio.run(main())

    except KeyboardInterrupt:
        logger.info("Web scraper service stopped by user (KeyboardInterrupt).")
    except Exception as e:
        logger.critical(
            f"An unhandled error occurred in the synchronous main block: {e}",
            exc_info=True,
        )
    finally:
        logger.info("Application process exiting.")
        stop_queue_listener()
