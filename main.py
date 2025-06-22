import os

from config import ResolveTime, settings
from job_scheduler import JobScheduler
from logger import setup_logger, stop_queue_listener
from report.server import start_web_server
from scheduler import init_async_services, scrape_job, stop_async_loop

logger = setup_logger()

os.makedirs("dbs", exist_ok=True)

start_web_server()

# Initialize the async event loop and ScraperEngine in a background thread
init_async_services()

# Instantiate JobScheduler
app_scheduler: JobScheduler = JobScheduler()
run_interval: ResolveTime = settings.run_interval

if run_interval.unit == "seconds":
    app_scheduler.every_seconds(run_interval.interval).do(scrape_job)
elif run_interval.unit == "minutes":
    app_scheduler.every_minutes(run_interval.interval).do(scrape_job)

if __name__ == "__main__":
    logger.info(
        f"Web scraper service started with options: {settings.model_dump(exclude_unset=False, exclude_none=False, serialize_as_any=True)}"
    )
    try:
        scrape_job()  # Initial synchronous call to dispatch the async job
        app_scheduler.run_pending_continuously(check_interval_seconds=1)
    except KeyboardInterrupt:
        logger.info("Web scraper service stopped by user.")
    except Exception as e:
        logger.critical(
            f"An unhandled error occurred in the main loop: {e}", exc_info=True
        )
    finally:
        # Attempt to stop the asyncio loop gracefully on exit
        stop_async_loop()
        stop_queue_listener()
        logger.info("Web scraper service shut down.")
