import asyncio
import logging
from typing import Any, Callable, Coroutine, List

logger = logging.getLogger("WebScraper")


class JobScheduler:
    """
    A pure asyncio job scheduler with a fluent API, requiring no external dependencies.
    It works by creating a separate, long-running `asyncio.Task` for each scheduled job.
    """

    def __init__(self):
        self._tasks: List[asyncio.Task] = []
        logger.info("AsyncJobScheduler initialized.")

    def every_seconds(self, interval: int) -> "JobDefinition":
        """Schedules a job to run every specified number of seconds."""
        return JobDefinition(self, interval)

    def every_minutes(self, interval: int) -> "JobDefinition":
        """Schedules a job to run every specified number of minutes."""
        return JobDefinition(self, interval * 60)

    def every_hours(self, interval: int) -> "JobDefinition":
        """Schedules a job to run every specified number of hours."""
        return JobDefinition(self, interval * 3600)

    def _start_job_task(self, coro: Coroutine):
        """
        Internal method to create and register an asyncio.Task for a job's coroutine.
        """
        task = asyncio.create_task(coro)
        self._tasks.append(task)

    def get_tasks(self) -> List[asyncio.Task]:
        """Returns a list of all running job tasks."""
        return self._tasks


class JobDefinition:
    """
    A helper class to allow chaining of .do() calls.
    """

    def __init__(self, scheduler: JobScheduler, interval_seconds: int):
        self._scheduler = scheduler
        self._interval_seconds = interval_seconds
        if interval_seconds <= 0:
            raise ValueError("Job interval must be positive.")

    def do(self, job_func: Callable[..., Coroutine], *args: Any, **kwargs: Any) -> None:
        """
        Assigns the coroutine to be executed and starts it as a background task.
        """

        async def job_runner():
            """The wrapper coroutine that runs the user's job in a loop."""
            logger.info(
                f"Job '{job_func.__name__}' starting. Will run every {self._interval_seconds} seconds."
            )
            while True:
                try:
                    await job_func(*args, **kwargs)
                except asyncio.CancelledError:
                    logger.warning(f"Job '{job_func.__name__}' was cancelled.")
                    break
                except Exception:
                    logger.exception(
                        f"Job '{job_func.__name__}' encountered an unhandled exception."
                    )
                await asyncio.sleep(self._interval_seconds)

        self._scheduler._start_job_task(job_runner())
        logger.info(
            f"Job '{job_func.__name__}' has been scheduled and started as a background task."
        )
