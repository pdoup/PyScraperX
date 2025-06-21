import logging
import time
from typing import TYPE_CHECKING, Callable, List

import schedule

from report.state_manager import update_job_status

if TYPE_CHECKING:
    import scheduler

logger = logging.getLogger("WebScraper")


class JobScheduler:
    """
    A simplified wrapper around the 'schedule' library for scheduling jobs.
    Allows jobs to be scheduled with a fluent API for seconds, minutes, or hours.
    """

    def __init__(self):
        self._scheduled_jobs_refs: List[schedule.Job] = (
            []
        )  # Store references to schedule.Job objects
        logger.info("JobScheduler initialized.")

    def every_seconds(self, interval: int) -> "JobDefinition":
        """Schedules a job to run every specified number of seconds."""
        job_definition = JobDefinition(self, interval, "seconds")
        return job_definition

    def every_minutes(self, interval: int) -> "JobDefinition":
        """Schedules a job to run every specified number of minutes."""
        job_definition = JobDefinition(self, interval, "minutes")
        return job_definition

    def every_hours(self, interval: int) -> "JobDefinition":
        """Schedules a job to run every specified number of hours."""
        job_definition = JobDefinition(self, interval, "hours")
        return job_definition

    def _register_job_ref(self, job_obj: schedule.Job):
        """Internal method to record references to schedule.Job objects."""
        self._scheduled_jobs_refs.append(job_obj)
        # If the scheduled job is our main 'job' function, store its reference globally
        if job_obj.job_func.__name__ == "scrape_job":
            import scheduler  # Import here to avoid circular dependency at top-level

            scheduler._main_scheduler_job_ref = job_obj
            logger.info(f"Registered main scheduler job reference: {job_obj}")

    def run_pending_continuously(self, check_interval_seconds: int = 1):
        """
        Runs scheduled jobs continuously, checking for pending jobs every second.
        This method will block the current thread.
        """
        logger.info(
            f"JobScheduler: Starting continuous loop (checking every {check_interval_seconds}s)."
        )

        for job_info in schedule.get_jobs():
            logger.info(f"JobScheduler: Scheduled job: {job_info}")

        while True:
            schedule.run_pending()

            current_next_run = schedule.next_run()
            if current_next_run:
                logger.debug(
                    f"Updating main_scheduler_job next_run to: {current_next_run}"
                )
                update_job_status("main_scheduler_job", next_run=current_next_run)
            else:
                logger.debug(
                    "main_scheduler_job has no next_run scheduled (current_next_run is None)."
                )
                update_job_status("main_scheduler_job", next_run=None)

            time.sleep(check_interval_seconds)


class JobDefinition:
    """
    A helper class to allow chaining of .do() calls, mimicking schedule's API.
    """

    def __init__(self, scheduler: JobScheduler, interval: int, unit: str):
        self._scheduler = scheduler
        self._interval = interval
        self._unit = unit
        self._job = schedule.every(interval)  # Start the schedule chaining

        if unit == "seconds":
            self._job = self._job.seconds
        elif unit == "minutes":
            self._job = self._job.minutes
        elif unit == "hours":
            self._job = self._job.hours
        else:
            raise ValueError(f"Unsupported time unit: {unit}")

    def do(self, job_func: Callable, *args, **kwargs):
        """
        Assigns the function to be executed by the scheduled job.
        """
        job_obj = self._job.do(job_func, *args, **kwargs)
        self._scheduler._register_job_ref(job_obj)  # Register the job object
        logger.info(
            f"JobScheduler: Job '{job_func.__name__}' configured for every {self._interval} {self._unit}."
        )
