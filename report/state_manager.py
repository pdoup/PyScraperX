import asyncio
import datetime
import threading
from typing import Any, Dict, Optional

from config import settings
from models import JobStatus
from report.job_models import RestartJobBatchResponse, RestartJobResponse

# Dictionary to hold the status and metrics of each scraper job
# Key: unique_job_id (e.g., hostname_index)
# Value: Dict containing job details
# Protected by a lock for thread-safe access
job_status_data: Dict[str, Dict[str, Any]] = {}
_job_status_lock = threading.Lock()


def update_job_status(job_id: str, **kwargs):
    """Atomically updates the status of a specific job."""
    with _job_status_lock:
        if job_id not in job_status_data:
            job_status_data[job_id] = {
                "id": job_id,
                "name": job_id,  # Default name, can be overridden by kwargs
                "status": JobStatus.INITIALIZED,  # e.g., Initialized, Running, Success, Failed
                "last_run": None,  # datetime.datetime ISO format string
                "next_run": None,  # datetime.datetime ISO format string
                "duration_ms": 0.0,
                "error_message": None,
                "total_runs": 0,
                "success_count": 0,
                "fail_count": 0,
                "last_status_change": datetime.datetime.now().isoformat(),
                "max_retries": settings.max_retries,
                "retry_count": 0,
            }
        job_status_data[job_id].update(kwargs)
        job_status_data[job_id][
            "last_status_change"
        ] = datetime.datetime.now().isoformat()

        if "status" in kwargs and kwargs["status"] == JobStatus.SUCCESS:
            job_status_data[job_id]["retry_count"] = 0

        # Ensure timestamp fields are always strings (ISO format) for JSON serialization
        for key in ["last_run", "next_run"]:
            if isinstance(job_status_data[job_id].get(key), datetime.datetime):
                job_status_data[job_id][key] = job_status_data[job_id][key].isoformat()


def get_all_job_statuses() -> Dict[str, Dict[str, Any]]:
    """Returns a copy of all job statuses for reading."""
    with _job_status_lock:
        return job_status_data.copy()


def get_job_status(job_id: str) -> Optional[Dict[str, Any]]:
    """Returns the status of a single job."""
    with _job_status_lock:
        return job_status_data.get(job_id, {}).copy()


async def restart_job(job_id: str) -> RestartJobResponse:
    """
    Resets the status of a failed job so it can be run again.

    Checks that the job exists and is in a 'Failed' or 'Permanently Failed' state.
    If validation passes, it resets the retry count, fail count, and error message.

    Args:
        job_id: The unique ID of the job to restart.

    Returns:
        A tuple where the first element is a success boolean and the second is
        a descriptive message.
    """
    with _job_status_lock:
        job: Dict = job_status_data.get(job_id)

        if not job:
            return RestartJobResponse(
                success=False, message=f"Job '{job_id}' not found."
            )

        current_status: JobStatus = job.get("status", "")
        if not current_status.is_failed():
            return RestartJobResponse(
                success=False,
                message=f"Job '{job_id}' cannot be restarted from its current state: '{job.get('status', 'N/A')}'",
            )

        # Reset the relevant fields to allow the scheduler to run the job again
        job["status"] = JobStatus.SCHEDULED
        job["retry_count"] = 0
        job["fail_count"] = 0  # Resetting fail count on manual restart
        job["error_message"] = None
        job["last_status_change"] = datetime.datetime.now().isoformat()

        return RestartJobResponse(
            success=True,
            message=f"Job '{job_id}' has been reset and will run on the next schedule.",
        )


async def restart_all_failed_jobs() -> RestartJobBatchResponse:
    """
    Finds all jobs marked as 'Permanently Failed' and restarts them concurrently.

    This function is designed to be efficient by first identifying all target
    jobs under a lock, and then running the restart operations concurrently
    without holding the lock for the entire duration.

    Returns:
        A dictionary summarizing the outcome of the bulk restart operation.
    """
    with _job_status_lock:
        all_jobs = list(job_status_data.values())

    failed_job_ids = [
        job["id"]
        for job in all_jobs
        if job.get("status") == JobStatus.PERMANENTLY_FAILED
    ]

    if not failed_job_ids:
        return RestartJobBatchResponse(
            restarted_count=0,
            failed_count=0,
            message="No permanently failed jobs to restart",
        )

    # Execute all restart tasks concurrently
    restarted_count, failed_count = 0, 0
    for job_restart_result in asyncio.as_completed(map(restart_job, failed_job_ids)):
        result = await job_restart_result
        if isinstance(result, Exception) or not result.success:
            failed_count += 1
        else:
            restarted_count += 1

    message = f"Batch restart summary: {restarted_count} jobs restarted, {failed_count} failed."
    return RestartJobBatchResponse(
        restarted_count=restarted_count, failed_count=failed_count, message=message
    )
