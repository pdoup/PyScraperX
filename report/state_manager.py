import asyncio
import datetime
from typing import Any, Dict, List, Optional

from fastapi.encoders import jsonable_encoder

from config import settings
from models import JobStatus
from report.job_models import JobState, RestartJobBatchResponse, RestartJobResponse


class StateManager:
    """
    An asynchronous, in-memory store for managing job states.

    This class is designed to be a singleton or a shared instance in an
    asyncio application.
    """

    def __init__(self):
        self._job_status_data: Dict[str, JobState] = {}
        self._lock = asyncio.Lock()

    async def register_job(self, job_id: str, name: Optional[str] = None) -> JobState:
        """Registers a new job if it doesn't exist. Idempotent."""
        async with self._lock:
            if job_id not in self._job_status_data:
                self._job_status_data[job_id] = JobState(id=job_id, name=name or job_id)
            return self._job_status_data[job_id]

    async def update_job_status(self, job_id: str, **kwargs: Any) -> Optional[JobState]:
        """Atomically updates the status of a specific job."""
        async with self._lock:
            job = self._job_status_data.get(job_id)
            if not job:
                return None

            update_data = kwargs.copy()
            update_data["last_status_change"] = datetime.datetime.now()

            if "status" in update_data and update_data["status"] == JobStatus.SUCCESS:
                update_data["retry_count"] = 0

            updated_job = job.model_copy(update=update_data)
            self._job_status_data[job_id] = updated_job
            return updated_job

    async def get_job_status(self, job_id: str) -> Optional[JobState]:
        """Returns the state of a single job."""
        async with self._lock:
            job = self._job_status_data.get(job_id)
            return job.model_copy() if job else None

    async def get_all_job_statuses(
        self, json_encoded: Optional[bool] = False
    ) -> List[JobState]:
        """Returns a copy of all job states."""
        async with self._lock:
            return (
                [job.model_copy() for job in self._job_status_data.values()]
                if not json_encoded
                else jsonable_encoder(
                    [job.model_copy() for job in self._job_status_data.values()]
                )
            )

    async def restart_job(self, job_id: str) -> RestartJobResponse:
        """Resets the status of a failed job so it can be run again."""
        async with self._lock:
            job = self._job_status_data.get(job_id)

            if not job:
                return RestartJobResponse(
                    success=False, message=f"Job '{job_id}' not found."
                )

            if not job.status.is_failed():
                return RestartJobResponse(
                    success=False,
                    message=f"Job '{job_id}' cannot be restarted from its current state: '{job.status}'",
                )

            updated_job = job.model_copy(
                update={
                    "status": JobStatus.SCHEDULED,
                    "retry_count": 0,
                    "error_message": None,
                    "last_status_change": datetime.datetime.now(),
                }
            )
            self._job_status_data[job_id] = updated_job

            return RestartJobResponse(
                success=True,
                message=f"Job '{job_id}' has been reset and will run on the next schedule.",
            )

    async def restart_all_failed_jobs(self) -> RestartJobBatchResponse:
        """Finds all permanently failed jobs and restarts them."""
        async with self._lock:
            all_jobs = list(self._job_status_data.values())

        failed_job_ids = [
            job.id for job in all_jobs if job.status == JobStatus.PERMANENTLY_FAILED
        ]

        if not failed_job_ids:
            return RestartJobBatchResponse(
                restarted_count=0,
                failed_count=0,
                message="No permanently failed jobs to restart",
            )

        restarted_count, failed_count = 0, 0
        tasks = [self.restart_job(job_id) for job_id in failed_job_ids]
        for restart_job in asyncio.as_completed(
            tasks, timeout=settings.restart_all_timeout_seconds
        ):
            try:
                result = await restart_job
                if result.success:
                    restarted_count += 1
                else:
                    failed_count += 1
            except (TimeoutError, Exception) as e:
                failed_count += 1

        return RestartJobBatchResponse(
            restarted_count=restarted_count,
            failed_count=failed_count,
            message=f"Batch restart summary: {restarted_count} jobs restarted, {failed_count} failed.",
        )


# Shared instance
state_manager = StateManager()
