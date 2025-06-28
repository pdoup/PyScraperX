import datetime
from typing import Annotated, Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_serializer

from config import settings
from models import JobStatus

JobStateListType = Annotated[
    List[Dict[str, Any]],
    Field(
        ...,
        title="List of Job States",
        description="A non-empty list containing the current state of each job.",
        frozen=True,
        min_length=1,
        examples=[
            [
                {
                    "id": "job_001",
                    "name": "DataCollector",
                    "status": "initialized",
                    "last_run": None,
                    "next_run": None,
                    "duration_ms": 0.0,
                    "error_message": None,
                    "total_runs": 0,
                    "success_count": 0,
                    "fail_count": 0,
                    "last_status_change": "2025-06-28T10:00:00",
                    "max_retries": 3,
                    "retry_count": 0,
                }
            ]
        ],
    ),
]


class RestartJobRequest(BaseModel):
    jobId: str = Field(..., description="The unique ID of the job to restart")


class RestartJobResponse(BaseModel):
    success: bool = Field(..., description="Restart status for a job", frozen=True)
    message: str = Field(
        ..., description="Status message for job scheduled for restart", frozen=True
    )


class RestartJobBatchResponse(BaseModel):
    restarted_count: int = Field(
        ..., ge=0, description="Number of jobs scheduled for restart"
    )
    failed_count: int = Field(..., ge=0, description="Number of jobs failed to restart")
    message: str = Field(
        ..., description="Status message for jobs scheduled for restart", frozen=True
    )
    results: Optional[List[RestartJobResponse]] = Field(
        default_factory=list,
        min_items=1,
        description="List of individual job restart responses. Can contain one or more results.",
        frozen=True,
    )


class JobState(BaseModel):
    """Represents the state of a single job."""

    id: str
    name: str
    status: JobStatus = JobStatus.INITIALIZED
    last_run: Optional[datetime.datetime] = None
    next_run: Optional[datetime.datetime] = None
    duration_ms: float = 0.0
    error_message: Optional[str] = None
    total_runs: int = 0
    success_count: int = 0
    fail_count: int = 0
    last_status_change: datetime.datetime = Field(
        default_factory=datetime.datetime.now, init=False
    )
    max_retries: int = Field(default_factory=lambda: settings.max_retries, init=False)
    retry_count: int = 0

    model_config = {
        "extra": "forbid",
        "frozen": False,
    }

    @field_serializer(
        "last_run",
        "next_run",
        "last_status_change",
        when_used="json",
        check_fields=True,
    )
    def serialize_dt(self, v: Optional[datetime.datetime]) -> Optional[str]:
        return v.isoformat() if v else None


class JobStateUpdate(BaseModel):
    jobData: JobStateListType
