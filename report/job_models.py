# Define a Pydantic model for the request body to ensure data is valid
from typing import List, Optional

from pydantic import BaseModel, Field


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
