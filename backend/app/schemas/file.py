from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field

from backend.app.db.models.run import RunStatus


class FileUploadResponse(BaseModel):
    id: UUID
    original_filename: str
    file_size: int
    uploaded_at: datetime
    run_id: UUID
    status: RunStatus

    model_config = {"from_attributes": True}


class FileOut(BaseModel):
    id: UUID
    original_filename: str
    file_size: int
    uploaded_at: datetime

    model_config = {"from_attributes": True}


class RunOut(BaseModel):
    id: UUID
    file_id: UUID
    status: RunStatus
    dq_score_before: float | None = None
    dq_score_after: float | None = None
    recommendations_generated: dict[str, Any] | None = None
    recommendations_approved: dict[str, Any] | None = None
    created_at: datetime
    completed_at: datetime | None = None
    error_message: str | None = None

    model_config = {"from_attributes": True}


class RunStatusResponse(BaseModel):
    run_id: UUID
    file_id: UUID
    status: RunStatus
    dq_score_before: float | None = None
    dq_score_after: float | None = None
    error_message: str | None = None


class RecommendationsOut(BaseModel):
    run_id: UUID
    file_id: UUID
    status: RunStatus
    recommendations: dict[str, Any] | None = None


class RecommendationsUpdate(BaseModel):
    recommendations: dict[str, Any] = Field(
        ...,
        description="Edited recommendations JSON",
        examples=[
            {
                "schema": {"emp_id": {"type": "int", "nullable": False}},
                "missing_values": {"salary": {"strategy": "median"}},
                "duplicates": {"strategy": "drop", "subset": ["emp_id"]},
            }
        ],
    )


class DownloadResponse(BaseModel):
    run_id: UUID
    download_url: str
    expires_in_hours: int = 1
