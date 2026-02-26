from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, Field, model_validator

from backend.app.db.models.run import RunStatus


# ---------------------------------------------------------------------------
# Recommendations validation schema
# ---------------------------------------------------------------------------

class ColumnSchema(BaseModel):
    type: Literal["int", "float", "string", "date", "bool"]
    nullable: bool = True


class MissingValueStrategy(BaseModel):
    strategy: Literal["median", "mean", "mode", "fill", "drop_row"]
    value: str | int | float | None = None  # required when strategy == "fill"

    @model_validator(mode="after")
    def value_required_for_fill(self) -> "MissingValueStrategy":
        if self.strategy == "fill" and self.value is None:
            raise ValueError("'value' is required when strategy is 'fill'")
        return self


class DuplicatesConfig(BaseModel):
    strategy: Literal["drop", "keep_first", "keep_last"]
    subset: list[str] = Field(default_factory=list)
    keep: Literal["first", "last"] = "first"


class NormalizationConfig(BaseModel):
    columns: list[str] = Field(default_factory=list)


class CustomTransform(BaseModel):
    description: str
    type: Literal["computed_column", "rename_column", "filter_rows", "cast_type"]
    output_column: str
    logic: str


class RecommendationsSchema(BaseModel):
    """Validated structure of the recommendations JSON produced by DQ flow and edited by users."""

    schema_: dict[str, ColumnSchema] = Field(default_factory=dict, alias="schema")
    missing_values: dict[str, MissingValueStrategy] = Field(default_factory=dict)
    duplicates: DuplicatesConfig | None = None
    normalization: NormalizationConfig | None = None
    custom_transforms: list[CustomTransform] = Field(default_factory=list)
    # _metadata is pass-through
    metadata_: dict[str, Any] = Field(default_factory=dict, alias="_metadata")

    model_config = {"populate_by_name": True}

    def to_flow_dict(self) -> dict[str, Any]:
        """Serialize back to the dict shape that apply_recommendations() expects."""
        return self.model_dump(by_alias=True, exclude_none=True)


# ---------------------------------------------------------------------------
# API schemas
# ---------------------------------------------------------------------------

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
    dq_scores_before: dict[str, Any] | None = None
    dq_scores_after: dict[str, Any] | None = None
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
    dq_scores_before: dict[str, Any] | None = None
    dq_scores_after: dict[str, Any] | None = None
    error_message: str | None = None


class RecommendationsOut(BaseModel):
    run_id: UUID
    file_id: UUID
    status: RunStatus
    recommendations: dict[str, Any] | None = None


class RecommendationsUpdate(BaseModel):
    recommendations: RecommendationsSchema = Field(
        ...,
        description="Edited recommendations — validated against the DQ schema",
    )


class DownloadResponse(BaseModel):
    run_id: UUID
    download_url: str
    expires_in_hours: int = 1
