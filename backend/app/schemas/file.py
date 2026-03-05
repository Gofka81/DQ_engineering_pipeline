import re
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, Field, field_validator, model_validator

from backend.app.db.models.run import RunStatus


# ---------------------------------------------------------------------------
# Recommendations validation schema
# ---------------------------------------------------------------------------

class MissingValuesFill(BaseModel):
    strategy: Literal["median", "mean", "mode", "fill", "drop_row", "drop_column", "leave_null"]
    value: str | int | float | None = None  # required when strategy == "fill"

    @model_validator(mode="after")
    def value_required_for_fill(self) -> "MissingValuesFill":
        if self.strategy == "fill" and self.value is None:
            raise ValueError("'value' is required when strategy is 'fill'")
        return self


class ColumnConfig(BaseModel):
    type: Literal["int", "float", "string", "date", "bool"]
    nullable: bool = True
    missing_values: MissingValuesFill | None = None
    normalize: Literal["min_max", "z_score", False] = False
    warnings: list[str] = Field(default_factory=list)
    note: str | None = None
    rename_to: str | None = None
    sentinel_values: list[float] | None = None

    @field_validator("rename_to")
    @classmethod
    def rename_to_must_be_identifier(cls, v: str | None) -> str | None:
        if v is not None and not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", v):
            raise ValueError(
                f"rename_to='{v}' is not a valid identifier — use snake_case "
                f"(letters, digits, underscores; must start with a letter or underscore)"
            )
        return v


class DuplicatesConfig(BaseModel):
    # "ignore" is the default so an empty duplicates dict {} still validates cleanly.
    strategy: Literal["drop", "ignore"] = "ignore"
    subset: list[str] = Field(default_factory=list)
    keep: Literal["first", "last"] = "first"


class OutliersConfig(BaseModel):
    strategy: Literal["keep", "winsorise", "remove", "cap"] = "keep"
    method: Literal["iqr"] = "iqr"
    lower: float | None = None
    upper: float | None = None


class RecommendationsSchema(BaseModel):
    """Validated structure of the recommendations JSON produced by DQ flow and edited by users."""

    columns: dict[str, ColumnConfig] = Field(default_factory=dict)
    duplicates: DuplicatesConfig | None = None
    outliers: dict[str, OutliersConfig] = Field(default_factory=dict)
    # custom_transforms: pass-through — not strictly validated here
    custom_transforms: list[dict[str, Any]] = Field(default_factory=list)
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
