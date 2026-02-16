from sqlalchemy import Column, String, Float, DateTime, ForeignKey, Enum
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
import enum
import uuid


class RunStatus(str, enum.Enum):
    PENDING = "PENDING"
    ANALYZING = "ANALYZING"
    AWAITING_REVIEW = "AWAITING_REVIEW"
    TRANSFORMING = "TRANSFORMING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


from backend.app.db.base import Base


class Run(Base):
    __tablename__ = "runs"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    file_id = Column(UUID(as_uuid=True), ForeignKey("files.id"), nullable=False, index=True)
    status = Column(
        Enum(RunStatus, name="run_status"),
        default=RunStatus.PENDING,
        nullable=False,
        index=True,
    )

    # DQ scores
    dq_score_before = Column(Float, nullable=True)
    dq_score_after = Column(Float, nullable=True)

    # Recommendations as JSONB
    recommendations_generated = Column(JSONB, nullable=True)
    recommendations_approved = Column(JSONB, nullable=True)

    # Output file path
    minio_curated_path = Column(String(512), nullable=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    completed_at = Column(DateTime(timezone=True), nullable=True)

    # Error tracking
    error_message = Column(String(2000), nullable=True)

    # Relationship
    file = relationship("File", back_populates="runs")
