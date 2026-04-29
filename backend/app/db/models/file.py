from sqlalchemy import Boolean, Column, String, BigInteger, DateTime, Integer, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
import uuid

from backend.app.db.base import Base


class File(Base):
    __tablename__ = "files"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    original_filename = Column(String(255), nullable=False)
    minio_raw_path = Column(String(512), nullable=False)
    file_size = Column(BigInteger, nullable=False)
    content_type = Column(String(100), default="text/csv")
    has_header = Column(Boolean, nullable=False, server_default="TRUE")
    uploaded_at = Column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    user = relationship("User", back_populates="files")
    runs = relationship("Run", back_populates="file", cascade="all, delete-orphan")
