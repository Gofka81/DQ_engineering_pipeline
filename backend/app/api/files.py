import uuid
from io import BytesIO
from uuid import UUID

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.core.config import settings
from backend.app.core.minio_service import MinioService, get_minio_service
from backend.app.core.redis_service import RedisService, get_redis_service
from backend.app.db.engine import get_db
from backend.app.db.models import File as FileModel, Run, RunStatus
from backend.app.dependencies import get_current_active_user
from backend.app.schemas.auth import UserOut
from backend.app.schemas.file import (
    DownloadResponse,
    FileOut,
    FileUploadResponse,
    RecommendationsOut,
    RecommendationsUpdate,
    RunOut,
    RunStatusResponse,
)

router = APIRouter(prefix="/files", tags=["files"])

MAX_FILE_SIZE = settings.MAX_FILE_SIZE_MB * 1024 * 1024  # Convert to bytes


@router.post("/upload", response_model=FileUploadResponse, status_code=status.HTTP_201_CREATED)
async def upload_file(
    file: UploadFile = File(..., description="CSV file to upload"),
    db: AsyncSession = Depends(get_db),
    minio: MinioService = Depends(get_minio_service),
    redis: RedisService = Depends(get_redis_service),
    current_user: UserOut = Depends(get_current_active_user),
):
    """
    Upload a CSV file for DQ analysis.
    Creates file record and initial run, stores file in MinIO raw bucket.
    """
    # Validate file extension
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only CSV files are allowed",
        )

    # Validate content type
    if file.content_type not in ("text/csv", "application/csv", "application/octet-stream"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid content type: {file.content_type}. Expected text/csv",
        )

    # Read file content
    content = await file.read()
    file_size = len(content)

    # Validate file size
    if file_size > MAX_FILE_SIZE:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"File too large. Maximum size is {settings.MAX_FILE_SIZE_MB}MB",
        )

    if file_size == 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="File is empty",
        )

    # Generate unique file ID
    file_id = str(uuid.uuid4())

    # Upload to MinIO
    minio_path = minio.upload_file(
        file_id=file_id,
        file_data=BytesIO(content),
        file_size=file_size,
        original_filename=file.filename,
        content_type="text/csv",
    )

    # Create file record
    db_file = FileModel(
        user_id=current_user.id,
        original_filename=file.filename,
        minio_raw_path=minio_path,
        file_size=file_size,
        content_type="text/csv",
    )
    db.add(db_file)
    await db.flush()  # Get the file ID

    # Create initial run
    db_run = Run(
        file_id=db_file.id,
        status=RunStatus.PENDING,
    )
    db.add(db_run)
    await db.commit()
    await db.refresh(db_file)
    await db.refresh(db_run)

    # Push job to Redis queue for DQ analysis
    redis.push_dq_job(
        run_id=db_run.id,
        file_id=db_file.id,
        minio_path=minio_path,
    )

    return FileUploadResponse(
        id=db_file.id,
        original_filename=db_file.original_filename,
        file_size=db_file.file_size,
        uploaded_at=db_file.uploaded_at,
        run_id=db_run.id,
        status=db_run.status,
    )


@router.get("", response_model=list[FileOut])
async def list_files(
    db: AsyncSession = Depends(get_db),
    current_user: UserOut = Depends(get_current_active_user),
):
    """List all files uploaded by the current user."""
    result = await db.execute(
        select(FileModel)
        .where(FileModel.user_id == current_user.id)
        .order_by(FileModel.uploaded_at.desc())
    )
    files = result.scalars().all()
    return files


@router.get("/{file_id}", response_model=FileOut)
async def get_file(
    file_id: UUID,
    db: AsyncSession = Depends(get_db),
    current_user: UserOut = Depends(get_current_active_user),
):
    """Get file details."""
    result = await db.execute(
        select(FileModel).where(
            FileModel.id == file_id,
            FileModel.user_id == current_user.id,
        )
    )
    file = result.scalar_one_or_none()

    if not file:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="File not found",
        )

    return file


@router.get("/{file_id}/status", response_model=RunStatusResponse)
async def get_file_status(
    file_id: UUID,
    db: AsyncSession = Depends(get_db),
    current_user: UserOut = Depends(get_current_active_user),
):
    """Get the latest run status for a file."""
    # Verify file ownership
    file_result = await db.execute(
        select(FileModel).where(
            FileModel.id == file_id,
            FileModel.user_id == current_user.id,
        )
    )
    if not file_result.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="File not found",
        )

    # Get latest run
    result = await db.execute(
        select(Run)
        .where(Run.file_id == file_id)
        .order_by(Run.created_at.desc())
        .limit(1)
    )
    run = result.scalar_one_or_none()

    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No runs found for this file",
        )

    return RunStatusResponse(
        run_id=run.id,
        file_id=run.file_id,
        status=run.status,
        dq_score_before=run.dq_score_before,
        dq_score_after=run.dq_score_after,
        error_message=run.error_message,
    )


@router.get("/{file_id}/recommendations", response_model=RecommendationsOut)
async def get_recommendations(
    file_id: UUID,
    db: AsyncSession = Depends(get_db),
    current_user: UserOut = Depends(get_current_active_user),
):
    """Get generated recommendations for a file."""
    # Verify file ownership
    file_result = await db.execute(
        select(FileModel).where(
            FileModel.id == file_id,
            FileModel.user_id == current_user.id,
        )
    )
    if not file_result.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="File not found",
        )

    # Get latest run
    result = await db.execute(
        select(Run)
        .where(Run.file_id == file_id)
        .order_by(Run.created_at.desc())
        .limit(1)
    )
    run = result.scalar_one_or_none()

    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No runs found for this file",
        )

    if run.status not in (RunStatus.AWAITING_REVIEW, RunStatus.TRANSFORMING, RunStatus.COMPLETED):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Recommendations not ready. Current status: {run.status}",
        )

    return RecommendationsOut(
        run_id=run.id,
        file_id=run.file_id,
        status=run.status,
        recommendations=run.recommendations_generated,
    )


@router.put("/{file_id}/recommendations", response_model=RunStatusResponse)
async def update_recommendations(
    file_id: UUID,
    body: RecommendationsUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: UserOut = Depends(get_current_active_user),
):
    """
    Submit edited recommendations and trigger transform flow.
    """
    # Verify file ownership
    file_result = await db.execute(
        select(FileModel).where(
            FileModel.id == file_id,
            FileModel.user_id == current_user.id,
        )
    )
    if not file_result.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="File not found",
        )

    # Get latest run
    result = await db.execute(
        select(Run)
        .where(Run.file_id == file_id)
        .order_by(Run.created_at.desc())
        .limit(1)
    )
    run = result.scalar_one_or_none()

    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No runs found for this file",
        )

    if run.status != RunStatus.AWAITING_REVIEW:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Cannot update recommendations. Current status: {run.status}",
        )

    # Update run with approved recommendations
    run.recommendations_approved = body.recommendations
    run.status = RunStatus.TRANSFORMING
    await db.commit()
    await db.refresh(run)

    # TODO: Trigger Prefect Transform flow here

    return RunStatusResponse(
        run_id=run.id,
        file_id=run.file_id,
        status=run.status,
        dq_score_before=run.dq_score_before,
        dq_score_after=run.dq_score_after,
        error_message=run.error_message,
    )


@router.get("/{file_id}/runs", response_model=list[RunOut])
async def list_runs(
    file_id: UUID,
    db: AsyncSession = Depends(get_db),
    current_user: UserOut = Depends(get_current_active_user),
):
    """List all runs for a file."""
    # Verify file ownership
    file_result = await db.execute(
        select(FileModel).where(
            FileModel.id == file_id,
            FileModel.user_id == current_user.id,
        )
    )
    if not file_result.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="File not found",
        )

    result = await db.execute(
        select(Run)
        .where(Run.file_id == file_id)
        .order_by(Run.created_at.desc())
    )
    runs = result.scalars().all()
    return runs


# Runs endpoints
runs_router = APIRouter(prefix="/runs", tags=["runs"])


@runs_router.get("/{run_id}", response_model=RunOut)
async def get_run(
    run_id: UUID,
    db: AsyncSession = Depends(get_db),
    current_user: UserOut = Depends(get_current_active_user),
):
    """Get run details."""
    result = await db.execute(
        select(Run)
        .join(FileModel)
        .where(
            Run.id == run_id,
            FileModel.user_id == current_user.id,
        )
    )
    run = result.scalar_one_or_none()

    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Run not found",
        )

    return run


@runs_router.get("/{run_id}/download", response_model=DownloadResponse)
async def download_run_result(
    run_id: UUID,
    db: AsyncSession = Depends(get_db),
    minio: MinioService = Depends(get_minio_service),
    current_user: UserOut = Depends(get_current_active_user),
):
    """Get presigned URL to download the cleaned file."""
    result = await db.execute(
        select(Run)
        .join(FileModel)
        .where(
            Run.id == run_id,
            FileModel.user_id == current_user.id,
        )
    )
    run = result.scalar_one_or_none()

    if not run:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Run not found",
        )

    if run.status != RunStatus.COMPLETED:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Run not completed. Current status: {run.status}",
        )

    if not run.minio_curated_path:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No output file available",
        )

    download_url = minio.get_presigned_url(
        object_name=run.minio_curated_path,
        bucket=minio.curated_bucket,
        expires_hours=1,
    )

    return DownloadResponse(
        run_id=run.id,
        download_url=download_url,
        expires_in_hours=1,
    )
