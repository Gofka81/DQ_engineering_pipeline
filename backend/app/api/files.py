import asyncio
import json
import uuid
from io import BytesIO
from typing import Optional
from uuid import UUID

import redis.asyncio as aioredis
from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile, status
from fastapi.responses import StreamingResponse
from jose import JWTError, jwt
from sqlalchemy import select, true
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.core.config import settings
from backend.app.core.minio_service import MinioService, get_minio_service
from backend.app.core.redis_service import RedisService, get_redis_service
from backend.app.db.engine import get_db
from backend.app.db.models import File as FileModel, Run, RunStatus
from backend.app.dependencies import get_current_active_user, get_user_by_username
from backend.app.schemas.auth import UserOut
from backend.app.schemas.file import (
    DownloadResponse,
    FileOut,
    FileUploadResponse,
    FileWithLatestRunOut,
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
    try:
        redis.push_job(
            job_type="dq_analysis",
            run_id=db_run.id,
            file_id=db_file.id,
            minio_path=minio_path,
        )
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Analysis job could not be queued. Please try again.",
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
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=200),
):
    """List all files uploaded by the current user."""
    result = await db.execute(
        select(FileModel)
        .where(FileModel.user_id == current_user.id)
        .order_by(FileModel.uploaded_at.desc())
        .offset(skip)
        .limit(limit)
    )
    files = result.scalars().all()
    return files


@router.get("/with-latest-run", response_model=list[FileWithLatestRunOut])
async def list_files_with_latest_run(
    db: AsyncSession = Depends(get_db),
    current_user: UserOut = Depends(get_current_active_user),
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=200),
):
    runs_lateral = (
        select(
            Run.id.label("run_id"),
            Run.status.label("run_status"),
            Run.created_at.label("run_created_at"),
        )
        .where(Run.file_id == FileModel.id)
        .order_by(Run.created_at.desc())
        .limit(1)
        .lateral("latest_run")
    )
    stmt = (
        select(
            FileModel.id,
            FileModel.original_filename,
            FileModel.file_size,
            FileModel.uploaded_at,
            runs_lateral.c.run_id,
            runs_lateral.c.run_status,
            runs_lateral.c.run_created_at,
        )
        .where(FileModel.user_id == current_user.id)
        .outerjoin(runs_lateral, true())
        .order_by(FileModel.uploaded_at.desc())
        .offset(skip)
        .limit(limit)
    )
    result = await db.execute(stmt)
    return [FileWithLatestRunOut(**row) for row in result.mappings().all()]


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
        dq_scores_before=run.dq_scores_before,
        dq_scores_after=run.dq_scores_after,
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
    redis: RedisService = Depends(get_redis_service),
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

    # Save approved recommendations and transition to TRANSFORMING
    run.recommendations_approved = body.recommendations.to_flow_dict()
    run.status = RunStatus.TRANSFORMING
    await db.commit()
    await db.refresh(run)

    # Fetch the raw file path so the transform flow can load the original CSV
    file_result = await db.execute(
        select(FileModel).where(FileModel.id == file_id)
    )
    file_record = file_result.scalar_one()

    # Trigger transform flow via Redis
    try:
        redis.push_job(
            job_type="transform",
            run_id=run.id,
            file_id=file_id,
            minio_path=file_record.minio_raw_path,
        )
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Transform job could not be queued. Please try again.",
        )

    return RunStatusResponse(
        run_id=run.id,
        file_id=run.file_id,
        status=run.status,
        dq_scores_before=run.dq_scores_before,
        dq_scores_after=run.dq_scores_after,
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


@router.get("/{file_id}/events")
async def stream_run_events(
    file_id: UUID,
    token: str = Query(..., description="JWT token (EventSource cannot set headers)"),
    db: AsyncSession = Depends(get_db),
):
    """
    SSE stream for live run status updates.
    Accepts JWT as ?token= query param because EventSource doesn't support headers.
    Sends current state immediately on connect, then streams Redis Pub/Sub messages.
    """
    # --- Auth via query param ---
    credentials_exception = HTTPException(status_code=401, detail="Invalid token")
    try:
        payload = jwt.decode(
            token,
            settings.SECRET_KEY.get_secret_value(),
            algorithms=[settings.ALGORITHM],
        )
        username: Optional[str] = payload.get("sub")
        if not username:
            raise credentials_exception
    except JWTError:
        raise credentials_exception

    user = await get_user_by_username(db, username)
    if not user or user.disabled:
        raise credentials_exception

    # --- Verify file ownership ---
    file_result = await db.execute(
        select(FileModel).where(FileModel.id == file_id, FileModel.user_id == user.id)
    )
    if not file_result.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="File not found")

    # --- Get latest run ---
    run_result = await db.execute(
        select(Run).where(Run.file_id == file_id).order_by(Run.created_at.desc()).limit(1)
    )
    run = run_result.scalar_one_or_none()
    if not run:
        raise HTTPException(status_code=404, detail="No runs found for this file")

    run_id = str(run.id)
    initial_payload = {
        "status": run.status.value if hasattr(run.status, "value") else str(run.status),
        "dq_scores_before": run.dq_scores_before,
        "dq_scores_after": run.dq_scores_after,
        "error_message": run.error_message,
    }

    async def event_generator():
        # Send current state immediately (cold-start / reconnect recovery)
        yield f"data: {json.dumps(initial_payload)}\n\n"

        # If already in a terminal state, no need to subscribe
        terminal = {"COMPLETED", "FAILED"}
        current_status = initial_payload["status"]
        if current_status in terminal:
            return

        # Subscribe to Redis Pub/Sub for live updates
        r = aioredis.Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT)
        try:
            async with r.pubsub() as ps:
                await ps.subscribe(f"run:{run_id}:status")
                async for msg in ps.listen():
                    if msg["type"] == "message":
                        data = msg["data"]
                        if isinstance(data, bytes):
                            data = data.decode()
                        yield f"data: {data}\n\n"
                        # Close stream once terminal state reached
                        try:
                            parsed = json.loads(data)
                            if parsed.get("status") in terminal:
                                return
                        except Exception:
                            pass
        finally:
            await r.aclose()

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


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
