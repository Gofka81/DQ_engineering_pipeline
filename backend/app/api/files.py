import asyncio
import base64
import json
import uuid
from io import BytesIO
from typing import Literal, Optional
from uuid import UUID

import pandas as pd

import redis.asyncio as aioredis
from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile, status
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
    DataPreviewResponse,
    DownloadResponse,
    DropImpactBreakdown,
    DropImpactRequest,
    DropImpactResponse,
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
    has_header: bool = Form(True, description="Whether the CSV has a header row"),
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

    # Generate one UUID used for both the DB record and the MinIO path prefix
    file_id = uuid.uuid4()

    # Upload to MinIO
    minio_path = minio.upload_file(
        file_id=str(file_id),
        file_data=BytesIO(content),
        file_size=file_size,
        original_filename=file.filename,
        content_type="text/csv",
    )

    # Create file record. Pass id explicitly so DB and MinIO share the same UUID.
    db_file = FileModel(
        id=file_id,
        user_id=current_user.id,
        original_filename=file.filename,
        minio_raw_path=minio_path,
        file_size=file_size,
        content_type="text/csv",
        has_header=has_header,
    )
    db.add(db_file)
    await db.flush()

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
            has_header=has_header,
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
        has_header=db_file.has_header,
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
            has_header=file_record.has_header,
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


@runs_router.post("/{run_id}/restart", response_model=RunStatusResponse)
async def restart_run(
    run_id: UUID,
    db: AsyncSession = Depends(get_db),
    minio: MinioService = Depends(get_minio_service),
    current_user: UserOut = Depends(get_current_active_user),
):
    """
    Clone a COMPLETED or FAILED run into a new AWAITING_REVIEW run.
    The new run inherits the file, DQ scores before, and recommendations so the
    user can adjust settings and re-apply the transform without re-uploading.
    For FAILED runs, only allowed when recommendations_generated exists
    (i.e. analysis completed before the failure).
    """
    result = await db.execute(
        select(Run)
        .join(FileModel)
        .where(Run.id == run_id, FileModel.user_id == current_user.id)
    )
    source = result.scalar_one_or_none()
    if not source:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")

    if source.status not in (RunStatus.COMPLETED, RunStatus.FAILED):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Can only restart a COMPLETED or FAILED run. Current status: {source.status}",
        )

    if not source.recommendations_generated:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot restart: analysis did not complete — no recommendations to copy",
        )

    # Start from what the user previously approved so they don't redo all edits.
    # Fall back to auto-generated if the run never reached the transform step.
    if source.recommendations_approved:
        new_recs = dict(source.recommendations_approved)
        # Restore _eda from generated. Required for the EDA dashboard tab.
        generated = dict(source.recommendations_generated)
        if "_eda" not in new_recs and "_eda" in generated:
            new_recs["_eda"] = generated["_eda"]
    else:
        new_recs = dict(source.recommendations_generated)

    new_run = Run(
        file_id=source.file_id,
        status=RunStatus.AWAITING_REVIEW,
        recommendations_generated=new_recs,
        dq_scores_before=source.dq_scores_before,
    )
    db.add(new_run)
    await db.commit()
    await db.refresh(new_run)

    # Copy drop masks to the new run path. Best-effort, 404 is handled gracefully.
    try:
        from minio.commonconfig import CopySource
        minio.client.copy_object(
            minio.raw_bucket,
            f"{source.file_id}/dropmasks_{new_run.id}.json",
            CopySource(minio.raw_bucket, f"{source.file_id}/dropmasks_{run_id}.json"),
        )
    except Exception:
        pass

    return RunStatusResponse(
        run_id=new_run.id,
        file_id=new_run.file_id,
        status=new_run.status,
        dq_scores_before=new_run.dq_scores_before,
        dq_scores_after=None,
        error_message=None,
    )


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


# ---------------------------------------------------------------------------
# Bitset helpers (pure Python, no numpy in backend)
# ---------------------------------------------------------------------------

def _unpack_b64(b64: str, n_rows: int) -> bytearray:
    """Decode a base64 packed bitset and mask off pad bits in the last byte."""
    raw = base64.b64decode(b64)
    result = bytearray(raw)
    remainder = n_rows % 8
    if remainder:
        result[-1] &= 0xFF ^ ((1 << (8 - remainder)) - 1)
    return result


def _popcount_b64(b64: str, n_rows: int) -> int:
    return bin(int.from_bytes(_unpack_b64(b64, n_rows), "big")).count("1")


def _union_popcount(b64_list: list[str], n_rows: int) -> int:
    if not b64_list:
        return 0
    arrays = [_unpack_b64(b, n_rows) for b in b64_list]
    union = bytearray(arrays[0])
    for arr in arrays[1:]:
        for i in range(len(union)):
            union[i] |= arr[i]
    return bin(int.from_bytes(union, "big")).count("1")


@runs_router.post("/{run_id}/drop-impact", response_model=DropImpactResponse)
async def compute_drop_impact(
    run_id: UUID,
    body: DropImpactRequest,
    db: AsyncSession = Depends(get_db),
    minio: MinioService = Depends(get_minio_service),
    current_user: UserOut = Depends(get_current_active_user),
):
    """Return row-level drop counts for the given strategy selections, using precomputed bitsets."""
    result = await db.execute(
        select(Run)
        .join(FileModel)
        .where(Run.id == run_id, FileModel.user_id == current_user.id)
    )
    run = result.scalar_one_or_none()
    if not run:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")

    path = f"{run.file_id}/dropmasks_{run_id}.json"
    try:
        bio = minio.download_file(path)
        data = json.loads(bio.read())
    except HTTPException:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Drop masks not available for this run")

    n_rows: int = data["n_rows"]
    ops: dict[str, str] = data["ops"]
    op_meta: dict[str, dict] = data["op_meta"]

    active_ops: list[str] = []
    null_count = 0
    outlier_count = 0
    dup_count = 0

    for op_name, meta in op_meta.items():
        if meta["type"] == "null":
            col = meta["col"]
            if body.null_strategies.get(col) == "drop_row":
                active_ops.append(op_name)
                null_count += _popcount_b64(ops[op_name], n_rows)
        elif meta["type"] == "outlier":
            col = meta["col"]
            if body.outlier_strategies.get(col) == "remove":
                active_ops.append(op_name)
                outlier_count += _popcount_b64(ops[op_name], n_rows)
        elif meta["type"] == "duplicate":
            if body.duplicates_strategy == "drop":
                active_ops.append(op_name)
                dup_count = _popcount_b64(ops[op_name], n_rows)

    rows_dropped = _union_popcount([ops[k] for k in active_ops], n_rows)
    individual_sum = null_count + outlier_count + dup_count
    overlap_saved = max(0, individual_sum - rows_dropped)

    return DropImpactResponse(
        rows_before=n_rows,
        rows_dropped=rows_dropped,
        rows_after=n_rows - rows_dropped,
        breakdown=DropImpactBreakdown(
            null_drops=null_count,
            outlier_drops=outlier_count,
            duplicate_drops=dup_count,
            overlap_saved=overlap_saved,
        ),
    )


@runs_router.get("/{run_id}/preview", response_model=DataPreviewResponse)
async def get_run_preview(
    run_id: UUID,
    stage: Literal["raw", "cleaned"] = Query(default="raw"),
    db: AsyncSession = Depends(get_db),
    minio: MinioService = Depends(get_minio_service),
    current_user: UserOut = Depends(get_current_active_user),
):
    """Return first 20 rows of the raw upload (stage=raw) or the cleaned output (stage=cleaned)."""
    result = await db.execute(
        select(Run)
        .join(FileModel)
        .where(Run.id == run_id, FileModel.user_id == current_user.id)
    )
    run = result.scalar_one_or_none()
    if not run:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")

    if stage == "cleaned":
        if run.status != RunStatus.COMPLETED or not run.minio_curated_path:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Cleaned file not available yet",
            )
        bucket = minio.curated_bucket
        path = run.minio_curated_path
    else:
        file_result = await db.execute(select(FileModel).where(FileModel.id == run.file_id))
        file = file_result.scalar_one()
        bucket = minio.raw_bucket
        path = file.minio_raw_path

    try:
        response = minio.client.get_object(bucket_name=bucket, object_name=path)
        if stage == "raw" and not file.has_header:
            df = pd.read_csv(response, nrows=20, header=None)
            df.columns = [f"col_{i}" for i in range(len(df.columns))]
        else:
            df = pd.read_csv(response, nrows=20)
        response.close()
        response.release_conn()
    except Exception:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Preview unavailable")

    df = df.where(pd.notna(df), None)
    return DataPreviewResponse(columns=df.columns.tolist(), rows=df.values.tolist())
