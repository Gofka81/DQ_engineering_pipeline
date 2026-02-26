"""
Transform Prefect flow — applies approved recommendations to the raw CSV
and saves the cleaned file to the MinIO curated bucket.

Flow order:
    1. update_run_status → TRANSFORMING
    2. load_dataframe_from_minio — load original raw CSV
    3. load_approved_recommendations — fetch from DB
    4. apply_transform — pure pandas (dq_logic.apply_recommendations)
    5. upload_cleaned_file — write CSV to curated bucket
    6. profile + score — calculate dq_scores_after
    7. save_transform_results_to_db — curated path + scores + COMPLETED
"""
import io
import json
from typing import Any



import pandas as pd
from minio.error import S3Error
from prefect import flow, task
from prefect.logging import get_run_logger

from clients import get_minio_client, get_minio_raw_bucket, get_minio_curated_bucket, get_pg_conn
from flows.dq_logic import apply_recommendations, parse_csv, profile_dataframe, score_profile_detailed


@task(retries=3, retry_delay_seconds=5)
async def update_run_status(run_id: str, status: str, error_message: str = None):
    """Update run status in PostgreSQL."""
    logger = get_run_logger()
    logger.info(f"Updating run {run_id} to status: {status}")

    conn = await get_pg_conn()
    try:
        await conn.execute(
            """
            UPDATE runs
            SET status        = $1::run_status,
                error_message = $2,
                completed_at  = CASE
                                    WHEN $1 IN ('COMPLETED', 'FAILED') THEN NOW()
                                    ELSE completed_at
                                END
            WHERE id = $3::uuid
            """,
            status,
            error_message,
            run_id,
        )
    finally:
        await conn.close()

    logger.info(f"Run {run_id} status updated to {status}")


@task(retries=3, retry_delay_seconds=5)
def load_dataframe_from_minio(minio_path: str) -> pd.DataFrame:
    """Load CSV from MinIO raw bucket into a DataFrame."""
    logger = get_run_logger()
    logger.info(f"Loading dataframe from MinIO: {minio_path}")

    client = get_minio_client()
    bucket = get_minio_raw_bucket()

    response = None
    try:
        response = client.get_object(bucket_name=bucket, object_name=minio_path)
        df, malformed_rows = parse_csv(response)
    except S3Error as e:
        raise RuntimeError(f"Failed to load {minio_path} from MinIO bucket '{bucket}': {e}")
    finally:
        if response:
            response.close()
            response.release_conn()

    if malformed_rows > 0:
        logger.warning(f"Skipped {malformed_rows} malformed rows")

    logger.info(f"Loaded dataframe: {len(df)} rows × {len(df.columns)} columns")
    return df


@task(retries=3, retry_delay_seconds=5)
async def load_approved_recommendations(run_id: str) -> dict[str, Any]:
    """Fetch the user-approved recommendations JSON from PostgreSQL."""
    logger = get_run_logger()
    logger.info(f"Loading approved recommendations for run {run_id}")

    conn = await get_pg_conn()
    try:
        row = await conn.fetchrow(
            "SELECT recommendations_approved FROM runs WHERE id = $1::uuid",
            run_id,
        )
        if row is None:
            raise RuntimeError(f"Run {run_id} not found in database")
        if row["recommendations_approved"] is None:
            raise RuntimeError(f"Run {run_id} has no approved recommendations")
        # asyncpg returns JSONB as a raw JSON string — decode it (Decision #23)
        raw = row["recommendations_approved"]
        recommendations = json.loads(raw) if isinstance(raw, str) else dict(raw)
    finally:
        await conn.close()

    logger.info("Approved recommendations loaded")
    return recommendations


@task
def apply_transform(df: pd.DataFrame, recommendations: dict[str, Any]) -> pd.DataFrame:
    """Apply approved recommendations to the DataFrame."""
    logger = get_run_logger()
    rows_before = len(df)
    cleaned_df = apply_recommendations(df, recommendations)
    logger.info(
        f"Transform applied: {rows_before} → {len(cleaned_df)} rows "
        f"({rows_before - len(cleaned_df)} dropped)"
    )
    return cleaned_df


@task(retries=3, retry_delay_seconds=5)
def upload_cleaned_file(df: pd.DataFrame, file_id: str, run_id: str) -> str:
    """
    Upload cleaned CSV to MinIO curated bucket.

    Object path: curated/{file_id}/{run_id}/cleaned.csv
    Returns the object path.
    """
    logger = get_run_logger()

    buf = io.BytesIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    file_size = buf.getbuffer().nbytes

    object_path = f"{file_id}/{run_id}/cleaned.csv"

    client = get_minio_client()
    bucket = get_minio_curated_bucket()

    try:
        client.put_object(
            bucket_name=bucket,
            object_name=object_path,
            data=buf,
            length=file_size,
            content_type="text/csv",
        )
    except S3Error as e:
        raise RuntimeError(f"Failed to upload cleaned file to MinIO: {e}")

    logger.info(f"Cleaned file uploaded: {bucket}/{object_path} ({file_size} bytes)")
    return object_path


@task
def score_cleaned_data(df: pd.DataFrame) -> dict[str, float]:
    """Profile cleaned DataFrame and return all 5 DQ scores."""
    logger = get_run_logger()
    profile = profile_dataframe(df, malformed_rows=0)
    scores  = score_profile_detailed(profile)
    logger.info(
        f"DQ scores after transform: overall={scores['overall']:.2f} "
        f"(C={scores['completeness']:.1f} U={scores['uniqueness']:.1f} "
        f"V={scores['validity']:.1f} Co={scores['consistency']:.1f})"
    )
    return scores


@task(retries=3, retry_delay_seconds=5)
async def save_transform_results_to_db(
    run_id: str,
    dq_scores_after: dict[str, float],
    minio_curated_path: str,
):
    """Save curated path, after-scores, and mark run COMPLETED."""
    logger = get_run_logger()
    logger.info(f"Saving transform results for run {run_id}")

    conn = await get_pg_conn()
    try:
        await conn.execute(
            """
            UPDATE runs
            SET dq_scores_after    = $1::jsonb,
                minio_curated_path = $2,
                status             = 'COMPLETED'::run_status,
                completed_at       = NOW()
            WHERE id = $3::uuid
            """,
            json.dumps(dq_scores_after),
            minio_curated_path,
            run_id,
        )
    finally:
        await conn.close()

    logger.info(f"Run {run_id} marked COMPLETED")


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(name="Transform", log_prints=True)
async def transform_flow(run_id: str, file_id: str, minio_path: str):
    """
    Transform flow — applies user-approved recommendations and saves cleaned file.

    Args:
        run_id:     UUID of the run record
        file_id:    UUID of the file record
        minio_path: Path to the raw file in MinIO (same path DQ flow used)
    """
    logger = get_run_logger()
    logger.info(f"Starting Transform for run_id={run_id}, file_id={file_id}")

    try:
        await update_run_status(run_id, "TRANSFORMING")

        df              = load_dataframe_from_minio(minio_path)
        recommendations = await load_approved_recommendations(run_id)
        cleaned_df      = apply_transform(df, recommendations)
        curated_path    = upload_cleaned_file(cleaned_df, file_id, run_id)
        dq_scores_after = score_cleaned_data(cleaned_df)

        await save_transform_results_to_db(run_id, dq_scores_after, curated_path)

        logger.info(
            f"Transform completed. "
            f"Score: {dq_scores_after['overall']:.2f} "
            f"(saved to {curated_path})"
        )

    except Exception as e:
        logger.error(f"Transform failed: {e}", exc_info=True)
        await update_run_status(run_id, "FAILED", error_message=str(e))
        raise


if __name__ == "__main__":
    transform_flow(
        run_id="test-run-id",
        file_id="test-file-id",
        minio_path="raw/test.csv",
    )
