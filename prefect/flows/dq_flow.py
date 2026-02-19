"""
DQ Analysis Prefect flow — task definitions and flow orchestration only.

Business logic lives in flows/dq_logic.py.
Client factories live in clients.py.
"""
import asyncio
import json
from typing import Any

import pandas as pd
from minio.error import S3Error
from prefect import flow, task
from prefect.logging import get_run_logger

from clients import get_minio_client, get_minio_raw_bucket, get_pg_conn
from flows.dq_logic import build_recommendations, parse_csv, profile_dataframe, score_profile


@task(retries=3, retry_delay_seconds=5)
def update_run_status(run_id: str, status: str, error_message: str = None):
    """Update run status in PostgreSQL."""
    logger = get_run_logger()
    logger.info(f"Updating run {run_id} to status: {status}")

    async def _update():
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

    asyncio.run(_update())
    logger.info(f"Run {run_id} status updated to {status}")


@task(retries=3, retry_delay_seconds=5)
def load_dataframe_from_minio(minio_path: str) -> tuple[pd.DataFrame, int]:
    """
    Load CSV from MinIO into a pandas DataFrame.

    Streams the MinIO response directly into pd.read_csv() — no intermediate
    file on disk and no BytesIO copy. Malformed rows (mismatched field count)
    are skipped and counted rather than crashing the flow.

    Returns:
        df:             parsed DataFrame
        malformed_rows: count of skipped rows (surfaced in profile)
    """
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
        logger.warning(f"Skipped {malformed_rows} malformed rows (mismatched field count)")

    logger.info(f"Loaded dataframe: {len(df)} rows × {len(df.columns)} columns")
    return df, malformed_rows


@task
def profile_data(df: pd.DataFrame, malformed_rows: int = 0) -> dict[str, Any]:
    """Profile a DataFrame to identify data quality issues."""
    logger = get_run_logger()
    logger.info("Profiling data...")

    profile = profile_dataframe(df, malformed_rows)

    logger.info(
        f"Profiling complete: {profile['total_rows']} rows | "
        f"completeness={profile['completeness']:.1f}% "
        f"uniqueness={profile['uniqueness']:.1f}% "
        f"validity={profile['validity']:.1f}% "
        f"consistency={profile['consistency']:.1f}%"
    )
    return profile


@task
def calculate_dq_score(profile: dict[str, Any]) -> float:
    """Calculate weighted DQ score from profile components."""
    logger = get_run_logger()
    dq_score = score_profile(profile)
    logger.info(f"DQ Score: {dq_score:.2f}")
    return dq_score


@task
def generate_recommendations(profile: dict[str, Any], dq_score: float) -> dict[str, Any]:
    """Generate recommendations JSON based on profiling results."""
    logger = get_run_logger()
    logger.info("Generating recommendations...")
    recommendations = build_recommendations(profile, dq_score)
    logger.info("Recommendations generated")
    return recommendations


@task(retries=3, retry_delay_seconds=5)
def save_results_to_db(run_id: str, dq_score: float, recommendations: dict[str, Any]):
    """Save DQ score and recommendations to PostgreSQL."""
    logger = get_run_logger()
    logger.info(f"Saving results to database for run {run_id}")

    async def _save():
        conn = await get_pg_conn()
        try:
            await conn.execute(
                """
                UPDATE runs
                SET dq_score_before           = $1,
                    recommendations_generated = $2::jsonb
                WHERE id = $3::uuid
                """,
                dq_score,
                json.dumps(recommendations),
                run_id,
            )
        finally:
            await conn.close()

    asyncio.run(_save())
    logger.info(f"Results saved: dq_score_before={dq_score:.2f}")


@flow(name="DQ Analysis", log_prints=True)
def dq_analysis_flow(run_id: str, file_id: str, minio_path: str):
    """
    Main DQ Analysis flow.

    Args:
        run_id:     UUID of the run record
        file_id:    UUID of the file record
        minio_path: Path to file in MinIO raw bucket
    """
    logger = get_run_logger()
    logger.info(f"Starting DQ Analysis for run_id={run_id}, file_id={file_id}")

    try:
        update_run_status(run_id, "ANALYZING")

        df, malformed_rows = load_dataframe_from_minio(minio_path)
        profile            = profile_data(df, malformed_rows)
        dq_score           = calculate_dq_score(profile)
        recommendations    = generate_recommendations(profile, dq_score)

        save_results_to_db(run_id, dq_score, recommendations)
        update_run_status(run_id, "AWAITING_REVIEW")

        logger.info(f"DQ Analysis completed successfully. Score: {dq_score}")

    except Exception as e:
        logger.error(f"DQ Analysis failed: {e}", exc_info=True)
        update_run_status(run_id, "FAILED", error_message=str(e))
        raise


if __name__ == "__main__":
    dq_analysis_flow(
        run_id="test-run-id",
        file_id="test-file-id",
        minio_path="raw/test.csv",
    )
