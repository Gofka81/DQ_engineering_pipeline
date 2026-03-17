"""
Transform Prefect flow — applies approved recommendations to the raw CSV
and saves the cleaned file to the MinIO curated bucket.

Flow order:
    1. update_run_status → TRANSFORMING
    2. load_dataframe_from_minio — load original raw CSV
    3. load_approved_recommendations — fetch from DB
    4. generate_missing_transform_codes — LLM 2 for user-added hints (if any)
    5. apply_transform — pure pandas (dq_logic.apply_recommendations)
    6. upload_cleaned_file — write CSV to curated bucket
    7. profile + score — calculate dq_scores_after
    8. save_transform_results_to_db — curated path + scores + COMPLETED
"""
import io
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import redis as redis_sync
from minio.error import S3Error
from prefect import flow, task
from prefect.logging import get_run_logger

from clients import get_minio_client, get_minio_raw_bucket, get_minio_curated_bucket, get_pg_conn
from flows.dq_logic import apply_recommendations, count_issues_from_profile, parse_csv, profile_dataframe, score_profile_detailed
from flows.llm_enrichment import generate_transform_code


# ---------------------------------------------------------------------------
# File logger — writes to /prefect/logs/transform_<filename>_<timestamp>.log
# ---------------------------------------------------------------------------

def _setup_file_logger(run_id: str, filename: str) -> tuple[logging.Logger, Path]:
    logs_dir = Path("/logs")
    logs_dir.mkdir(exist_ok=True)

    ts = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
    log_path = logs_dir / f"transform_{filename}_{ts}.log"

    log_name = f"transform_run.{run_id}"
    log = logging.getLogger(log_name)
    log.setLevel(logging.DEBUG)
    log.handlers.clear()
    log.propagate = False

    fmt = logging.Formatter(
        "%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    fh = logging.FileHandler(log_path, mode="w", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    log.addHandler(fh)

    # Share handler with dq_logic and llm_enrichment so their logs land in the same file
    for module in ("flows.dq_logic", "flows.llm_enrichment"):
        mod_log = logging.getLogger(module)
        mod_log.setLevel(logging.DEBUG)
        if not any(isinstance(h, logging.FileHandler) and h.baseFilename == str(log_path)
                   for h in mod_log.handlers):
            mod_log.addHandler(fh)

    return log, log_path


def _flog(run_id: str) -> logging.Logger:
    """Return the named file logger for this run (no-op if not yet set up)."""
    return logging.getLogger(f"transform_run.{run_id}")


def _publish_status(run_id: str, payload: dict) -> None:
    """Publish run status event to Redis Pub/Sub channel (fire-and-forget)."""
    try:
        r = redis_sync.Redis(
            host=os.environ.get("REDIS_HOST", "localhost"),
            port=int(os.environ.get("REDIS_PORT", "6379")),
        )
        r.publish(f"run:{run_id}:status", json.dumps(payload))
        r.close()
    except Exception:
        pass  # SSE is best-effort; never fail the flow


@task(retries=3, retry_delay_seconds=5)
async def update_run_status(run_id: str, status: str, error_message: str = None):
    """Update run status in PostgreSQL."""
    logger = get_run_logger()
    log = _flog(run_id)

    msg = f"Status → {status}" + (f" | error: {error_message}" if error_message else "")
    logger.info(msg)
    log.info(f"[status] {msg}")

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

    log.debug(f"[status] DB committed: run={run_id} status={status}")


@task(retries=3, retry_delay_seconds=5)
def load_dataframe_from_minio(minio_path: str, run_id: str) -> pd.DataFrame:
    """Load CSV from MinIO raw bucket into a DataFrame."""
    logger = get_run_logger()
    log = _flog(run_id)

    logger.info(f"Loading dataframe from MinIO: {minio_path}")
    log.info(f"[load] reading raw CSV: {minio_path}")

    client = get_minio_client()
    bucket = get_minio_raw_bucket()

    response = None
    try:
        response = client.get_object(bucket_name=bucket, object_name=minio_path)
        df, malformed_rows = parse_csv(response)
    except S3Error as e:
        log.error(f"[load] S3Error: {e}")
        raise RuntimeError(f"Failed to load {minio_path} from MinIO bucket '{bucket}': {e}")
    finally:
        if response:
            response.close()
            response.release_conn()

    if malformed_rows > 0:
        logger.warning(f"Skipped {malformed_rows} malformed rows")
        log.warning(f"[load] skipped {malformed_rows} malformed rows")

    logger.info(f"Loaded dataframe: {len(df)} rows × {len(df.columns)} columns")
    log.info(f"[load] {len(df)} rows × {len(df.columns)} columns | columns: {list(df.columns)}")
    return df


@task(retries=3, retry_delay_seconds=5)
async def load_approved_recommendations(run_id: str) -> dict[str, Any]:
    """Fetch the user-approved recommendations JSON from PostgreSQL."""
    logger = get_run_logger()
    log = _flog(run_id)

    logger.info(f"Loading approved recommendations for run {run_id}")
    log.info("[recs] loading approved recommendations from DB")

    conn = await get_pg_conn()
    try:
        row = await conn.fetchrow(
            "SELECT recommendations_approved FROM runs WHERE id = $1::uuid",
            run_id,
        )
        if row is None:
            log.error(f"[recs] run {run_id} not found in DB")
            raise RuntimeError(f"Run {run_id} not found in database")
        if row["recommendations_approved"] is None:
            log.error(f"[recs] run {run_id} has no approved recommendations")
            raise RuntimeError(f"Run {run_id} has no approved recommendations")
        # asyncpg returns JSONB as a raw JSON string — decode it (Decision #23)
        raw = row["recommendations_approved"]
        recommendations = json.loads(raw) if isinstance(raw, str) else dict(raw)
    finally:
        await conn.close()

    cols = recommendations.get("columns", {})
    log.info(f"[recs] {len(cols)} column(s): {list(cols.keys())}")

    for col, cd in cols.items():
        parts = [f"type={cd.get('type')}"]
        if cd.get("missing_values"):
            parts.append(f"fill={cd['missing_values'].get('strategy')}")
        if cd.get("normalize") and cd["normalize"] is not False:
            parts.append(f"normalize={cd['normalize']}")
        if cd.get("rename_to"):
            parts.append(f"rename→{cd['rename_to']}")
        if cd.get("transform_hint"):
            parts.append(f"hint='{cd['transform_hint']}'")
        if cd.get("transform_code"):
            parts.append(f"code='{cd['transform_code']}'")
        log.debug(f"  col [{col}]: {', '.join(parts)}")

    dup = recommendations.get("duplicates", {})
    log.info(f"[recs] duplicates: strategy={dup.get('strategy')} subset={dup.get('subset')}")

    outliers = recommendations.get("outliers", {})
    if outliers:
        log.info(f"[recs] outliers for {len(outliers)} column(s): {list(outliers.keys())}")
        for col, oc in outliers.items():
            log.debug(
                f"  outlier [{col}]: count={oc.get('count')} strategy={oc.get('strategy')} "
                f"lower={oc.get('lower')} upper={oc.get('upper')}"
            )

    hints_needing_code = [
        c for c, cd in cols.items() if cd.get("transform_hint") and not cd.get("transform_code")
    ]
    if hints_needing_code:
        log.warning(
            f"[recs] {len(hints_needing_code)} column(s) have transform_hint but no transform_code "
            f"— will generate: {hints_needing_code}"
        )
    else:
        log.info("[recs] all transform_hints already have transform_code (or none present)")

    logger.info("Approved recommendations loaded")
    return recommendations


@task
def generate_missing_transform_codes(
    recommendations: dict[str, Any],
    df: pd.DataFrame,
    run_id: str,
) -> dict[str, Any]:
    """Generate transform_code for any column that has transform_hint but no transform_code."""
    logger = get_run_logger()
    log = _flog(run_id)

    pending = {
        col: cd["transform_hint"]
        for col, cd in recommendations.get("columns", {}).items()
        if cd.get("transform_hint") and not cd.get("transform_code")
    }

    if not pending:
        log.info("[gen_codes] no pending hints — skipping LLM 2")
        logger.info("generate_missing_transform_codes: nothing to generate")
        return recommendations

    log.info(f"[gen_codes] {len(pending)} column(s) need transform_code:")
    logger.info(f"generate_missing_transform_codes: generating for {list(pending.keys())}")
    for col, hint in pending.items():
        log.info(f"  [{col}] hint: {hint}")

    updated = generate_transform_code(recommendations, df)

    generated = {
        col: cd["transform_code"]
        for col, cd in updated.get("columns", {}).items()
        if cd.get("transform_code") and col in pending
    }
    failed = [col for col in pending if col not in generated]

    log.info(f"[gen_codes] generated {len(generated)}/{len(pending)} transform_code(s)")
    for col, code in generated.items():
        log.info(f"  [{col}] code: {code}")
    if failed:
        log.warning(f"[gen_codes] no code produced for: {failed}")
        logger.warning(f"generate_missing_transform_codes: failed for {failed}")

    return updated


@task
def apply_transform(df: pd.DataFrame, recommendations: dict[str, Any], run_id: str) -> pd.DataFrame:
    """Apply approved recommendations to the DataFrame."""
    logger = get_run_logger()
    log = _flog(run_id)

    rows_before = len(df)
    cols_before = set(df.columns)
    log.info(f"[apply] start — {rows_before} rows × {len(df.columns)} columns")

    cleaned_df = apply_recommendations(df, recommendations)

    rows_after = len(cleaned_df)
    cols_removed = cols_before - set(cleaned_df.columns)
    dropped_rows = rows_before - rows_after

    msg = (
        f"Transform applied: {rows_before} → {rows_after} rows "
        f"({dropped_rows} dropped)"
    )
    logger.info(msg)
    log.info(f"[apply] {msg}")
    if cols_removed:
        log.info(f"[apply] columns removed (renamed/dropped): {sorted(cols_removed)}")
    log.info(f"[apply] output columns: {list(cleaned_df.columns)}")
    return cleaned_df


@task(retries=3, retry_delay_seconds=5)
def upload_cleaned_file(df: pd.DataFrame, file_id: str, run_id: str) -> str:
    """
    Upload cleaned CSV to MinIO curated bucket.

    Object path: curated/{file_id}/{run_id}/cleaned.csv
    Returns the object path.
    """
    logger = get_run_logger()
    log = _flog(run_id)

    buf = io.BytesIO()
    df.to_csv(buf, index=False)
    buf.seek(0)
    file_size = buf.getbuffer().nbytes

    object_path = f"{file_id}/{run_id}/cleaned.csv"

    client = get_minio_client()
    bucket = get_minio_curated_bucket()

    log.info(f"[upload] {file_size} bytes → {bucket}/{object_path}")

    try:
        client.put_object(
            bucket_name=bucket,
            object_name=object_path,
            data=buf,
            length=file_size,
            content_type="text/csv",
        )
    except S3Error as e:
        log.error(f"[upload] S3Error: {e}")
        raise RuntimeError(f"Failed to upload cleaned file to MinIO: {e}")

    msg = f"Cleaned file uploaded: {bucket}/{object_path} ({file_size} bytes)"
    logger.info(msg)
    log.info(f"[upload] done — {msg}")
    return object_path


@task
def score_cleaned_data(df: pd.DataFrame, run_id: str, approved_outliers: dict | None = None) -> dict:
    """Profile cleaned DataFrame and return DQ scores + issue counts."""
    logger = get_run_logger()
    log = _flog(run_id)

    log.info(f"[score] profiling {len(df)} rows × {len(df.columns)} columns")
    profile = profile_dataframe(df, malformed_rows=0)
    scores = score_profile_detailed(profile)
    scores["issues_found"] = count_issues_from_profile(profile, approved_outliers=approved_outliers)
    scores["total_rows"] = profile["total_rows"]
    scores["total_columns"] = profile["total_columns"]
    scores["malformed_rows"] = profile["malformed_rows"]

    msg = (
        f"DQ scores after transform: overall={scores['overall']:.2f} "
        f"(C={scores['completeness']:.1f} U={scores['uniqueness']:.1f} "
        f"V={scores['validity']:.1f} Co={scores['consistency']:.1f})"
    )
    logger.info(msg)
    log.info(f"[score] {msg}")

    issues = scores["issues_found"]
    log.info(
        f"[score] issues after: missing={issues.get('missing', 0)} "
        f"dupes={issues.get('duplicates', 0)} "
        f"type_err={issues.get('type_mismatches', 0)} "
        f"sentinels={issues.get('sentinel_values', 0)} "
        f"outliers={issues.get('outliers', 0)} "
        f"format={issues.get('format_inconsistencies', 0)}"
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
    log = _flog(run_id)

    logger.info(f"Saving transform results for run {run_id}")
    log.info(f"[save] writing COMPLETED + scores + curated_path={minio_curated_path}")

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
    log.info(f"[save] run {run_id} COMPLETED")


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
    filename = Path(minio_path).stem
    log, log_path = _setup_file_logger(run_id, filename)

    logger = get_run_logger()
    logger.info(f"Starting Transform for run_id={run_id}, file_id={file_id}")
    log.info(f"=== Transform flow start | run={run_id} file={file_id} path={minio_path} ===")
    log.info(f"Log file: {log_path}")

    try:
        await update_run_status(run_id, "TRANSFORMING")
        _publish_status(run_id, {"status": "TRANSFORMING"})

        df              = load_dataframe_from_minio(minio_path, run_id)
        recommendations = await load_approved_recommendations(run_id)
        recommendations = generate_missing_transform_codes(recommendations, df, run_id)
        cleaned_df      = apply_transform(df, recommendations, run_id)
        curated_path    = upload_cleaned_file(cleaned_df, file_id, run_id)
        dq_scores_after = score_cleaned_data(cleaned_df, run_id, approved_outliers=recommendations.get("outliers"))

        await save_transform_results_to_db(run_id, dq_scores_after, curated_path)
        _publish_status(run_id, {"status": "COMPLETED", "dq_scores_after": dq_scores_after})

        msg = (
            f"Transform completed. "
            f"Score: {dq_scores_after['overall']:.2f} "
            f"(saved to {curated_path})"
        )
        logger.info(msg)
        log.info(f"=== {msg} ===")

    except Exception as e:
        logger.error(f"Transform failed: {e}", exc_info=True)
        log.error(f"=== Transform FAILED: {e} ===", exc_info=True)
        await update_run_status(run_id, "FAILED", error_message=str(e))
        _publish_status(run_id, {"status": "FAILED", "error_message": str(e)})
        raise


if __name__ == "__main__":
    transform_flow(
        run_id="test-run-id",
        file_id="test-file-id",
        minio_path="raw/test.csv",
    )
