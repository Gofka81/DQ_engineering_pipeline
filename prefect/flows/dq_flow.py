"""
DQ Analysis Prefect flow — task definitions and flow orchestration only.

Business logic lives in flows/dq_logic.py.
Client factories live in clients.py.
"""
import io
import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd
from minio.error import S3Error
from prefect import flow, task
from prefect.logging import get_run_logger

from clients import get_minio_client, get_minio_raw_bucket, get_pg_conn
from flows.common import publish_status as _publish_status, setup_file_logger, update_run_status
from flows.dq_logic import build_dropmasks, build_recommendations, count_issues_from_profile, parse_csv, profile_dataframe, score_profile, score_profile_detailed
from flows.llm_enrichment import enrich_recommendations


def _flog(run_id: str) -> logging.Logger:
    return logging.getLogger(f"dq_run.{run_id}")


@task(retries=3, retry_delay_seconds=5)
def load_dataframe_from_minio(minio_path: str, run_id: str, has_header: bool = True) -> tuple[pd.DataFrame, int]:
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
    log = _flog(run_id)

    logger.info(f"Loading dataframe from MinIO: {minio_path}")
    log.info(f"[load] reading raw CSV: {minio_path} (has_header={has_header})")

    client = get_minio_client()
    bucket = get_minio_raw_bucket()

    response = None
    try:
        response = client.get_object(bucket_name=bucket, object_name=minio_path)
        df, malformed_rows = parse_csv(response, has_header=has_header)
    except S3Error as e:
        log.error(f"[load] S3Error: {e}")
        raise RuntimeError(f"Failed to load {minio_path} from MinIO bucket '{bucket}': {e}")
    finally:
        if response:
            response.close()
            response.release_conn()

    if malformed_rows > 0:
        logger.warning(f"Skipped {malformed_rows} malformed rows (mismatched field count)")
        log.warning(f"[load] skipped {malformed_rows} malformed rows")

    logger.info(f"Loaded dataframe: {len(df)} rows × {len(df.columns)} columns")
    log.info(f"[load] {len(df)} rows × {len(df.columns)} columns | columns: {list(df.columns)}")
    return df, malformed_rows


@task
def profile_data(df: pd.DataFrame, run_id: str, malformed_rows: int = 0) -> dict[str, Any]:
    """Profile a DataFrame to identify data quality issues."""
    logger = get_run_logger()
    log = _flog(run_id)

    logger.info("Profiling data...")
    log.info(f"[profile] starting — {len(df)} rows × {len(df.columns)} columns")

    profile = profile_dataframe(df, malformed_rows)

    msg = (
        f"Profiling complete: {profile['total_rows']} rows | "
        f"completeness={profile['completeness']:.1f}% "
        f"uniqueness={profile['uniqueness']:.1f}% "
        f"validity={profile['validity']:.1f}% "
        f"consistency={profile['consistency']:.1f}%"
    )
    logger.info(msg)
    log.info(f"[profile] {msg}")
    log.info(
        f"[profile] missing_cells={profile['missing_cells']} "
        f"duplicate_rows={profile['duplicate_rows']} "
        f"malformed_rows={profile['malformed_rows']}"
    )

    for col, cp in profile["column_profiles"].items():
        parts = [f"type={cp.get('detected_type')}"]
        if cp.get("null_pct", 0) > 0:
            parts.append(f"null={cp['null_pct']:.1f}%")
        if cp.get("outliers", {}).get("count", 0) > 0:
            parts.append(f"outliers={cp['outliers']['count']}")
        if cp.get("invalid_count", 0) > 0:
            parts.append(f"invalid={cp['invalid_count']}")
        if cp.get("format_inconsistency"):
            parts.append("format_inconsistency=True")
        log.debug(f"  col [{col}]: {', '.join(parts)}")

    return profile


@task
def calculate_dq_score(profile: dict[str, Any], run_id: str) -> float:
    """Calculate weighted DQ score from profile components."""
    logger = get_run_logger()
    log = _flog(run_id)

    dq_score = score_profile(profile)
    logger.info(f"DQ Score: {dq_score:.2f}")
    log.info(f"[score] overall={dq_score:.2f}")
    return dq_score


@task
def generate_recommendations(
    df: pd.DataFrame, profile: dict[str, Any], dq_score: float, run_id: str
) -> dict[str, Any]:
    """Generate recommendations JSON based on profiling results."""
    logger = get_run_logger()
    log = _flog(run_id)

    logger.info("Generating recommendations...")
    log.info("[recs] generating baseline recommendations")

    recommendations = build_recommendations(df, profile, dq_score)

    cols = recommendations.get("columns", {})
    n_fill = sum(1 for cd in cols.values() if cd.get("missing_values"))
    n_outlier = len(recommendations.get("outliers", {}))
    n_sentinel = sum(1 for cd in cols.values() if cd.get("sentinel_values"))
    n_warnings = sum(len(cd.get("warnings", [])) for cd in cols.values())

    log.info(
        f"[recs] {len(cols)} columns | fill={n_fill} outlier_cols={n_outlier} "
        f"sentinel_cols={n_sentinel} warnings={n_warnings}"
    )
    for col, cd in cols.items():
        parts = [f"type={cd.get('type')}"]
        if cd.get("missing_values"):
            parts.append(f"fill={cd['missing_values'].get('strategy')}")
        if cd.get("normalize") and cd["normalize"] is not False:
            parts.append(f"normalize={cd['normalize']}")
        if cd.get("warnings"):
            parts.append(f"warnings={len(cd['warnings'])}")
        if cd.get("sentinel_values"):
            parts.append(f"sentinels={cd['sentinel_values']}")
        log.debug(f"  col [{col}]: {', '.join(parts)}")

    logger.info("Recommendations generated")
    return recommendations


@task
def enrich_with_llm(
        profile: dict[str, Any],
        base_recommendations: dict[str, Any],
        df: pd.DataFrame,
        run_id: str,
) -> dict[str, Any]:
    """Enrich recommendations via LLM (Runner → Validator). Soft failure — returns base_recs on any error."""
    logger = get_run_logger()
    log = _flog(run_id)

    logger.info("Starting LLM enrichment (Runner → Validator)...")
    log.info("[llm1] starting LLM enrichment (enrich_recommendations)")

    enriched = enrich_recommendations(profile, base_recommendations, df)

    n_notes = sum(1 for cd in enriched.get("columns", {}).values() if cd.get("note"))
    n_hints = sum(1 for cd in enriched.get("columns", {}).values() if cd.get("transform_hint"))
    n_renames = sum(1 for cd in enriched.get("columns", {}).values() if cd.get("rename_to"))

    msg = f"LLM enrichment done: {n_notes} notes, {n_hints} hints, {n_renames} renames"
    logger.info(msg)
    log.info(f"[llm1] {msg}")
    return enriched



@task
def upload_dropmasks(df: pd.DataFrame, profile: dict[str, Any], recs: dict[str, Any], file_id: str, run_id: str) -> None:
    """Build and upload drop-impact bitsets to MinIO raw bucket."""
    log = _flog(run_id)
    try:
        dropmasks = build_dropmasks(df, profile, recs)
        dropmasks_bytes = json.dumps(dropmasks).encode()
        path = f"{file_id}/dropmasks_{run_id}.json"
        client = get_minio_client()
        bucket = get_minio_raw_bucket()
        client.put_object(bucket, path, io.BytesIO(dropmasks_bytes), len(dropmasks_bytes))
        log.info(f"[dropmasks] uploaded {len(dropmasks['ops'])} ops to {path}")
    except Exception as e:
        # Non-critical — dropmask absence degrades gracefully (endpoint returns 404)
        log.warning(f"[dropmasks] upload failed (non-fatal): {e}")


@task(retries=3, retry_delay_seconds=5)
async def save_results_to_db(run_id: str, profile: dict[str, Any], recommendations: dict[str, Any]):
    """Save DQ scores (4 dimensions + overall) and recommendations to PostgreSQL."""
    logger = get_run_logger()
    log = _flog(run_id)

    logger.info(f"Saving results to database for run {run_id}")
    log.info("[save] computing detailed scores and writing to DB")

    dq_scores = score_profile_detailed(profile)
    dq_scores["issues_found"] = count_issues_from_profile(profile)
    dq_scores["total_rows"] = profile["total_rows"]
    dq_scores["total_columns"] = profile["total_columns"]
    dq_scores["malformed_rows"] = profile["malformed_rows"]

    conn = await get_pg_conn()
    try:
        await conn.execute(
            """
            UPDATE runs
            SET dq_scores_before          = $1::jsonb,
                recommendations_generated = $2::jsonb
            WHERE id = $3::uuid
            """,
            json.dumps(dq_scores),
            json.dumps(recommendations),
            run_id,
        )
    finally:
        await conn.close()

    msg = (
        f"Results saved: overall={dq_scores['overall']:.2f} "
        f"(C={dq_scores['completeness']:.1f} U={dq_scores['uniqueness']:.1f} "
        f"V={dq_scores['validity']:.1f} Co={dq_scores['consistency']:.1f})"
    )
    logger.info(msg)
    log.info(f"[save] {msg}")

    issues = dq_scores["issues_found"]
    log.info(
        f"[save] issues: missing={issues.get('missing', 0)} "
        f"dupes={issues.get('duplicates', 0)} "
        f"type_err={issues.get('type_mismatches', 0)} "
        f"sentinels={issues.get('sentinel_values', 0)} "
        f"outliers={issues.get('outliers', 0)} "
        f"format={issues.get('format_inconsistencies', 0)}"
    )


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(name="DQ Analysis", log_prints=True)
async def dq_analysis_flow(run_id: str, file_id: str, minio_path: str, has_header: bool = True):
    """
    Main DQ Analysis flow.

    Args:
        run_id:     UUID of the run record
        file_id:    UUID of the file record
        minio_path: Path to file in MinIO raw bucket
        has_header: whether the CSV has a header row (default True)
    """
    filename = Path(minio_path).stem
    log, log_path = setup_file_logger(run_id, filename, "dq")

    logger = get_run_logger()
    logger.info(f"Starting DQ Analysis for run_id={run_id}, file_id={file_id}")
    log.info(f"=== DQ Analysis flow start | run={run_id} file={file_id} path={minio_path} has_header={has_header} ===")
    log.info(f"Log file: {log_path}")

    try:
        await update_run_status(run_id, "ANALYZING")
        _publish_status(run_id, {"status": "ANALYZING"})

        df, malformed_rows       = load_dataframe_from_minio(minio_path, run_id, has_header=has_header)
        profile                  = profile_data(df, run_id, malformed_rows)
        dq_score                 = calculate_dq_score(profile, run_id)
        recommendations          = generate_recommendations(df, profile, dq_score, run_id)
        enriched_recommendations = enrich_with_llm(profile, recommendations, df, run_id)
        upload_dropmasks(df, profile, enriched_recommendations, file_id, run_id)

        await save_results_to_db(run_id, profile, enriched_recommendations)
        await update_run_status(run_id, "AWAITING_REVIEW")
        dq_scores = score_profile_detailed(profile)
        _publish_status(run_id, {"status": "AWAITING_REVIEW", "dq_scores_before": dq_scores})

        msg = f"DQ Analysis completed. Score: {dq_score:.2f}"
        logger.info(msg)
        log.info(f"=== {msg} ===")

    except Exception as e:
        logger.error(f"DQ Analysis failed: {e}", exc_info=True)
        log.error(f"=== DQ Analysis FAILED: {e} ===", exc_info=True)
        await update_run_status(run_id, "FAILED", error_message=str(e))
        _publish_status(run_id, {"status": "FAILED", "error_message": str(e)})
        raise


