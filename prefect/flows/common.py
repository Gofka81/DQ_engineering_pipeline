"""
Shared utilities for DQ Analysis and Transform Prefect flows.
"""
import json
import logging
import os
from datetime import datetime
from pathlib import Path

import redis as redis_sync
from prefect import task
from prefect.logging import get_run_logger

from clients import get_pg_conn


def publish_status(run_id: str, payload: dict) -> None:
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


def setup_file_logger(run_id: str, filename: str, prefix: str) -> tuple[logging.Logger, Path]:
    """
    Create a file logger for a flow run.

    Args:
        run_id:   UUID of the run
        filename: Base filename (used in log filename)
        prefix:   Flow prefix — "dq" for DQ Analysis, "transform" for Transform
    """
    logs_dir = Path("/logs")
    logs_dir.mkdir(exist_ok=True)

    ts = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%S")
    log_path = logs_dir / f"{prefix}_{filename}_{ts}.log"

    log_name = f"{prefix}_run.{run_id}"
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


@task(retries=3, retry_delay_seconds=5)
async def update_run_status(run_id: str, status: str, error_message: str = None):
    """Update run status in PostgreSQL."""
    logger = get_run_logger()
    msg = f"Status → {status}" + (f" | error: {error_message}" if error_message else "")
    logger.info(msg)

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

    logger.debug(f"DB committed: run={run_id} status={status}")
