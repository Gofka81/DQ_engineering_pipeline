#!/usr/bin/env python3
"""
Redis Worker - Polls Redis queue and triggers Prefect deployments.
Runs as a background process in the prefect-worker container.

Architecture:
- BRPOP blocks until job available (no CPU waste)
- Triggers Prefect deployment without waiting (timeout=0)
- Worker picks up the deployment and executes the flow
"""
import asyncio
import json
import logging
import os
import sys

import redis
from prefect.deployments import run_deployment

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("redis_worker")


async def trigger_dq_flow(job: dict) -> None:
    """
    Trigger DQ Analysis Prefect deployment without waiting for completion.

    Args:
        job: Dict with run_id, file_id, minio_path
    """
    try:
        flow_run = await run_deployment(
            name="DQ Analysis/dq-analysis",
            parameters={
                "run_id": job["run_id"],
                "file_id": job["file_id"],
                "minio_path": job["minio_path"],
            },
            timeout=0,  # Don't wait for completion (fire and forget)
            as_subflow=False,  # Not a subflow (standalone trigger)
        )
        logger.info(f"Successfully triggered flow run: {flow_run.id}")
    except Exception as e:
        logger.error(f"Failed to trigger deployment: {e}", exc_info=True)
        raise


async def main():
    """Main loop: poll Redis queue and trigger Prefect deployments."""
    # Get configuration from environment
    redis_host = os.getenv("REDIS_HOST", "redis")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))
    queue_name = os.getenv("REDIS_DQ_QUEUE", "dq_jobs")

    logger.info(f"Redis worker starting...")
    logger.info(f"Redis: {redis_host}:{redis_port}")
    logger.info(f"Queue: {queue_name}")

    # Connect to Redis
    try:
        redis_client = redis.Redis(
            host=redis_host,
            port=redis_port,
            decode_responses=True,
        )
        await redis_client.ping()
        logger.info("Successfully connected to Redis")
    except redis.ConnectionError as e:
        logger.error(f"Failed to connect to Redis: {e}")
        sys.exit(1)

    # Main loop - BRPOP blocks until job available
    logger.info(f"Listening for jobs on queue: {queue_name}")

    while True:
        try:
            # BRPOP blocks forever (timeout=0) until a job is available
            result = redis_client.brpop(queue_name, timeout=0)

            if result:
                _, job_json = result
                job = json.loads(job_json)

                logger.info(f"Received job: run_id={job.get('run_id')}, file_id={job.get('file_id')}")

                # Trigger the Prefect deployment
                await trigger_dq_flow(job)

        except json.JSONDecodeError as e:
            logger.error(f"Failed to decode job JSON: {e}")
            continue

        except KeyboardInterrupt:
            logger.info("Received shutdown signal. Exiting...")
            break

        except Exception as e:
            logger.error(f"Error processing job: {e}", exc_info=True)
            # Sleep before retrying to avoid tight loop on persistent errors
            await asyncio.sleep(5)
            continue


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Redis worker stopped")
