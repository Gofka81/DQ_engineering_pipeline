import json
from typing import Any

import redis

from backend.app.core.config import settings


class RedisService:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self.client = redis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            decode_responses=True,
        )
        self.dq_queue = settings.REDIS_DQ_QUEUE
        self._initialized = True

    def push_dq_job(self, run_id, file_id, minio_path: str) -> None:
        """Push a DQ analysis job to the queue."""
        job = {
            "run_id": str(run_id),  # Convert UUID to string
            "file_id": str(file_id),  # Convert UUID to string
            "minio_path": minio_path,
        }
        self.client.lpush(self.dq_queue, json.dumps(job))

    def pop_dq_job(self, timeout: int = 0) -> dict[str, Any] | None:
        """
        Pop a DQ job from the queue (blocking).
        timeout=0 means block forever until a job is available.
        """
        result = self.client.brpop(self.dq_queue, timeout=timeout)
        if result:
            _, job_json = result
            return json.loads(job_json)
        return None

    def ping(self) -> bool:
        """Check Redis connection."""
        try:
            return self.client.ping()
        except redis.ConnectionError:
            return False


def get_redis_service() -> RedisService:
    """FastAPI dependency for Redis service."""
    return RedisService()
