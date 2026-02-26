import json

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
        self.jobs_queue = settings.REDIS_JOBS_QUEUE
        self._initialized = True

    def push_job(self, job_type: str, run_id, file_id, minio_path: str) -> None:
        """Push a job to the shared queue. job_type: 'dq_analysis' | 'transform'."""
        job = {
            "job_type":   job_type,
            "run_id":     str(run_id),
            "file_id":    str(file_id),
            "minio_path": minio_path,
        }
        self.client.lpush(self.jobs_queue, json.dumps(job))

    def ping(self) -> bool:
        """Check Redis connection."""
        try:
            return self.client.ping()
        except redis.ConnectionError:
            return False


def get_redis_service() -> RedisService:
    """FastAPI dependency for Redis service."""
    return RedisService()
