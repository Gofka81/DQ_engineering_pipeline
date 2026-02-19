import os

import asyncpg
from minio import Minio


def get_minio_client() -> Minio:
    """Create a MinIO client from environment variables."""
    return Minio(
        endpoint=os.environ.get("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.environ.get("MINIO_ROOT_USER", "minioadmin"),
        secret_key=os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin"),
        secure=os.environ.get("MINIO_SECURE", "false").lower() == "true",
    )


def get_minio_raw_bucket() -> str:
    return os.environ.get("MINIO_RAW_BUCKET", "raw")


async def get_pg_conn() -> asyncpg.Connection:
    """Create a PostgreSQL connection from environment variables."""
    return await asyncpg.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        user=os.environ.get("POSTGRES_USER", "prefect"),
        password=os.environ.get("POSTGRES_PASSWORD", "prefect"),
        database=os.environ.get("BACKEND_DB", "backend"),
    )
