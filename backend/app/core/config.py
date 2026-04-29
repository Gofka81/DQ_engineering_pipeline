from pathlib import Path

from pydantic import PostgresDsn, SecretStr, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from typing import Optional

env_path = Path(__file__).resolve().parents[3] / ".env"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=env_path if env_path.exists() else None,
        env_file_encoding="utf-8",
        env_ignore_empty=True,
        extra="ignore",
    )

    # Environment
    ENVIRONMENT: str = Field(default="development")
    CORS_ORIGINS: list[str] = Field(default=["*"])

    # PostgreSQL
    POSTGRES_HOST: str = Field(default="localhost")
    POSTGRES_PORT: int = Field(default=5432)
    POSTGRES_USER: str = Field(default="postgres")
    POSTGRES_PASSWORD: SecretStr = Field(default="postgres")
    BACKEND_DB: str = Field(default="backend")

    # JWT
    SECRET_KEY: SecretStr = Field(default="change-this-very-long-random-string-please")
    ALGORITHM: str = Field(default="HS256")
    ACCESS_TOKEN_EXPIRE_MINUTES: int = Field(default=1440)

    # MinIO
    MINIO_ENDPOINT: str = Field(default="localhost:9000")
    MINIO_PUBLIC_ENDPOINT: Optional[str] = Field(default=None)
    MINIO_ROOT_USER: str = Field(default="minioadmin")
    MINIO_ROOT_PASSWORD: SecretStr = Field(default="minioadmin")
    MINIO_RAW_BUCKET: str = Field(default="raw")
    MINIO_CURATED_BUCKET: str = Field(default="curated")
    MINIO_SECURE: bool = Field(default=False)

    # File upload
    MAX_FILE_SIZE_MB: int = Field(default=200)

    # Redis
    REDIS_HOST: str = Field(default="localhost")
    REDIS_PORT: int = Field(default=6379)
    REDIS_JOBS_QUEUE: str = Field(default="jobs")

    # Optional: full async DSN
    DATABASE_URL: Optional[PostgresDsn] = None

    def model_post_init(self, __context) -> None:
        if self.DATABASE_URL is None:
            self.DATABASE_URL = PostgresDsn.build(
                scheme="postgresql+asyncpg",
                username=self.POSTGRES_USER,
                password=self.POSTGRES_PASSWORD.get_secret_value(),
                host=self.POSTGRES_HOST,
                port=self.POSTGRES_PORT,
                path=f"{self.BACKEND_DB.lstrip('/')}",
            )


settings = Settings()
