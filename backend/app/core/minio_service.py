from io import BytesIO
from typing import BinaryIO

from minio import Minio
from minio.error import S3Error
from fastapi import HTTPException, status

from backend.app.core.config import settings


class MinioService:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self.client = Minio(
            endpoint=settings.MINIO_ENDPOINT,
            access_key=settings.MINIO_ROOT_USER,
            secret_key=settings.MINIO_ROOT_PASSWORD.get_secret_value(),
            secure=settings.MINIO_SECURE,
        )
        public_url = settings.MINIO_PUBLIC_ENDPOINT or settings.MINIO_ENDPOINT
        public_secure = public_url.startswith("https://") if settings.MINIO_PUBLIC_ENDPOINT else settings.MINIO_SECURE
        public_endpoint = public_url.split("://", 1)[-1] if "://" in public_url else public_url
        self.presign_client = Minio(
            endpoint=public_endpoint,
            access_key=settings.MINIO_ROOT_USER,
            secret_key=settings.MINIO_ROOT_PASSWORD.get_secret_value(),
            secure=public_secure,
        )
        self.raw_bucket = settings.MINIO_RAW_BUCKET
        self.curated_bucket = settings.MINIO_CURATED_BUCKET

        self._ensure_bucket(self.raw_bucket)
        self._ensure_bucket(self.curated_bucket)
        self._initialized = True

    def _ensure_bucket(self, bucket_name: str) -> None:
        try:
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
        except S3Error as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"MinIO bucket creation failed: {exc}",
            )

    def upload_file(
        self,
        file_id: str,
        file_data: BinaryIO,
        file_size: int,
        original_filename: str,
        content_type: str = "text/csv",
        bucket: str | None = None,
    ) -> str:
        """
        Upload file to MinIO bucket.
        Returns object path (key).
        """
        bucket = bucket or self.raw_bucket
        object_name = f"{file_id}/{original_filename}"

        try:
            self.client.put_object(
                bucket_name=bucket,
                object_name=object_name,
                data=file_data,
                length=file_size,
                content_type=content_type,
            )
        except S3Error as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"MinIO upload failed: {exc}",
            )

        return object_name

    def download_file(self, object_name: str, bucket: str | None = None) -> BytesIO:
        """
        Download file from MinIO bucket.
        Returns file content as BytesIO.
        """
        bucket = bucket or self.raw_bucket

        try:
            response = self.client.get_object(bucket_name=bucket, object_name=object_name)
            data = BytesIO(response.read())
            response.close()
            response.release_conn()
            return data
        except S3Error as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"File not found: {exc}",
            )

    def get_presigned_url(
        self,
        object_name: str,
        bucket: str | None = None,
        expires_hours: int = 1,
    ) -> str:
        """
        Generate presigned URL for file download.
        """
        from datetime import timedelta

        bucket = bucket or self.curated_bucket

        try:
            url = self.presign_client.presigned_get_object(
                bucket_name=bucket,
                object_name=object_name,
                expires=timedelta(hours=expires_hours),
            )
            return url
        except S3Error as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to generate presigned URL: {exc}",
            )

    def delete_file(self, object_name: str, bucket: str | None = None) -> None:
        """Delete file from MinIO bucket."""
        bucket = bucket or self.raw_bucket

        try:
            self.client.remove_object(bucket_name=bucket, object_name=object_name)
        except S3Error as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"MinIO delete failed: {exc}",
            )


def get_minio_service() -> MinioService:
    """FastAPI dependency for MinIO service."""
    return MinioService()
