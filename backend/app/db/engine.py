from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from backend.app.core.config import settings


engine: AsyncEngine = create_async_engine(
    settings.DATABASE_URL.unicode_string(),
    echo=settings.ENVIRONMENT == "development",   # SQL logging in dev
    pool_pre_ping=True,                           # detect stale connections
    pool_size=5,                                  # small for RPi / local dev
    max_overflow=10,
    future=True,
)

# Async session factory
AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    FastAPI dependency for async DB session
    Usage: db: AsyncSession = Depends(get_db)
    """
    async with AsyncSessionLocal() as session:
        yield session