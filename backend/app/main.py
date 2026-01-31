from fastapi import FastAPI, Depends

from backend.app.core.config import settings
from backend.app.api.auth import router as auth_router
from backend.app.dependencies import get_current_active_user
from backend.app.schemas.auth import UserOut

app = FastAPI(
    title="Data Cleaning Platform API",
    description="API for user-guided data quality & transformation",
    version="0.1.0",
    docs_url="/docs" if settings.ENVIRONMENT != "production" else None,
)


app.include_router(auth_router, prefix="/api")


@app.get("/health")
async def health():
    return {"status": "healthy", "environment": settings.ENVIRONMENT}


@app.get("/api/users/me", response_model=UserOut)
async def read_users_me(current_user: UserOut = Depends(get_current_active_user)):
    return current_user


@app.get("/api/protected/me", response_model=UserOut)
async def get_my_profile(current_user: UserOut = Depends(get_current_active_user)):
    """Simple protected route to verify JWT works"""
    return current_user