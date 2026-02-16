from datetime import timedelta
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.core.config import settings
from backend.app.core.security import create_access_token, verify_password, get_password_hash
from backend.app.db.engine import get_db
from backend.app.dependencies import get_user_by_username
from backend.app.schemas.auth import Token, UserCreate, UserRegisterOut
from backend.app.db.models import User


router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/login", response_model=Token)
async def login_for_access_token(
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()],
    db: AsyncSession = Depends(get_db)
):
    user = await get_user_by_username(db, form_data.username)
    if not user or not verify_password(form_data.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        subject=user.username,
        expires_delta=access_token_expires,
    )

    return Token(access_token=access_token, token_type="bearer")

@router.post(
    "/register",
    response_model=UserRegisterOut,
    status_code=status.HTTP_201_CREATED,
    summary="Register a new user",
    responses={
        409: {"description": "Username or email already registered"},
        422: {"description": "Validation error"}
    }
)
async def register_new_user(
    user_in: UserCreate,
    db: AsyncSession = Depends(get_db)
):
    # Check duplicates
    stmt = select(User).where(
        (User.username == user_in.username) | (User.email == user_in.email)
    )
    result = await db.execute(stmt)
    existing = result.scalar_one_or_none()

    if existing:
        if existing.username == user_in.username:
            raise HTTPException(409, "Username already registered")
        raise HTTPException(409, "Email already registered")

    # Hash password
    hashed = get_password_hash(user_in.password)

    # Create & save
    new_user = User(
        username=user_in.username,
        email=user_in.email,
        hashed_password=hashed,
        disabled=False
    )

    db.add(new_user)
    await db.commit()
    await db.refresh(new_user)

    return UserRegisterOut.model_validate(new_user)

