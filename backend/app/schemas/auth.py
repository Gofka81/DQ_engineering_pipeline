from pydantic import BaseModel, EmailStr, constr, Field
from typing import Optional


class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"


class TokenData(BaseModel):
    username: Optional[str] = None


class UserBase(BaseModel):
    username: str = Field(..., min_length=3, max_length=50)
    email: EmailStr


class UserOut(UserBase):
    disabled: Optional[bool] = None


class UserInDB(UserOut):
    hashed_password: str


class UserCreate(BaseModel):
    username: constr(min_length=3, max_length=50, strip_whitespace=True) = Field(...)
    email: EmailStr = Field(...)
    password: constr(min_length=8, max_length=128, strip_whitespace=True) = Field(...)


class UserRegisterOut(BaseModel):
    id: int
    username: str
    email: EmailStr
    disabled: bool = False

    class Config:
        from_attributes = True  # allows .from_orm()