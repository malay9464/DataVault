from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy import text
from datetime import datetime, timedelta
import jwt

from db import engine
from security import verify_password

# ⚠️ move to env later
SECRET_KEY = "CHANGE_THIS_SECRET"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/login")

def authenticate_user(email: str, password: str):
    with engine.begin() as conn:
        user = conn.execute(
            text("""
                SELECT id, email, password_hash, role, is_active
                FROM users
                WHERE email = :email
            """),
            {"email": email}
        ).mappings().first()

    if not user:
        return None
    if not user["is_active"]:
        return None
    if not verify_password(password, user["password_hash"]):
        return None

    return user


def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(
        minutes=ACCESS_TOKEN_EXPIRE_MINUTES
    )
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def get_current_user(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = payload.get("user_id")
        role = payload.get("role")
    except jwt.PyJWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token"
        )

    with engine.begin() as conn:
        user = conn.execute(
            text("""
                SELECT id, email, role, is_active
                FROM users
                WHERE id = :id
            """),
            {"id": user_id}
        ).mappings().first()

    if not user or not user["is_active"]:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="User inactive or not found"
        )

    return user
