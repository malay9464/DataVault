from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import text

from db import engine
from auth import get_current_user
from security import hash_password
from permissions import admin_only

router = APIRouter(prefix="/users", tags=["users"])


@router.get("")
def list_users(user: dict = Depends(get_current_user)):
    admin_only(user)

    with engine.begin() as conn:
        rows = conn.execute(
            text("""
                SELECT id, email, role, is_active, created_at, last_login_at
                FROM users
                ORDER BY created_at DESC
            """)
        ).mappings().all()

    return rows


@router.post("")
def create_user(
    email: str,
    password: str,
    role: str = "user",
    user: dict = Depends(get_current_user)
):
    admin_only(user)

    if role not in ("admin", "user"):
        raise HTTPException(status_code=400, detail="Invalid role")

    with engine.begin() as conn:
        try:
            conn.execute(
                text("""
                    INSERT INTO users (email, password_hash, role)
                    VALUES (:e, :p, :r)
                """),
                {
                    "e": email.lower(),
                    "p": hash_password(password),
                    "r": role
                }
            )
        except:
            raise HTTPException(status_code=400, detail="User exists")

    return {"success": True}


@router.put("/{user_id}/status")
def set_user_status(
    user_id: int,
    is_active: bool,
    user: dict = Depends(get_current_user)
):
    admin_only(user)

    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE users
                SET is_active = :a
                WHERE id = :id
            """),
            {"a": is_active, "id": user_id}
        )

    return {"success": True}


@router.put("/{user_id}/role")
def change_role(
    user_id: int,
    role: str,
    user: dict = Depends(get_current_user)
):
    admin_only(user)

    if role not in ("admin", "user"):
        raise HTTPException(status_code=400)

    with engine.begin() as conn:
        conn.execute(
            text("""
                UPDATE users
                SET role = :r
                WHERE id = :id
            """),
            {"r": role, "id": user_id}
        )

    return {"success": True}
