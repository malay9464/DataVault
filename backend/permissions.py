from fastapi import HTTPException, Depends, status
from auth import get_current_user

def is_admin(user: dict) -> bool:
    return user["role"] == "admin"


def can_delete_upload(user: dict, upload_owner_id: int) -> bool:
    if is_admin(user):
        return True
    return user["id"] == upload_owner_id


def can_access_upload(user: dict, upload_owner_id: int) -> bool:
    if is_admin(user):
        return True
    return user["id"] == upload_owner_id


def admin_only(user: dict):
    if user["role"] != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required"
        )