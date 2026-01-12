from fastapi import (
    FastAPI, UploadFile, File, Query,
    HTTPException, Depends
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy import text
from datetime import date
import pandas as pd
import io, json, os, time
from users import router as users_router
from db import engine
from logger import log_to_csv
from auth import authenticate_user, create_access_token, get_current_user
from permissions import can_delete_upload, admin_only
from security import hash_password


app = FastAPI()

app.include_router(users_router)

app.mount("/static", StaticFiles(directory="../frontend"), name="static")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------- AUTH ----------------
@app.post("/login")
def login(form: OAuth2PasswordRequestForm = Depends()):
    user = authenticate_user(form.username, form.password)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    token = create_access_token({
        "user_id": user["id"],
        "role": user["role"]
    })

    return {"access_token": token, "token_type": "bearer"}


# ---------------- UTIL ----------------
def clean_nan(row):
    return {k: (None if pd.isna(v) else v) for k, v in row.items()}


# ---------------- CATEGORIES (ADMIN VIA UI LATER) ----------------
@app.get("/categories")
def list_categories():
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT c.id, c.name, COUNT(u.upload_id) AS uploads
            FROM categories c
            LEFT JOIN upload_log u ON u.category_id = c.id
            GROUP BY c.id
            ORDER BY c.name
        """)).fetchall()

    return [
        {"id": r.id, "name": r.name, "uploads": r.uploads}
        for r in rows
    ]


# ---------------- UPLOAD ----------------
@app.post("/upload")
async def upload_file(
    category_id: int = Query(...),
    file: UploadFile = File(...),
    user: dict = Depends(get_current_user)
):
    contents = await file.read()
    name = file.filename.lower()

    if name.endswith(".csv"):
        df = pd.read_csv(io.BytesIO(contents))
    elif name.endswith(".xls") or name.endswith(".xlsx"):
        df = pd.read_excel(io.BytesIO(contents))
    else:
        raise HTTPException(status_code=400, detail="Unsupported file")

    total_records = len(df)
    cleaned_df = df.drop_duplicates()
    duplicate_records = total_records - len(cleaned_df)
    upload_id = int(time.time())

    with engine.begin() as conn:
        for _, row in cleaned_df.iterrows():
            conn.execute(
                text("""
                    INSERT INTO cleaned_data (upload_id, row_data)
                    VALUES (:uid, :data)
                """),
                {
                    "uid": upload_id,
                    "data": json.dumps(clean_nan(row.to_dict()), default=str)
                }
            )

        conn.execute(
            text("""
                INSERT INTO upload_log
                (upload_id, category_id, filename,
                 total_records, duplicate_records,
                 failed_records, status, created_by_user_id)
                VALUES
                (:uid, :cid, :f, :t, :d, 0, 'SUCCESS', :user_id)
            """),
            {
                "uid": upload_id,
                "cid": category_id,
                "f": file.filename,
                "t": total_records,
                "d": duplicate_records,
                "user_id": user["id"]
            }
        )

    log_to_csv(file.filename, total_records, duplicate_records, 0, "SUCCESS")
    return {"success": True}


# ---------------- UPLOAD LIST ----------------
@app.get("/uploads")
def list_uploads(
    category_id: int | None = None,
    filename: str | None = None,
    total_min: int | None = None,
    total_max: int | None = None,
    dup_min: int | None = None,
    dup_max: int | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
    current_user: dict = Depends(get_current_user)
):
    where = []
    params = {}

    if category_id:
        where.append("u.category_id = :cid")
        params["cid"] = category_id

    if filename:
        where.append("LOWER(u.filename) LIKE :fname")
        params["fname"] = f"%{filename.lower()}%"

    if total_min is not None:
        where.append("u.total_records >= :tmin")
        params["tmin"] = total_min

    if total_max is not None:
        where.append("u.total_records <= :tmax")
        params["tmax"] = total_max

    if dup_min is not None:
        where.append("u.duplicate_records >= :dmin")
        params["dmin"] = dup_min

    if dup_max is not None:
        where.append("u.duplicate_records <= :dmax")
        params["dmax"] = dup_max

    if date_from:
        where.append("u.uploaded_at::date >= :df")
        params["df"] = date_from

    if date_to:
        where.append("u.uploaded_at::date <= :dt")
        params["dt"] = date_to

    where_sql = "WHERE " + " AND ".join(where) if where else ""

    with engine.begin() as conn:
        rows = conn.execute(
            text(f"""
                SELECT u.upload_id, u.filename,
                       u.total_records, u.duplicate_records,
                       u.failed_records, u.status,
                       u.uploaded_at,
                       u.created_by_user_id,
                       usr.email AS uploaded_by,
                       c.name AS category
                FROM upload_log u
                JOIN categories c ON c.id = u.category_id
                JOIN users usr ON usr.id = u.created_by_user_id
                {where_sql}
                ORDER BY u.uploaded_at DESC
            """),
            params
        ).fetchall()

    return [
        {
            "upload_id": r.upload_id,
            "filename": r.filename,
            "total_records": r.total_records,
            "duplicate_records": r.duplicate_records,
            "failed_records": r.failed_records,
            "status": r.status,
            "uploaded_at": str(r.uploaded_at),
            "category": r.category,
            "created_by_user_id": r.created_by_user_id,
            "uploaded_by": r.uploaded_by
        }
        for r in rows
    ]

@app.get("/my-uploads")
def list_my_uploads(
    current_user: dict = Depends(get_current_user)
):
    with engine.begin() as conn:
        rows = conn.execute(
            text("""
                SELECT u.upload_id, u.filename,
                       u.total_records, u.duplicate_records,
                       u.failed_records, u.status,
                       u.uploaded_at,
                       u.created_by_user_id,
                       c.name AS category
                FROM upload_log u
                JOIN categories c ON c.id = u.category_id
                WHERE u.created_by_user_id = :uid
                ORDER BY u.uploaded_at DESC
            """),
            {"uid": current_user["id"]}
        ).fetchall()

    return [
        {
            "upload_id": r.upload_id,
            "filename": r.filename,
            "total_records": r.total_records,
            "duplicate_records": r.duplicate_records,
            "failed_records": r.failed_records,
            "status": r.status,
            "uploaded_at": str(r.uploaded_at),
            "category": r.category,
            "created_by_user_id": r.created_by_user_id
        }
        for r in rows
    ]

# ---------------- PREVIEW ----------------
@app.get("/preview")
def preview_data(
    upload_id: int,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    user: dict = Depends(get_current_user)
):
    offset = (page - 1) * page_size

    with engine.begin() as conn:
        total = conn.execute(
            text("SELECT COUNT(*) FROM cleaned_data WHERE upload_id=:uid"),
            {"uid": upload_id}
        ).scalar()

        rows = conn.execute(
            text("""
                SELECT row_data
                FROM cleaned_data
                WHERE upload_id=:uid
                ORDER BY id
                LIMIT :l OFFSET :o
            """),
            {"uid": upload_id, "l": page_size, "o": offset}
        ).fetchall()

    if not rows:
        return {"columns": [], "rows": [], "total_records": total}

    data = [r.row_data for r in rows]
    return {
        "columns": list(data[0].keys()),
        "rows": [list(d.values()) for d in data],
        "total_records": total
    }

@app.get("/admin/users")
def list_users(current_user: dict = Depends(get_current_user)):
    if current_user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin only")

    with engine.begin() as conn:
        rows = conn.execute(
            text("""
                SELECT id, email, role, is_active
                FROM users
                ORDER BY email
            """)
        ).fetchall()

    return [
        {
            "id": r.id,
            "email": r.email,
            "role": r.role,
            "is_active": r.is_active
        }
        for r in rows
    ]

@app.post("/admin/users/{user_id}/reset-password")
def reset_user_password(
    user_id: int,
    new_password: str = Query(...),
    current_user: dict = Depends(get_current_user)
):
    if current_user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin only")

    password_hash = hash_password(new_password)

    with engine.begin() as conn:
        res = conn.execute(
            text("""
                UPDATE users
                SET password_hash = :ph
                WHERE id = :uid
            """),
            {"ph": password_hash, "uid": user_id}
        )

    if res.rowcount == 0:
        raise HTTPException(status_code=404, detail="User not found")

    return {"success": True}

# ---------------- EXPORT ----------------
@app.get("/export")
def export_data(
    upload_id: int,
    user: dict = Depends(get_current_user)
):
    with engine.begin() as conn:
        rows = conn.execute(
            text("""
                SELECT row_data
                FROM cleaned_data
                WHERE upload_id=:uid
                ORDER BY id
            """),
            {"uid": upload_id}
        ).fetchall()

    if not rows:
        raise HTTPException(status_code=404)

    df = pd.DataFrame([r.row_data for r in rows])
    stream = io.StringIO()
    df.to_csv(stream, index=False)
    stream.seek(0)

    return StreamingResponse(
        stream,
        media_type="text/csv",
        headers={
            "Content-Disposition":
            f"attachment; filename=cleaned_{upload_id}.csv"
        }
    )

# ---------------- DELETE ----------------
@app.delete("/upload/{upload_id}")
def delete_upload(
    upload_id: int,
    user: dict = Depends(get_current_user)
):
    with engine.begin() as conn:
        owner = conn.execute(
            text("""
                SELECT created_by_user_id
                FROM upload_log
                WHERE upload_id=:uid
            """),
            {"uid": upload_id}
        ).scalar()

        if owner is None:
            raise HTTPException(status_code=404)

        if not can_delete_upload(user, owner):
            raise HTTPException(status_code=403, detail="Not allowed")

        conn.execute(
            text("DELETE FROM cleaned_data WHERE upload_id=:uid"),
            {"uid": upload_id}
        )
        conn.execute(
            text("DELETE FROM upload_log WHERE upload_id=:uid"),
            {"uid": upload_id}
        )

    return {"success": True}

@app.post("/categories")
def create_category(
    name: str = Query(...),
    current_user: dict = Depends(get_current_user)
):
    if current_user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin only")

    with engine.begin() as conn:
        try:
            conn.execute(
                text("INSERT INTO categories (name) VALUES (:n)"),
                {"n": name.strip()}
            )
        except:
            raise HTTPException(status_code=400, detail="Category already exists")

    return {"success": True}

# ---------------- STATIC ----------------
@app.get("/")
def serve_upload():
    return FileResponse(os.path.join("..", "frontend", "upload.html"))

@app.get("/preview.html")
def serve_preview():
    return FileResponse(os.path.join("..", "frontend", "preview.html"))

@app.get("/me")
def me(current_user: dict = Depends(get_current_user)):
    return {
        "id": current_user["id"],
        "email": current_user["email"],
        "role": current_user["role"]
    }

@app.post("/admin/users")
def create_user(
    email: str = Query(...),
    password: str = Query(...),
    role: str = Query("user"),
    current_user: dict = Depends(get_current_user)
):
    # 🔒 Admin check
    if current_user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin only")

    if role not in ("admin", "user"):
        raise HTTPException(status_code=400, detail="Invalid role")

    password_hash = hash_password(password)

    with engine.begin() as conn:
        try:
            conn.execute(
                text("""
                    INSERT INTO users (email, password_hash, role, is_active)
                    VALUES (:email, :ph, :role, true)
                """),
                {
                    "email": email.lower().strip(),
                    "ph": password_hash,
                    "role": role
                }
            )
        except:
            raise HTTPException(
                status_code=400,
                detail="User already exists"
            )

    return {"success": True}

@app.get("/me")
def me(current_user: dict = Depends(get_current_user)):
    return current_user

@app.get("/users.html")
def serve_users():
    return FileResponse(os.path.join("..", "frontend", "users.html"))
