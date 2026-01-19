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
from db import engine, copy_cleaned_data
from logger import log_to_csv
from auth import authenticate_user, create_access_token, get_current_user
from permissions import can_delete_upload, admin_only
from security import hash_password
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas

# ---------------- CANONICAL FIELD KEYWORDS ----------------

EMAIL_KEYWORDS = ["email", "e-mail", "mail"]

PHONE_KEYWORDS = ["phone", "mobile", "contact", "cell"]

NAME_FIELDS = {
    "name", "full_name", "customer_name", "client_name", "first_name", "fullname"
}

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

# ---------------- FIELD NORMALIZATION ----------------

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    # standardize column names
    df.columns = [c.strip().lower() for c in df.columns]
    return df


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensures every row has canonical keys: email, phone, name
    using semantic column matching (real-world safe)
    """

    # normalize column names
    df.columns = [
        c.strip().lower().replace(" ", "_").replace("-", "_")
        for c in df.columns
    ]

    # ensure canonical columns exist
    for col in ["email", "phone", "name"]:
        if col not in df.columns:
            df[col] = None

    def extract_canonical(row):
        email = None
        phone = None
        name = None

        for col, val in row.items():
            if pd.isna(val):
                continue

            col_l = col.lower()

            # EMAIL DETECTION
            if email is None and any(k in col_l for k in EMAIL_KEYWORDS):
                email = str(val).strip().lower()

            # PHONE DETECTION
            if phone is None and any(k in col_l for k in PHONE_KEYWORDS):
                phone = str(val).strip()

            # NAME DETECTION
            if name is None and "name" in col_l:
                name = str(val).strip()

        row["email"] = email
        row["phone"] = phone
        row["name"] = name

        return row

    return df.apply(extract_canonical, axis=1)


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


@app.post("/upload")
async def upload_file(
    category_id: int = Query(...),
    file: UploadFile = File(...),
    user: dict = Depends(get_current_user)
):
    contents = await file.read()
    name = file.filename.lower()

    upload_id = int(time.time())

    total_records = 0
    duplicate_records = 0
    seen_hashes = set()

    # ---------- 1. DROP INDEX ----------
    with engine.connect() as conn:
        conn.execute(text("DROP INDEX IF EXISTS idx_cleaned_data_upload_id"))
        conn.commit()

    # ---------- 2. STREAM FILE SAFELY ----------

    def csv_chunk_reader():
        """
        Try UTF-8 first, fallback to latin1 (Windows CSVs)
        """
        try:
            return pd.read_csv(
                io.BytesIO(contents),
                chunksize=100_000,
                encoding="utf-8"
            )
        except UnicodeDecodeError:
            return pd.read_csv(
                io.BytesIO(contents),
                chunksize=100_000,
                encoding="latin1"
            )

    # CSV handling (streaming)
    if name.endswith(".csv"):

        reader = csv_chunk_reader()

        for chunk in reader:

            total_records += len(chunk)

            chunk = normalize_dataframe(chunk)

            chunk = chunk.drop_duplicates()

            chunk["__hash"] = chunk.astype(str).agg("|".join, axis=1)
            chunk = chunk[~chunk["__hash"].isin(seen_hashes)]

            seen_hashes.update(chunk["__hash"])
            chunk = chunk.drop(columns=["__hash"])

            copy_cleaned_data(engine, upload_id, chunk)


    # Excel handling (cannot stream, but still fast)
    elif name.endswith(".xls") or name.endswith(".xlsx"):

        df = pd.read_excel(io.BytesIO(contents))

        total_records = len(df)

        df = normalize_dataframe(df)

        df = df.drop_duplicates()

        df["__hash"] = df.astype(str).agg("|".join, axis=1)
        df = df.drop_duplicates(subset="__hash")

        duplicate_records = total_records - len(df)

        df = df.drop(columns=["__hash"])

        copy_cleaned_data(engine, upload_id, df)

    else:
        raise HTTPException(status_code=400, detail="Unsupported file")

    if name.endswith(".csv"):
        duplicate_records = total_records - len(seen_hashes)

    # ---------- 3. RECREATE INDEX ----------
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE INDEX idx_cleaned_data_upload_id
            ON cleaned_data(upload_id)
        """))
        conn.commit()

    # ---------- 4. INSERT UPLOAD LOG ----------
    with engine.connect() as conn:
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
        conn.commit()

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
                SELECT id, row_data
                FROM cleaned_data
                WHERE upload_id=:uid
                ORDER BY id
                LIMIT :l OFFSET :o
            """),
            {"uid": upload_id, "l": page_size, "o": offset}
        ).fetchall()

    if not rows:
        return {"columns": [], "rows": [], "total_records": total}

    return {
        "columns": list(rows[0].row_data.keys()),
        "rows": [
            {
                "id": r.id,
                "values": list(r.row_data.values())
            }
            for r in rows
        ],
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
    format: str = Query("csv", regex="^(csv|excel)$"),
    user: dict = Depends(get_current_user)
):
    with engine.begin() as conn:
        rows = conn.execute(
            text("""
                SELECT row_data
                FROM cleaned_data
                WHERE upload_id = :uid
                ORDER BY id
            """),
            {"uid": upload_id}
        ).fetchall()

    if not rows:
        raise HTTPException(status_code=404, detail="No data found")

    df = pd.DataFrame([r.row_data for r in rows])

    # ---------- CSV ----------
    if format == "csv":
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

    # ---------- EXCEL ----------
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Cleaned Data")
    output.seek(0)

    return StreamingResponse(
        output,
        media_type=(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ),
        headers={
            "Content-Disposition":
            f"attachment; filename=cleaned_{upload_id}.xlsx"
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

@app.put("/categories/{category_id}")
def rename_category(
    category_id: int,
    name: str = Query(...),
    current_user: dict = Depends(get_current_user)
):
    if current_user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin only")

    with engine.begin() as conn:
        res = conn.execute(
            text("""
                UPDATE categories
                SET name = :name
                WHERE id = :cid
            """),
            {"name": name.strip(), "cid": category_id}
        )

        if res.rowcount == 0:
            raise HTTPException(status_code=404, detail="Category not found")

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

@app.delete("/categories/{category_id}")
def delete_category(
    category_id: int,
    current_user: dict = Depends(get_current_user)
):
    if current_user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin only")

    with engine.begin() as conn:
        count = conn.execute(
            text("""
                SELECT COUNT(*)
                FROM upload_log
                WHERE category_id = :cid
            """),
            {"cid": category_id}
        ).scalar()

        if count > 0:
            raise HTTPException(
                status_code=400,
                detail="Category has uploads"
            )

        res = conn.execute(
            text("DELETE FROM categories WHERE id = :cid"),
            {"cid": category_id}
        )

        if res.rowcount == 0:
            raise HTTPException(status_code=404, detail="Category not found")

    return {"success": True}

# ---------------- RELATED RECORDS (FILE SCOPED) ----------------

@app.get("/related-records")
def related_records(
    upload_id: int,
    row_id: int,
    user: dict = Depends(get_current_user)
):
    with engine.begin() as conn:

        # Step 1: Get base record identifiers
        base = conn.execute(
            text("""
                SELECT
                    row_data->>'email' AS email,
                    row_data->>'phone' AS phone
                FROM cleaned_data
                WHERE id = :rid
                  AND upload_id = :uid
            """),
            {"rid": row_id, "uid": upload_id}
        ).fetchone()

        if not base:
            raise HTTPException(status_code=404, detail="Record not found")

        email = base.email
        phone = base.phone

        # Step 2: Find related records inside SAME file
        rows = conn.execute(
            text("""
                SELECT id, row_data
                FROM cleaned_data
                WHERE upload_id = :uid
                  AND (
                        (:email IS NOT NULL AND row_data->>'email' = :email)
                     OR (:phone IS NOT NULL AND row_data->>'phone' = :phone)
                  )
                ORDER BY id
            """),
            {
                "uid": upload_id,
                "email": email,
                "phone": phone
            }
        ).fetchall()

    return {
        "match_email": email,
        "match_phone": phone,
        "total": len(rows),
        "records": [
            {"id": r.id, "data": r.row_data}
            for r in rows
        ]
    }

# ---------------- RELATED RECORDS SUMMARY ----------------

@app.get("/related-summary")
def related_summary(
    upload_id: int,
    user: dict = Depends(get_current_user)
):
    with engine.begin() as conn:
        rows = conn.execute(
            text("""
                WITH base AS (
                    SELECT id,
                           row_data,
                           lower(trim(row_data->>'email')) AS email,
                           regexp_replace(row_data->>'phone', '\\D', '', 'g') AS phone,
                           lower(trim(row_data->>'name')) AS name
                    FROM cleaned_data
                    WHERE upload_id = :uid
                ),
                related_keys AS (
                    SELECT email, phone, name
                    FROM base
                    GROUP BY email, phone, name
                    HAVING
                        COUNT(*) > 1
                        OR email IN (
                            SELECT email FROM base
                            GROUP BY email HAVING COUNT(*) > 1
                        )
                        OR phone IN (
                            SELECT phone FROM base
                            GROUP BY phone HAVING COUNT(*) > 1
                        )
                )
                SELECT b.id, b.row_data
                FROM base b
                JOIN related_keys r
                  ON (
                        b.email IS NOT NULL AND b.email = r.email
                     OR b.phone IS NOT NULL AND b.phone = r.phone
                     OR b.name IS NOT NULL AND b.name = r.name
                  )
                ORDER BY b.id
            """),
            {"uid": upload_id}
        ).fetchall()

    return {
        "total": len(rows),
        "records": [{"id": r.id, "data": r.row_data} for r in rows]
    }

# ---------------- RELATED RECORD SEARCH ----------------

@app.get("/related-search")
def related_search(
    upload_id: int,
    value: str,
    user: dict = Depends(get_current_user)
):
    with engine.begin() as conn:
        rows = conn.execute(
            text("""
                SELECT id, row_data
                FROM cleaned_data
                WHERE upload_id = :uid
                  AND (
                        LOWER(row_data->>'email') = LOWER(:val)
                     OR row_data->>'phone' = :val
                  )
                ORDER BY id
            """),
            {"uid": upload_id, "val": value}
        ).fetchall()

    return {
        "total": len(rows),
        "records": [{"id": r.id, "data": r.row_data} for r in rows]
    }
