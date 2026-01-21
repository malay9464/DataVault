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

def detect_relation_fields(conn, upload_id: int):
    stats = conn.execute(
        text("""
            SELECT
                COUNT(*) AS total,
                COUNT(row_data->>'email') AS email_cnt,
                COUNT(row_data->>'phone') AS phone_cnt
            FROM cleaned_data
            WHERE upload_id = :uid
        """),
        {"uid": upload_id}
    ).fetchone()

    fields = []

    if stats.total == 0:
        return fields

    if stats.email_cnt / stats.total >= 0.05:
        fields.append("email")

    if stats.phone_cnt / stats.total >= 0.05:
        fields.append("phone")

    return fields

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
    # Normalize column names once
    df.columns = [
        c.strip().lower().replace(" ", "_").replace("-", "_")
        for c in df.columns
    ]

    email_cols = [
    c for c in df.columns
    if any(k in c for k in ["email", "mail"])
    ]
    phone_cols = [c for c in df.columns if any(k in c for k in ["phone", "mobile", "contact"])]
    name_cols  = [c for c in df.columns if "name" in c]

    def clean_email(v):
        if pd.isna(v): return None
        return str(v).strip().lower()

    def clean_phone(v):
        if pd.isna(v): return None
        return "".join(c for c in str(v) if c.isdigit())

    def clean_name(v):
        if pd.isna(v): return None
        return str(v).strip().lower()

    df["email"] = (
        df[email_cols]
        .apply(lambda r: next((clean_email(v) for v in r if pd.notna(v)), None), axis=1)
        if email_cols else None
    )

    df["phone"] = (
        df[phone_cols]
        .apply(lambda r: next((clean_phone(v) for v in r if pd.notna(v)), None), axis=1)
        if phone_cols else None
    )

    df["name"] = (
        df[name_cols]
        .apply(lambda r: next((clean_name(v) for v in r if pd.notna(v)), None), axis=1)
        if name_cols else None
    )

    return df



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
    original_filename = file.filename
    name = original_filename.lower()  # ONLY for extension checks

    # ---------- FILENAME DUPLICATE CHECK (CASE-SENSITIVE) ----------
    with engine.begin() as conn:
        exists = conn.execute(
            text("""
                SELECT 1
                FROM upload_log
                WHERE filename = :fname
                LIMIT 1
            """),
            {"fname": original_filename}
        ).scalar()

    if exists:
        raise HTTPException(
            status_code=409,
            detail="File with the same name already exists"
        )

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

# Add these routes in your main.py with the other static file routes

@app.get("/related.html")
def serve_related():
    return FileResponse(os.path.join("..", "frontend", "related.html"))

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

# Add this route in your main.py with the other static file routes
@app.get("/related.html")
def serve_related():
    return FileResponse(os.path.join("..", "frontend", "related.html"))

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
@app.get("/related-summary")
def related_summary(
    upload_id: int,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    user: dict = Depends(get_current_user)
):
    offset = (page - 1) * page_size

    with engine.begin() as conn:
        # 1. Detect usable fields
        fields = detect_relation_fields(conn, upload_id)

        if not fields:
            return {
                "relation_fields": [],
                "total_records": 0,
                "page": page,
                "page_size": page_size,
                "records": [],
                "note": "No relational identifiers found in this dataset"
            }

        conditions = []

        if "email" in fields:
            conditions.append("""
                row_data->>'email' IN (
                    SELECT row_data->>'email'
                    FROM cleaned_data
                    WHERE upload_id = :uid
                      AND row_data->>'email' IS NOT NULL
                      AND row_data->>'email' != ''
                    GROUP BY row_data->>'email'
                    HAVING COUNT(*) > 1
                )
            """)

        if "phone" in fields:
            conditions.append("""
                row_data->>'phone' IN (
                    SELECT row_data->>'phone'
                    FROM cleaned_data
                    WHERE upload_id = :uid
                      AND row_data->>'phone' IS NOT NULL
                      AND row_data->>'phone' != ''
                    GROUP BY row_data->>'phone'
                    HAVING COUNT(*) > 1
                )
            """)

        where_clause = " OR ".join(conditions)

        total = conn.execute(
            text(f"""
                SELECT COUNT(*)
                FROM cleaned_data
                WHERE upload_id = :uid
                  AND ({where_clause})
            """),
            {"uid": upload_id}
        ).scalar()

        rows = conn.execute(
            text(f"""
                SELECT id, row_data
                FROM cleaned_data
                WHERE upload_id = :uid
                  AND ({where_clause})
                ORDER BY id
                LIMIT :limit OFFSET :offset
            """),
            {
                "uid": upload_id,
                "limit": page_size,
                "offset": offset
            }
        ).fetchall()

    return {
        "relation_fields": fields,
        "total_records": total,
        "page": page,
        "page_size": page_size,
        "records": [
            {"id": r.id, "data": r.row_data}
            for r in rows
        ]
    }

@app.get("/related-search")
def related_search(
    upload_id: int,
    value: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    user: dict = Depends(get_current_user)
):
    """
    OPTIMIZED: Search for specific email or phone.
    Uses indexes for instant results even on 10 lakh records.
    """
    
    offset = (page - 1) * page_size
    value = value.strip()
    normalized_phone = ''.join(c for c in value if c.isdigit())
    
    # Build the WHERE clause dynamically to avoid matching empty strings
    where_conditions = ["LOWER(TRIM(row_data->>'email')) = LOWER(:val)"]
    query_params = {"uid": upload_id, "val": value}
    
    # Only add phone condition if we have digits in the search value
    if normalized_phone:
        where_conditions.append("REGEXP_REPLACE(COALESCE(row_data->>'phone', ''), '[^0-9]', '', 'g') = :norm_phone")
        query_params["norm_phone"] = normalized_phone
    
    where_sql = " OR ".join(where_conditions)
    
    with engine.begin() as conn:
        # Count total (fast with indexes)
        total = conn.execute(
            text(f"""
                SELECT COUNT(*)
                FROM cleaned_data
                WHERE upload_id = :uid
                AND ({where_sql})
            """),
            query_params
        ).scalar()
        
        # Get paginated results (fast with indexes)
        rows = conn.execute(
            text(f"""
                SELECT 
                    id,
                    row_data,
                    LOWER(TRIM(row_data->>'email')) AS email,
                    REGEXP_REPLACE(COALESCE(row_data->>'phone', ''), '[^0-9]', '', 'g') AS phone
                FROM cleaned_data
                WHERE upload_id = :uid
                AND ({where_sql})
                ORDER BY id
                LIMIT :limit OFFSET :offset
            """),
            {
                **query_params,
                "limit": page_size,
                "offset": offset
            }
        ).fetchall()

    return {
        "search_value": value,
        "total_records": total,
        "page": page,
        "page_size": page_size,
        "records": [
            {
                "id": r.id,
                "data": r.row_data,
                "matched_email": r.email,
                "matched_phone": r.phone
            }
            for r in rows
        ]
    }


@app.get("/related-stats")
def related_stats(
    upload_id: int,
    user: dict = Depends(get_current_user)
):
    """
    OPTIMIZED: Get statistics using parallel queries and indexes.
    """
    
    with engine.begin() as conn:
        # Use parallel queries for speed
        stats = conn.execute(
            text("""
                WITH email_dups AS (
                    SELECT 
                        LOWER(TRIM(row_data->>'email')) AS email,
                        COUNT(*) AS cnt
                    FROM cleaned_data
                    WHERE upload_id = :uid
                    AND LOWER(TRIM(row_data->>'email')) IS NOT NULL
                    AND LOWER(TRIM(row_data->>'email')) != ''
                    GROUP BY LOWER(TRIM(row_data->>'email'))
                    HAVING COUNT(*) > 1
                ),
                phone_dups AS (
                    SELECT 
                        REGEXP_REPLACE(COALESCE(row_data->>'phone', ''), '[^0-9]', '', 'g') AS phone,
                        COUNT(*) AS cnt
                    FROM cleaned_data
                    WHERE upload_id = :uid
                    AND REGEXP_REPLACE(COALESCE(row_data->>'phone', ''), '[^0-9]', '', 'g') != ''
                    GROUP BY REGEXP_REPLACE(COALESCE(row_data->>'phone', ''), '[^0-9]', '', 'g')
                    HAVING COUNT(*) > 1
                )
                SELECT 
                    (SELECT COUNT(*) FROM email_dups) AS duplicate_emails,
                    (SELECT COALESCE(SUM(cnt), 0) FROM email_dups) AS total_email_records,
                    (SELECT COUNT(*) FROM phone_dups) AS duplicate_phones,
                    (SELECT COALESCE(SUM(cnt), 0) FROM phone_dups) AS total_phone_records
            """),
            {"uid": upload_id}
        ).fetchone()
    
    return {
        "duplicate_emails": stats.duplicate_emails or 0,
        "total_email_records": stats.total_email_records or 0,
        "duplicate_phones": stats.duplicate_phones or 0,
        "total_phone_records": stats.total_phone_records or 0
    }

# Replace the entire /related-grouped endpoint

@app.get("/related-grouped")
def related_grouped(
    upload_id: int,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    user: dict = Depends(get_current_user)
):
    """
    Returns related records GROUPED by email/phone.
    Shows ONLY the COMMON identifier(s) shared by ALL records in the group.
    """
    
    with engine.begin() as conn:
        # Get all duplicate groups (email + phone)
        groups = conn.execute(
            text("""
                WITH normalized AS (
                    SELECT 
                        id,
                        row_data,
                        LOWER(TRIM(row_data->>'email')) AS email,
                        REGEXP_REPLACE(COALESCE(row_data->>'phone', ''), '[^0-9]', '', 'g') AS phone
                    FROM cleaned_data
                    WHERE upload_id = :uid
                ),
                duplicate_emails AS (
                    SELECT email AS match_key, 'email' AS match_type, COUNT(*) AS record_count
                    FROM normalized
                    WHERE email IS NOT NULL AND email != ''
                    GROUP BY email
                    HAVING COUNT(*) > 1
                ),
                duplicate_phones AS (
                    SELECT phone AS match_key, 'phone' AS match_type, COUNT(*) AS record_count
                    FROM normalized
                    WHERE phone IS NOT NULL AND phone != ''
                    GROUP BY phone
                    HAVING COUNT(*) > 1
                ),
                all_duplicates AS (
                    SELECT * FROM duplicate_emails
                    UNION ALL
                    SELECT * FROM duplicate_phones
                )
                SELECT 
                    match_key,
                    match_type,
                    record_count
                FROM all_duplicates
                ORDER BY record_count DESC, match_key
            """),
            {"uid": upload_id}
        ).fetchall()
        
        # Build a mapping of email -> phone and phone -> email to merge groups
        email_to_phones = {}
        phone_to_emails = {}
        
        for group in groups:
            if group.match_type == 'email':
                if group.match_key not in email_to_phones:
                    email_to_phones[group.match_key] = set()
            else:
                if group.match_key not in phone_to_emails:
                    phone_to_emails[group.match_key] = set()
        
        # Find emails and phones that belong together
        for group in groups:
            if group.match_type == 'email':
                email = group.match_key
                phones_for_email = conn.execute(
                    text("""
                        SELECT DISTINCT REGEXP_REPLACE(COALESCE(row_data->>'phone', ''), '[^0-9]', '', 'g') AS phone
                        FROM cleaned_data
                        WHERE upload_id = :uid
                        AND LOWER(TRIM(row_data->>'email')) = :email
                        AND REGEXP_REPLACE(COALESCE(row_data->>'phone', ''), '[^0-9]', '', 'g') != ''
                    """),
                    {"uid": upload_id, "email": email}
                ).fetchall()
                for p in phones_for_email:
                    if p.phone:
                        email_to_phones[email].add(p.phone)
                        if p.phone not in phone_to_emails:
                            phone_to_emails[p.phone] = set()
                        phone_to_emails[p.phone].add(email)
            else:
                phone = group.match_key
                emails_for_phone = conn.execute(
                    text("""
                        SELECT DISTINCT LOWER(TRIM(row_data->>'email')) AS email
                        FROM cleaned_data
                        WHERE upload_id = :uid
                        AND REGEXP_REPLACE(COALESCE(row_data->>'phone', ''), '[^0-9]', '', 'g') = :phone
                        AND LOWER(TRIM(row_data->>'email')) IS NOT NULL
                        AND LOWER(TRIM(row_data->>'email')) != ''
                    """),
                    {"uid": upload_id, "phone": phone}
                ).fetchall()
                for e in emails_for_phone:
                    if e.email:
                        phone_to_emails[phone].add(e.email)
                        if e.email not in email_to_phones:
                            email_to_phones[e.email] = set()
                        email_to_phones[e.email].add(phone)
        
        # Merge groups: use Union-Find approach
        merged_groups = {}
        visited_emails = set()
        visited_phones = set()
        
        for email in email_to_phones:
            if email in visited_emails:
                continue
            
            email_cluster = set()
            phone_cluster = set()
            queue = [('email', email)]
            
            while queue:
                typ, val = queue.pop(0)
                
                if typ == 'email':
                    if val in visited_emails:
                        continue
                    visited_emails.add(val)
                    email_cluster.add(val)
                    
                    for phone in email_to_phones.get(val, []):
                        if phone not in visited_phones:
                            queue.append(('phone', phone))
                
                else:  # phone
                    if val in visited_phones:
                        continue
                    visited_phones.add(val)
                    phone_cluster.add(val)
                    
                    for email_val in phone_to_emails.get(val, []):
                        if email_val not in visited_emails:
                            queue.append(('email', email_val))
            
            group_key = f"merged_{len(merged_groups)}"
            merged_groups[group_key] = (email_cluster, phone_cluster)
        
        for phone in phone_to_emails:
            if phone in visited_phones:
                continue
            
            email_cluster = set()
            phone_cluster = set()
            queue = [('phone', phone)]
            
            while queue:
                typ, val = queue.pop(0)
                
                if typ == 'phone':
                    if val in visited_phones:
                        continue
                    visited_phones.add(val)
                    phone_cluster.add(val)
                    
                    for email_val in phone_to_emails.get(val, []):
                        if email_val not in visited_emails:
                            queue.append(('email', email_val))
                
                else:  # email
                    if val in visited_emails:
                        continue
                    visited_emails.add(val)
                    email_cluster.add(val)
                    
                    for phone_val in email_to_phones.get(val, []):
                        if phone_val not in visited_phones:
                            queue.append(('phone', phone_val))
            
            group_key = f"merged_{len(merged_groups)}"
            merged_groups[group_key] = (email_cluster, phone_cluster)
        
        # Sort by cluster size (descending)
        sorted_groups = sorted(
            merged_groups.items(),
            key=lambda x: len(x[1][0]) + len(x[1][1]),
            reverse=True
        )
        
        # FIRST: Build all result groups (before pagination)
        all_result_groups = []
        for group_key, (emails, phones) in sorted_groups:
            # Get all records matching any email or phone in this cluster
            records = conn.execute(
                text("""
                    SELECT id, row_data
                    FROM cleaned_data
                    WHERE upload_id = :uid
                    AND (
                        LOWER(TRIM(row_data->>'email')) = ANY(:emails)
                        OR REGEXP_REPLACE(COALESCE(row_data->>'phone', ''), '[^0-9]', '', 'g') = ANY(:phones)
                    )
                    ORDER BY id
                """),
                {
                    "uid": upload_id,
                    "emails": list(emails) if emails else [None],
                    "phones": list(phones) if phones else [None]
                }
            ).fetchall()
            
            if not records:
                continue
            
            # Extract the COMMON identifiers shared by ALL records in this group
            common_emails = set()
            common_phones = set()
            
            # Get all unique emails in this group
            group_emails = set()
            group_phones = set()
            
            for record in records:
                email = record.row_data.get("email")
                phone = record.row_data.get("phone")
                
                if email and email.strip():
                    group_emails.add(email.strip().lower())
                if phone and phone.strip():
                    normalized_phone = ''.join(c for c in phone if c.isdigit())
                    if normalized_phone:
                        group_phones.add(normalized_phone)
            
            # Find COMMON identifiers (present in ALL records)
            if len(records) > 0:
                # Check if all records share the same email
                if len(group_emails) == 1:
                    common_emails = group_emails
                
                # Check if all records share the same phone
                if len(group_phones) == 1:
                    common_phones = group_phones
            
            # SKIP THIS GROUP if NO common identifier exists
            if not common_emails and not common_phones:
                continue
            
            # Format match key with ONLY common identifiers
            match_display = []
            if common_emails:
                match_display.extend([f"📧 {e}" for e in sorted(common_emails)])
            if common_phones:
                match_display.extend([f"📱 {p}" for p in sorted(common_phones)])
            
            match_type = 'merged' if (common_emails and common_phones) else ('email' if common_emails else 'phone')
            match_key = " | ".join(match_display)
            
            all_result_groups.append({
                "match_key": match_key,
                "match_type": match_type,
                "record_count": len(records),
                "records": [
                    {"id": r.id, "data": r.row_data}
                    for r in records
                ]
            })
        
        # SECOND: Apply pagination on filtered results
        total_groups = len(all_result_groups)
        offset = (page - 1) * page_size
        paginated_groups = all_result_groups[offset:offset + page_size]
    
    return {
        "total_groups": total_groups,
        "page": page,
        "page_size": page_size,
        "groups": paginated_groups
    }