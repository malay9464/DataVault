from fastapi import (
    FastAPI, UploadFile, File, Query,
    HTTPException, Depends
)
from header import (
    detect_header_case,
    get_column_samples,
    apply_user_headers,
    normalize_header_name
)
import tempfile
from pydantic import BaseModel
from typing import Dict, Optional
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

class HeaderResolutionRequest(BaseModel):
    user_mapping: Dict[int, str] = {}
    first_row_is_data: bool = False

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

    email_cols = []
    for c in df.columns:
        if c in ["email", "e_mail", "mail", "email_address"]:
            email_cols = [c]  # Use exact match
            break
    if not email_cols:
        email_cols = [c for c in df.columns if "email" in c or c == "mail"]
    
    # Phone: look for exact matches first (INCLUDING contact fields)
    phone_cols = []
    for c in df.columns:
        if c in ["phone", "phone_no", "phone_number", "mobile", "mobile_no", "mobile_number", "contact", "contact_no", "contact_number"]:
            phone_cols = [c]  # Use exact match
            break
    if not phone_cols:
        phone_cols = [c for c in df.columns if "phone" in c or "mobile" in c or "contact" in c]
    
    # Name: look for exact matches first
    name_cols = []
    for c in df.columns:
        if c in ["name", "full_name", "fullname", "customer_name", "client_name"]:
            name_cols = [c]
            break
    if not name_cols:
        name_cols = [c for c in df.columns if "name" in c]

    def clean_email(v):
        if pd.isna(v): return None
        return str(v).strip().lower()

    def clean_phone(v):
        if pd.isna(v): return None
        return "".join(c for c in str(v) if c.isdigit())

    def clean_name(v):
        if pd.isna(v): return None
        return str(v).strip().lower()

    # Create normalized columns
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

    # ✅ Drop ONLY the columns that were used for canonical fields
    columns_to_drop = set(email_cols + phone_cols + name_cols)
    
    # Don't drop the normalized columns if they already existed
    columns_to_drop.discard("email")
    columns_to_drop.discard("phone")
    columns_to_drop.discard("name")
    
    df = df.drop(columns=list(columns_to_drop))

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

    upload_id = int(time.time() * 1000000)

    # Read first chunk/sheet to detect headers
    if name.endswith(".csv"):
        try:
            preview_df = pd.read_csv(io.BytesIO(contents), nrows=100, encoding="utf-8")
        except UnicodeDecodeError:
            preview_df = pd.read_csv(io.BytesIO(contents), nrows=100, encoding="latin1")
    elif name.endswith(".xls") or name.endswith(".xlsx"):
        preview_df = pd.read_excel(io.BytesIO(contents), nrows=100)
    else:
        raise HTTPException(status_code=400, detail="Unsupported file")

    # Detect header case
    case_type, metadata = detect_header_case(preview_df)
    
    # Get column samples for user review
    column_samples = get_column_samples(preview_df)
    
    # Store original headers
    original_headers = {
        'columns': [str(c) for c in preview_df.columns],
        'case_type': case_type,
        'metadata': metadata,
        'samples': column_samples
    }
  
    if case_type in ['missing', 'suspicious']:
        
        # 1. Save file to temp storage
        temp_dir = tempfile.gettempdir()
        temp_file_path = os.path.join(temp_dir, f"datavault_{upload_id}.data")
        temp_meta_path = os.path.join(temp_dir, f"datavault_{upload_id}.meta")
        
        with open(temp_file_path, 'wb') as f:
            f.write(contents)
        
        # 2. Save metadata separately (NO DATABASE WRITE)
        temp_metadata = {
            'upload_id': upload_id,
            'category_id': category_id,
            'filename': original_filename,
            'user_id': user["id"],
            'created_at': time.time(),
            'original_headers': original_headers
        }
        
        with open(temp_meta_path, 'w') as f:
            json.dump(temp_metadata, f)
        
        # 3. Return pending status WITHOUT creating DB record
        return {
            "success": False,
            "status": "pending_headers",
            "upload_id": upload_id,
            "case_type": case_type,
            "message": "Headers need review" if case_type == 'missing' else "First row might be data",
            "headers": original_headers
        }
       
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
    
    # Store final headers for valid case
    final_headers = {'columns': [str(c) for c in preview_df.columns]}
    
    with engine.connect() as conn:
        conn.execute(
            text("""
                INSERT INTO upload_log
                (upload_id, category_id, filename,
                 total_records, duplicate_records,
                 failed_records, status, created_by_user_id,
                 header_status, original_headers, final_headers, 
                 header_resolution_type)
                VALUES
                (:uid, :cid, :f, :t, :d, 0, 'SUCCESS', :user_id,
                 'no_issue', :orig, :final, 'original')
            """),
            {
                "uid": upload_id,
                "cid": category_id,
                "f": file.filename,
                "t": total_records,
                "d": duplicate_records,
                "user_id": user["id"],
                "orig": json.dumps(original_headers),
                "final": json.dumps(final_headers)
            }
        )
        conn.commit()

    log_to_csv(file.filename, total_records, duplicate_records, 0, "SUCCESS")
    
    return {
        "success": True,
        "upload_id": upload_id
    }

@app.get("/upload/{upload_id}/headers")
def get_upload_headers(
    upload_id: int,
    user: dict = Depends(get_current_user)
):
    """
    Get header information for a pending upload (NOT YET IN DATABASE).
    Reads from temporary storage.
    """
    temp_dir = tempfile.gettempdir()
    temp_meta_path = os.path.join(temp_dir, f"datavault_{upload_id}.meta")
    
    if not os.path.exists(temp_meta_path):
        raise HTTPException(
            status_code=404, 
            detail="Upload session expired or not found. Please re-upload."
        )
    
    # Read metadata from temp storage
    with open(temp_meta_path, 'r') as f:
        metadata = json.load(f)
    
    # Verify user ownership
    if metadata['user_id'] != user["id"] and user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Not authorized")
    
    return {
        "upload_id": upload_id,
        "filename": metadata['filename'],
        "header_info": metadata['original_headers']
    }

@app.post("/upload/{upload_id}/resolve-headers")
async def resolve_headers(
    upload_id: int,
    request: HeaderResolutionRequest,
    user: dict = Depends(get_current_user)
):
    """
    User submits header corrections.
    
    CRITICAL: Two-mode ingestion
    - RAW mode: User resolved headers → NO normalization
    - NORMALIZED mode: No user intervention → Apply normalization
    """
    
    user_mapping = request.user_mapping
    first_row_is_data = request.first_row_is_data
    
    temp_dir = tempfile.gettempdir()
    temp_file_path = os.path.join(temp_dir, f"datavault_{upload_id}.data")
    temp_meta_path = os.path.join(temp_dir, f"datavault_{upload_id}.meta")
    
    if not os.path.exists(temp_file_path) or not os.path.exists(temp_meta_path):
        raise HTTPException(
            status_code=400,
            detail="Upload session expired. Please re-upload the file."
        )
    
    # Read metadata
    with open(temp_meta_path, 'r') as f:
        metadata = json.load(f)
    
    # Verify user ownership
    if metadata['user_id'] != user["id"] and user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Not authorized")
    
    filename = metadata['filename']
    category_id = metadata['category_id']
    original_headers_json = metadata['original_headers']
    
    # Read file contents
    with open(temp_file_path, 'rb') as f:
        contents = f.read()
    
    name = filename.lower()
    
    # Determine header parameter
    header_param = None if first_row_is_data else 0
    
    # Get total number of columns
    if name.endswith(".csv"):
        try:
            sample_df = pd.read_csv(io.BytesIO(contents), nrows=1, encoding="utf-8", header=header_param)
        except UnicodeDecodeError:
            sample_df = pd.read_csv(io.BytesIO(contents), nrows=1, encoding="latin1", header=header_param)
    elif name.endswith(".xls") or name.endswith(".xlsx"):
        sample_df = pd.read_excel(io.BytesIO(contents), nrows=1, header=header_param)
    else:
        raise HTTPException(status_code=400, detail="Unsupported file")
    
    total_columns = len(sample_df.columns)
    
    # Build final column names
    final_column_names = []
    for idx in range(total_columns):
        if idx in user_mapping and user_mapping[idx].strip():
            final_column_names.append(user_mapping[idx].strip().lower().replace(' ', '_'))
        else:
            final_column_names.append(f'unnamed_{idx}')
    
    # Store final headers
    final_headers = {
        'columns': final_column_names,
        'user_mapping': user_mapping,
        'first_row_was_data': first_row_is_data
    }
    
    user_resolved = (len(user_mapping) > 0 or first_row_is_data)
    
    if user_resolved:
        ingestion_mode = 'raw'
        resolution_type = 'first_row_corrected' if first_row_is_data else 'user_assigned_partial'
    else:
        ingestion_mode = 'normalized'
        resolution_type = 'original'

    # Initialize counters
    total_records = 0
    duplicate_records = 0
    seen_hashes = set()

    # Drop index
    with engine.connect() as conn:
        conn.execute(text("DROP INDEX IF EXISTS idx_cleaned_data_upload_id"))
        conn.commit()

    if name.endswith(".csv"):
        try:
            reader = pd.read_csv(
                io.BytesIO(contents), 
                chunksize=100_000, 
                encoding="utf-8",
                header=header_param
            )
        except UnicodeDecodeError:
            reader = pd.read_csv(
                io.BytesIO(contents), 
                chunksize=100_000, 
                encoding="latin1",
                header=header_param
            )
        
        for chunk in reader:
            # Apply final column names
            chunk.columns = final_column_names
            
            total_records += len(chunk)
            
            if ingestion_mode == 'normalized':
                # NORMALIZED MODE: Apply auto-normalization
                chunk = normalize_dataframe(chunk)
            # else: RAW MODE - NO normalization, preserve ALL columns
            
            # Deduplication (works on current columns, whatever they are)
            chunk = chunk.drop_duplicates()
            chunk["__hash"] = chunk.astype(str).agg("|".join, axis=1)
            chunk = chunk[~chunk["__hash"].isin(seen_hashes)]
            seen_hashes.update(chunk["__hash"])
            chunk = chunk.drop(columns=["__hash"])
            
            copy_cleaned_data(engine, upload_id, chunk)

        duplicate_records = total_records - len(seen_hashes)
    
    else:
        if name.endswith(".xls") or name.endswith(".xlsx"):
            df = pd.read_excel(io.BytesIO(contents), header=header_param)
        else:
            raise HTTPException(status_code=400, detail="Unsupported file")
        
        # Apply final column names
        df.columns = final_column_names
        
        total_records = len(df)
        
        if ingestion_mode == 'normalized':
            # NORMALIZED MODE: Apply auto-normalization
            df = normalize_dataframe(df)
        # else: RAW MODE - NO normalization, preserve ALL columns
        
        # Deduplication (works on current columns, whatever they are)
        df = df.drop_duplicates()
        df["__hash"] = df.astype(str).agg("|".join, axis=1)
        df = df.drop_duplicates(subset="__hash")
        duplicate_records = total_records - len(df)
        df = df.drop(columns=["__hash"])
        
        copy_cleaned_data(engine, upload_id, df)

    # Recreate index
    with engine.connect() as conn:
        conn.execute(text("CREATE INDEX idx_cleaned_data_upload_id ON cleaned_data(upload_id)"))
        conn.commit()

    with engine.connect() as conn:
        conn.execute(
            text("""
                INSERT INTO upload_log
                (upload_id, category_id, filename,
                 total_records, duplicate_records,
                 failed_records, status, created_by_user_id,
                 header_status, original_headers, final_headers, 
                 header_resolution_type, first_row_is_data)
                VALUES
                (:uid, :cid, :f, :t, :d, 0, 'SUCCESS', :user_id,
                 'resolved', :orig, :final, :res_type, :first_data)
            """),
            {
                "uid": upload_id,
                "cid": category_id,
                "f": filename,
                "t": total_records,
                "d": duplicate_records,
                "user_id": metadata['user_id'],
                "orig": json.dumps(original_headers_json),
                "final": json.dumps(final_headers),
                "res_type": resolution_type,
                "first_data": first_row_is_data
            }
        )
        conn.commit()

    # Clean up temp files
    try:
        os.remove(temp_file_path)
        os.remove(temp_meta_path)
    except:
        pass

    log_to_csv(filename, total_records, duplicate_records, 0, "SUCCESS")
    
    return {
        "success": True,
        "upload_id": upload_id,
        "total_records": total_records,
        "duplicate_records": duplicate_records,
        "resolution_type": resolution_type,
        "ingestion_mode": ingestion_mode  # For debugging
    }

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

@app.post("/admin/cleanup-temp-uploads")
def cleanup_abandoned_uploads(user: dict = Depends(admin_only)):
    """
    Delete temporary upload files older than 1 hour.
    Should be called by a cron job or manually by admin.
    """
    import glob
    
    temp_dir = tempfile.gettempdir()
    pattern = os.path.join(temp_dir, "datavault_*")
    
    deleted_count = 0
    current_time = time.time()
    max_age = 3600  # 1 hour in seconds
    
    for filepath in glob.glob(pattern):
        try:
            file_age = current_time - os.path.getmtime(filepath)
            if file_age > max_age:
                os.remove(filepath)
                deleted_count += 1
        except Exception as e:
            print(f"Error deleting {filepath}: {e}")
    
    return {
        "success": True,
        "deleted_files": deleted_count,
        "message": f"Cleaned up {deleted_count} abandoned upload files"
    }

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
    sort_column: str = Query(None),
    sort_direction: str = Query("asc", regex="^(asc|desc)$"),
    user: dict = Depends(get_current_user)
):
    offset = (page - 1) * page_size

    with engine.begin() as conn:
        total = conn.execute(
            text("SELECT COUNT(*) FROM cleaned_data WHERE upload_id=:uid"),
            {"uid": upload_id}
        ).scalar()

        # Build ORDER BY clause
        order_by = "ORDER BY id"
        if sort_column:
            # Sanitize column name and build sort clause
            direction = "DESC" if sort_direction == "desc" else "ASC"
            order_by = f"ORDER BY row_data->>'{sort_column}' {direction}"

        rows = conn.execute(
            text(f"""
                SELECT id, row_data
                FROM cleaned_data
                WHERE upload_id=:uid
                {order_by}
                LIMIT :l OFFSET :o
            """),
            {"uid": upload_id, "l": page_size, "o": offset}
        ).fetchall()

    if not rows:
        return {"columns": [], "rows": [], "total_records": total}

    excluded_prefixes = ["original_", "raw_"]
    all_columns = list(rows[0].row_data.keys())
    normalized_columns = [
        col for col in all_columns 
        if not any(col.startswith(prefix) for prefix in excluded_prefixes)
    ]

    return {
        "columns": normalized_columns,
        "rows": [
            {
                "id": r.id,
                "values": [r.row_data.get(col) for col in normalized_columns]
            }
            for r in rows
        ],
        "total_records": total
    }

@app.get("/search")
def search_data(
    upload_id: int,
    query: str = Query(..., min_length=1),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    user: dict = Depends(get_current_user)
):
    """
    Server-side search across all columns.
    Much faster than loading all data to client.
    """
    offset = (page - 1) * page_size
    search_term = f"%{query.lower()}%"
    
    with engine.begin() as conn:
        # Get columns from first row
        first_row = conn.execute(
            text("""
                SELECT row_data
                FROM cleaned_data
                WHERE upload_id = :uid
                LIMIT 1
            """),
            {"uid": upload_id}
        ).fetchone()
        
        if not first_row:
            return {"columns": [], "rows": [], "total_records": 0}
        
        columns = list(first_row.row_data.keys())
        
        # Build search condition for all columns
        search_conditions = " OR ".join([
            f"LOWER(CAST(row_data->>'{col}' AS TEXT)) LIKE :search"
            for col in columns
        ])
        
        # Count total matching records
        total = conn.execute(
            text(f"""
                SELECT COUNT(*)
                FROM cleaned_data
                WHERE upload_id = :uid
                AND ({search_conditions})
            """),
            {"uid": upload_id, "search": search_term}
        ).scalar()
        
        # Get paginated results
        rows = conn.execute(
            text(f"""
                SELECT id, row_data
                FROM cleaned_data
                WHERE upload_id = :uid
                AND ({search_conditions})
                ORDER BY id
                LIMIT :limit OFFSET :offset
            """),
            {"uid": upload_id, "search": search_term, "limit": page_size, "offset": offset}
        ).fetchall()
    
    # Filter columns (same as preview)
    excluded_prefixes = ["original_", "raw_"]
    normalized_columns = [
        col for col in columns 
        if not any(col.startswith(prefix) for prefix in excluded_prefixes)
    ]
    
    return {
        "columns": normalized_columns,
        "rows": [
            {
                "id": r.id,
                "values": [r.row_data.get(col) for col in normalized_columns]
            }
            for r in rows
        ],
        "total_records": total,
        "search_query": query
    }

# ---------------- FILE METADATA ----------------
@app.get("/upload-metadata")
def get_upload_metadata(
    upload_id: int,
    user: dict = Depends(get_current_user)
):
    """
    Fetch metadata for a specific upload.
    Returns filename and other relevant info.
    """
    with engine.begin() as conn:
        result = conn.execute(
            text("""
                SELECT upload_id, filename, total_records, uploaded_at
                FROM upload_log
                WHERE upload_id = :uid
            """),
            {"uid": upload_id}
        ).fetchone()
        
        if not result:
            raise HTTPException(status_code=404, detail="Upload not found")
        
        return {
            "upload_id": result.upload_id,
            "filename": result.filename,
            "total_records": result.total_records,
            "uploaded_at": str(result.uploaded_at)
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

@app.get("/related-grouped")
def related_grouped(
    upload_id: int,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    match_type: str = Query("all", regex="^(all|email|phone|merged|both)$"),
    user: dict = Depends(get_current_user)
):
    """
    ✅ FIXED: Groups now match stats exactly
    - Email Only: Shows only email-based groups
    - Phone Only: Shows only phone-based groups  
    - Both (Merged): Shows only email+phone combination groups
    - All Groups: Shows all three types combined
    """
    if match_type == 'both':
        match_type = 'merged'
    
    with engine.begin() as conn:
        query = text("""
            WITH 
            normalized AS (
                SELECT 
                    id,
                    row_data,
                    LOWER(TRIM(row_data->>'email')) AS email,
                    REGEXP_REPLACE(COALESCE(row_data->>'phone', ''), '[^0-9]', '', 'g') AS phone
                FROM cleaned_data
                WHERE upload_id = :uid
            ),
            
            -- Email-based groups
            email_groups AS (
                SELECT 
                    email AS group_key,
                    'email' AS match_type,
                    ARRAY[email] AS emails,
                    NULL::text[] AS phones,
                    COUNT(*) AS record_count,
                    JSON_AGG(JSON_BUILD_OBJECT('id', id, 'data', row_data) ORDER BY id) AS records
                FROM normalized
                WHERE email IS NOT NULL AND email != ''
                GROUP BY email
                HAVING COUNT(*) > 1
            ),
            
            -- Phone-based groups
            phone_groups AS (
                SELECT 
                    phone AS group_key,
                    'phone' AS match_type,
                    NULL::text[] AS emails,
                    ARRAY[phone] AS phones,
                    COUNT(*) AS record_count,
                    JSON_AGG(JSON_BUILD_OBJECT('id', id, 'data', row_data) ORDER BY id) AS records
                FROM normalized
                WHERE phone IS NOT NULL AND phone != ''
                GROUP BY phone
                HAVING COUNT(*) > 1
            ),
            
            -- Merged groups (email + phone combinations)
            merged_groups AS (
                SELECT 
                    email || '_' || phone AS group_key,
                    'merged' AS match_type,
                    ARRAY[email] AS emails,
                    ARRAY[phone] AS phones,
                    COUNT(*) AS record_count,
                    JSON_AGG(JSON_BUILD_OBJECT('id', id, 'data', row_data) ORDER BY id) AS records
                FROM normalized
                WHERE email IS NOT NULL AND email != ''
                  AND phone IS NOT NULL AND phone != ''
                GROUP BY email, phone
                HAVING COUNT(*) > 1
            ),
            
            -- Combine based on filter
            all_groups AS (
                SELECT * FROM email_groups
                WHERE :match_type IN ('all', 'email')
                UNION ALL
                SELECT * FROM phone_groups
                WHERE :match_type IN ('all', 'phone')
                UNION ALL
                SELECT * FROM merged_groups
                WHERE :match_type IN ('all', 'merged')
            ),
            
            -- Rank by record count
            ranked_groups AS (
                SELECT 
                    *,
                    ROW_NUMBER() OVER (ORDER BY record_count DESC, group_key) AS row_num
                FROM all_groups
            )
            
            SELECT 
                emails,
                phones,
                match_type,
                record_count,
                records
            FROM ranked_groups
            WHERE row_num BETWEEN :offset + 1 AND :offset + :limit
            ORDER BY record_count DESC, group_key;
        """)
        
        offset = (page - 1) * page_size
        
        rows = conn.execute(
            query,
            {
                "uid": upload_id,
                "match_type": match_type,
                "offset": offset,
                "limit": page_size
            }
        ).fetchall()
        
        # Count total groups (simplified and accurate)
        count_query = text("""
            WITH 
            normalized AS (
                SELECT 
                    LOWER(TRIM(row_data->>'email')) AS email,
                    REGEXP_REPLACE(COALESCE(row_data->>'phone', ''), '[^0-9]', '', 'g') AS phone
                FROM cleaned_data
                WHERE upload_id = :uid
            ),
            email_group_count AS (
                SELECT COUNT(*) AS cnt
                FROM (
                    SELECT email
                    FROM normalized
                    WHERE email IS NOT NULL AND email != ''
                    GROUP BY email
                    HAVING COUNT(*) > 1
                ) e
            ),
            phone_group_count AS (
                SELECT COUNT(*) AS cnt
                FROM (
                    SELECT phone
                    FROM normalized
                    WHERE phone IS NOT NULL AND phone != ''
                    GROUP BY phone
                    HAVING COUNT(*) > 1
                ) p
            ),
            merged_group_count AS (
                SELECT COUNT(*) AS cnt
                FROM (
                    SELECT email, phone
                    FROM normalized
                    WHERE email IS NOT NULL AND email != ''
                      AND phone IS NOT NULL AND phone != ''
                    GROUP BY email, phone
                    HAVING COUNT(*) > 1
                ) m
            )
            SELECT 
                CASE 
                    WHEN :match_type = 'email' THEN (SELECT cnt FROM email_group_count)
                    WHEN :match_type = 'phone' THEN (SELECT cnt FROM phone_group_count)
                    WHEN :match_type = 'merged' THEN (SELECT cnt FROM merged_group_count)
                    ELSE 
                        (SELECT cnt FROM email_group_count) + 
                        (SELECT cnt FROM phone_group_count) + 
                        (SELECT cnt FROM merged_group_count)
                END AS total;
        """)
        
        total_groups = conn.execute(
            count_query,
            {"uid": upload_id, "match_type": match_type}
        ).scalar() or 0
        
        # Format results
        formatted_groups = []
        for row in rows:
            match_display = []
            if row.emails:
                match_display.extend([f"📧 {e}" for e in row.emails])
            if row.phones:
                match_display.extend([f"📱 {p}" for p in row.phones])
            
            formatted_groups.append({
                "match_key": " | ".join(match_display) if match_display else "Unknown",
                "match_type": row.match_type,
                "record_count": row.record_count,
                "records": row.records
            })
    
    return {
        "total_groups": total_groups,
        "page": page,
        "page_size": page_size,
        "groups": formatted_groups
    }

@app.get("/related-grouped-stats")
def related_grouped_stats(
    upload_id: int,
    user: dict = Depends(get_current_user)
):
    """
    ✅ FIXED: Stats now match exactly what's displayed in /related-grouped
    """
    with engine.begin() as conn:
        stats = conn.execute(
            text("""
                WITH 
                normalized AS (
                    SELECT 
                        id,
                        LOWER(TRIM(row_data->>'email')) AS email,
                        REGEXP_REPLACE(COALESCE(row_data->>'phone', ''), '[^0-9]', '', 'g') AS phone
                    FROM cleaned_data
                    WHERE upload_id = :uid
                ),
                
                -- Email-only groups (email duplicates with no phone duplicates)
                email_groups AS (
                    SELECT 
                        email,
                        COUNT(*) AS record_count
                    FROM normalized
                    WHERE email IS NOT NULL AND email != ''
                    GROUP BY email
                    HAVING COUNT(*) > 1
                ),
                
                -- Phone-only groups (phone duplicates with no email duplicates)
                phone_groups AS (
                    SELECT 
                        phone,
                        COUNT(*) AS record_count
                    FROM normalized
                    WHERE phone IS NOT NULL AND phone != ''
                    GROUP BY phone
                    HAVING COUNT(*) > 1
                ),
                
                -- Merged groups (both email AND phone have duplicates)
                merged_groups AS (
                    SELECT 
                        email,
                        phone,
                        COUNT(*) AS record_count
                    FROM normalized
                    WHERE email IS NOT NULL AND email != ''
                      AND phone IS NOT NULL AND phone != ''
                    GROUP BY email, phone
                    HAVING COUNT(*) > 1
                )
                
                SELECT 
                    (SELECT COUNT(*) FROM email_groups) AS email_groups,
                    (SELECT COALESCE(SUM(record_count), 0) FROM email_groups) AS email_records,
                    (SELECT COUNT(*) FROM phone_groups) AS phone_groups,
                    (SELECT COALESCE(SUM(record_count), 0) FROM phone_groups) AS phone_records,
                    (SELECT COUNT(*) FROM merged_groups) AS both_groups,
                    (SELECT COALESCE(SUM(record_count), 0) FROM merged_groups) AS both_records;
            """),
            {"uid": upload_id}
        ).fetchone()
    
    return {
        "email_groups": stats.email_groups,
        "email_records": stats.email_records,
        "phone_groups": stats.phone_groups,
        "phone_records": stats.phone_records,
        "both_groups": stats.both_groups,
        "both_records": stats.both_records
    }

@app.get("/header.html")
def serve_header():
    return FileResponse(os.path.join("..", "frontend", "header.html"))

@app.get("/upload/{upload_id}/header-metadata")
def get_header_metadata(
    upload_id: int,
    user: dict = Depends(get_current_user)
):
    """
    Get header resolution metadata for auditing.
    Shows original headers, final headers, and resolution type.
    """
    with engine.begin() as conn:
        result = conn.execute(
            text("""
                SELECT 
                    filename,
                    original_headers,
                    final_headers,
                    header_resolution_type,
                    first_row_is_data,
                    header_status
                FROM upload_log
                WHERE upload_id = :uid
            """),
            {"uid": upload_id}
        ).fetchone()
        
        if not result:
            raise HTTPException(status_code=404, detail="Upload not found")
        
        return {
            "upload_id": upload_id,
            "filename": result.filename,
            "original_headers": result.original_headers,
            "final_headers": result.final_headers,
            "resolution_type": result.header_resolution_type,
            "first_row_was_data": result.first_row_is_data,
            "header_status": result.header_status
        }