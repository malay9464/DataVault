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
from typing import Dict, Optional, List
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy import text, asc, desc, func
from datetime import date
import pandas as pd
import io, json, os, time
from users import router as users_router
from db import engine, copy_cleaned_data
from logger import log_to_csv
from auth import authenticate_user, create_access_token, get_current_user
from permissions import can_delete_upload, can_access_upload, admin_only
from security import hash_password
from reportlab.lib.pagesizes import A4
from fastapi.responses import StreamingResponse
from reportlab.pdfgen import canvas
from collections import defaultdict
import asyncio
import hashlib
import python_calamine
from concurrent.futures import ThreadPoolExecutor
import asyncio
import multiprocessing
from auth import SECRET_KEY, ALGORITHM
import jwt as pyjwt
import time as _t
import shutil

multiprocessing.freeze_support()
executor = ThreadPoolExecutor(max_workers=4)

# In-memory progress store: upload_id -> progress dict
upload_progress_store: dict = {}

class HeaderResolutionRequest(BaseModel):
    user_mapping: Dict[int, str] = {}
    first_row_is_data: bool = False

class BulkDeleteRequest(BaseModel):
    upload_ids: List[int]

class MoveUploadRequest(BaseModel):
    category_id: int

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

def assert_upload_access(conn, upload_id: int, user: dict):
    """Centralized ownership check for any upload_id access."""
    owner = conn.execute(
        text("SELECT created_by_user_id FROM upload_log WHERE upload_id = :uid"),
        {"uid": upload_id}
    ).scalar()

    if owner is None:
        raise HTTPException(status_code=404, detail="Upload not found")

    if not can_access_upload(user, owner):
        raise HTTPException(status_code=403, detail="Not authorized")

# ---------------- FIELD NORMALIZATION ----------------

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    # standardize column names
    df.columns = [c.strip().lower() for c in df.columns]
    return df

def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [
        c.strip().lower().replace(" ", "_").replace("-", "_")
        for c in df.columns
    ]

    email_cols = []
    for c in df.columns:
        if c in {"email", "e_mail", "mail", "email_address", "email_id", 
                 "mail_id", "e_mail_id", "e-mail", "e-mail_address", "e-mail_id", 
                 "e-mailid", "emailid", "emailaddress", "e_mail_address"}:
            email_cols = [c]
            break

    phone_cols = []
    for c in df.columns:
        if c in {"phone", "phone_no", "phone_number", "mobile", "mobile_no",
                 "mobile_number", "contact_no", "contact_number",
                 "cell", "cell_no", "cell_number", "telephone", "telephone_no", 
                 "telephone_number", "contact", "contact_information"}:
            phone_cols = [c]
            break
    if not phone_cols:
        phone_cols = [c for c in df.columns if "phone" in c or "mobile" in c]

    name_cols = []
    for c in df.columns:
        if c in {"name", "full_name", "fullname", "customer_name",
                 "client_name", "first_name", "last_name",
                 "person_name", "username"}:
            name_cols = [c]
            break

    # VECTORIZED — no apply(axis=1)
    if email_cols:
        df["email"] = df[email_cols[0]].astype(str).str.strip().str.lower()
        df["email"] = df["email"].where(df[email_cols[0]].notna(), None)
        df["email"] = df["email"].where(df["email"] != "nan", None)
        df["email"] = df["email"].where(df["email"] != "", None)

    if phone_cols:
        df["phone"] = df[phone_cols[0]].astype(str).str.replace(
            r"[^\d]", "", regex=True
        )
        df["phone"] = df["phone"].where(df[phone_cols[0]].notna(), None)
        df["phone"] = df["phone"].where(df["phone"] != "nan", None)
        df["phone"] = df["phone"].where(df["phone"] != "", None)

    if name_cols:
        df["name"] = df[name_cols[0]].astype(str).str.strip().str.lower()
        df["name"] = df["name"].where(df[name_cols[0]].notna(), None)
        df["name"] = df["name"].where(df["name"] != "nan", None)
        df["name"] = df["name"].where(df["name"] != "", None)

    columns_to_drop = set(email_cols + phone_cols + name_cols)
    columns_to_drop.discard("email")
    columns_to_drop.discard("phone")
    columns_to_drop.discard("name")
    columns_to_drop = columns_to_drop & set(df.columns)
    df = df.drop(columns=list(columns_to_drop))

    return df

@app.get("/categories")
def list_categories(
    user_id: int | None = None,
    current_user: dict = Depends(get_current_user)
):
    with engine.begin() as conn:
        if current_user["role"] == "admin":
            if user_id:
                rows = conn.execute(text("""
                    SELECT c.id, c.name, COUNT(u.upload_id) AS uploads,
                           c.created_by_user_id
                    FROM categories c
                    LEFT JOIN upload_log u ON u.category_id = c.id
                    WHERE c.created_by_user_id = :uid
                    GROUP BY c.id
                    ORDER BY c.name
                """), {"uid": user_id}).fetchall()
            else:
                rows = conn.execute(text("""
                    SELECT c.id, c.name, COUNT(u.upload_id) AS uploads,
                           c.created_by_user_id
                    FROM categories c
                    LEFT JOIN upload_log u ON u.category_id = c.id
                    GROUP BY c.id
                    ORDER BY c.name
                """)).fetchall()
        else:
            rows = conn.execute(text("""
                SELECT c.id, c.name, COUNT(u.upload_id) AS uploads,
                       c.created_by_user_id
                FROM categories c
                LEFT JOIN upload_log u ON u.category_id = c.id
                WHERE c.created_by_user_id = :uid
                GROUP BY c.id
                ORDER BY c.name
            """), {"uid": current_user["id"]}).fetchall()

    return [
        {"id": r.id, "name": r.name, "uploads": r.uploads}
        for r in rows
    ]

@app.get("/upload-progress/{upload_id}")
async def upload_progress_stream(
    upload_id: int,
    token: str = Query(...)
):
    """SSE progress stream. Token passed as query param (EventSource limitation)."""
    try:
        payload = pyjwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = payload.get("user_id")
        if not user_id:
            raise ValueError("No user_id")
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")

    async def event_stream():
        timeout = 0
        while timeout < 300:
            progress = upload_progress_store.get(upload_id)

            if progress:
                yield f"data: {json.dumps(progress)}\n\n"
                if progress.get("status") in ("done", "error"):
                    upload_progress_store.pop(upload_id, None)
                    break
            else:
                yield f"data: {json.dumps({'percent': 0, 'status': 'waiting', 'message': 'Waiting...'})}\n\n"

            await asyncio.sleep(0.5)
            timeout += 0.5

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive"
        }
    )

def _process_file_sync(contents: bytes, name: str, upload_id: int) -> tuple:
    import time
    start_total = time.time()

    total_records = 0
    duplicate_records = 0
    seen_hashes = set()

    def update(pct, msg):
        upload_progress_store[upload_id] = {
            "percent": pct, "status": "processing", "message": msg
        }

    if name.endswith(".xlsx") or name.endswith(".xls"):
        t1 = time.time()
        update(20, "Reading Excel file...")
        try:
            df = pd.read_excel(io.BytesIO(contents), engine="calamine", dtype=str)
        except Exception:
            df = pd.read_excel(io.BytesIO(contents), dtype=str)
        print(f"[SYNC] Read Excel: {time.time() - t1:.2f}s")

        total_records = len(df)
        
        t2 = time.time()
        update(50, f"Normalizing {total_records:,} rows...")
        df = normalize_dataframe(df)
        print(f"[SYNC] Normalize: {time.time() - t2:.2f}s")

        t3 = time.time()
        update(68, "Deduplicating...")
        df["__hash"] = pd.util.hash_pandas_object(df, index=False).astype(str)
        df = df.drop_duplicates(subset="__hash")
        duplicate_records = total_records - len(df)
        df = df.drop(columns=["__hash"])
        print(f"[SYNC] Deduplicate: {time.time() - t3:.2f}s")

        t4 = time.time()
        update(82, "Saving to database...")
        copy_cleaned_data(engine, upload_id, df)
        print(f"[SYNC] Save to DB: {time.time() - t4:.2f}s")

    elif name.endswith(".csv"):
        estimated_total = max(contents.count(b'\n') - 1, 1)
        update(20, "Processing rows...")

        def csv_reader():
            try:
                return pd.read_csv(
                    io.BytesIO(contents), chunksize=500_000,
                    encoding="utf-8", low_memory=False, dtype=str
                )
            except UnicodeDecodeError:
                return pd.read_csv(
                    io.BytesIO(contents), chunksize=500_000,
                    encoding="latin1", low_memory=False, dtype=str
                )

        rows_done = 0
        for chunk in csv_reader():
            total_records += len(chunk)
            chunk = normalize_dataframe(chunk)
            chunk["__hash"] = pd.util.hash_pandas_object(
                chunk, index=False
            ).astype(str)
            new_mask = ~chunk["__hash"].isin(seen_hashes)
            chunk = chunk[new_mask]
            seen_hashes.update(chunk["__hash"].tolist())
            chunk = chunk.drop(columns=["__hash"])
            copy_cleaned_data(engine, upload_id, chunk)

            rows_done += len(chunk)
            pct = int(20 + (rows_done / estimated_total) * 68)
            update(min(pct, 88),
                   f"Processed {rows_done:,} / {estimated_total:,} rows...")

        duplicate_records = total_records - len(seen_hashes)

    print(f"[SYNC] TOTAL _process_file_sync: {time.time() - start_total:.2f}s")
    return total_records, duplicate_records

@app.post("/upload")
async def upload_file(
    category_id: int = Query(...),
    upload_id_hint: int = Query(None),
    file: UploadFile = File(...),
    user: dict = Depends(get_current_user)
):
    if user["role"] == "admin":
        raise HTTPException(
            status_code=403, detail="Admins cannot upload files"
        )

    contents = await file.read()
    original_filename = file.filename
    name = original_filename.lower()

    # ── Duplicate filename check ──
    with engine.begin() as conn:
        exists = conn.execute(
            text("""
                SELECT 1 FROM upload_log
                WHERE filename = :fname AND created_by_user_id = :uid
                LIMIT 1
            """),
            {"fname": original_filename, "uid": user["id"]}
        ).scalar()

    if exists:
        raise HTTPException(
            status_code=409,
            detail="File with the same name already exists"
        )

    upload_id = upload_id_hint if upload_id_hint else int(time.time() * 1000000)

    # ── Header detection (reads only 100 rows — fast) ──
    if name.endswith(".csv"):
        try:
            preview_df = pd.read_csv(
                io.BytesIO(contents), nrows=100, encoding="utf-8"
            )
        except UnicodeDecodeError:
            preview_df = pd.read_csv(
                io.BytesIO(contents), nrows=100, encoding="latin1"
            )
    elif name.endswith(".xls") or name.endswith(".xlsx"):
        try:
            preview_df = pd.read_excel(
                io.BytesIO(contents), nrows=100, engine="calamine"
            )
        except Exception:
            preview_df = pd.read_excel(io.BytesIO(contents), nrows=100)
    else:
        raise HTTPException(status_code=400, detail="Unsupported file")

    case_type, metadata = detect_header_case(preview_df)
    column_samples = get_column_samples(preview_df)
    original_headers = {
        'columns': [str(c) for c in preview_df.columns],
        'case_type': case_type,
        'metadata': metadata,
        'samples': column_samples
    }

    # ── Header resolution redirect ──
    if case_type in ['missing', 'suspicious']:
        temp_dir = tempfile.gettempdir()
        with open(os.path.join(temp_dir, f"datavault_{upload_id}.data"), 'wb') as f:
            f.write(contents)
        with open(os.path.join(temp_dir, f"datavault_{upload_id}.meta"), 'w') as f:
            json.dump({
                'upload_id': upload_id,
                'category_id': category_id,
                'filename': original_filename,
                'user_id': user["id"],
                'created_at': time.time(),
                'original_headers': original_headers
            }, f)
        return {
            "success": False,
            "status": "pending_headers",
            "upload_id": upload_id,
            "case_type": case_type,
            "message": "Headers need review" if case_type == 'missing'
                       else "First row might be data",
            "headers": original_headers
        }

    # ── Save raw file to disk for background processing ──
    queue_dir = os.path.join(tempfile.gettempdir(), "datavault_queue")
    os.makedirs(queue_dir, exist_ok=True)
    queued_file_path = os.path.join(queue_dir, f"{upload_id}.data")
    with open(queued_file_path, 'wb') as f:
        f.write(contents)

    final_headers = {'columns': [str(c) for c in preview_df.columns]}

    # ── Insert upload_log immediately with processing_status = 'processing' ──
    with engine.connect() as conn:
        conn.execute(
            text("""
                INSERT INTO upload_log
                (upload_id, category_id, filename,
                 total_records, duplicate_records,
                 failed_records, status, created_by_user_id,
                 header_status, original_headers, final_headers,
                 header_resolution_type, processing_status)
                VALUES
                (:uid, :cid, :f, 0, 0, 0, 'PROCESSING', :user_id,
                 'no_issue', :orig, :final, 'original', 'processing')
            """),
            {
                "uid": upload_id, "cid": category_id,
                "f": original_filename,
                "user_id": user["id"],
                "orig": json.dumps(original_headers),
                "final": json.dumps(final_headers)
            }
        )
        conn.commit()

    # ── Fire background task — does NOT block response ──
    loop = asyncio.get_event_loop()
    loop.run_in_executor(
        executor,
        _process_file_background,
        upload_id, queued_file_path, original_filename
    )

    # ── Return immediately — user sees file in list right away ──
    return {
        "success": True,
        "upload_id": upload_id,
        "status": "processing",
        "message": "File queued for processing"
    }

def _process_file_background(upload_id: int, queued_file_path: str,
                              original_filename: str):
    """
    Runs in thread pool after upload returns.
    Reads queued file, processes it, updates upload_log when done.
    """
    try:
        with open(queued_file_path, 'rb') as f:
            contents = f.read()

        name = original_filename.lower()

        total_records, duplicate_records = _process_file_sync(
            contents, name, upload_id
        )

        # ── Update upload_log with final counts and mark ready ──
        with engine.connect() as conn:
            conn.execute(
                text("""
                    UPDATE upload_log
                    SET total_records = :t,
                        duplicate_records = :d,
                        status = 'SUCCESS',
                        processing_status = 'ready'
                    WHERE upload_id = :uid
                """),
                {
                    "t": total_records,
                    "d": duplicate_records,
                    "uid": upload_id
                }
            )
            conn.commit()

        log_to_csv(original_filename, total_records, duplicate_records, 0, "SUCCESS")

    except Exception as e:
        import traceback
        traceback.print_exc()

        # Mark as failed so frontend can show error state
        try:
            with engine.connect() as conn:
                conn.execute(
                    text("""
                        UPDATE upload_log
                        SET status = 'FAILED',
                            processing_status = 'failed'
                        WHERE upload_id = :uid
                    """),
                    {"uid": upload_id}
                )
                conn.commit()
        except Exception:
            pass

    finally:
        # Always clean up the queued file
        try:
            os.remove(queued_file_path)
        except Exception:
            pass

@app.get("/upload/{upload_id}/headers")
def get_upload_headers(
    upload_id: int,
    user: dict = Depends(get_current_user)
):
    temp_dir = tempfile.gettempdir()
    temp_meta_path = os.path.join(temp_dir, f"datavault_{upload_id}.meta")
    
    if not os.path.exists(temp_meta_path):
        raise HTTPException(
            status_code=404, 
            detail="Upload session expired or not found. Please re-upload."
        )
    
    with open(temp_meta_path, 'r') as f:
        metadata = json.load(f)
    
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
    
    with open(temp_meta_path, 'r') as f:
        metadata = json.load(f)
    
    if metadata['user_id'] != user["id"] and user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Not authorized")
    
    filename = metadata['filename']
    category_id = metadata['category_id']
    original_headers_json = metadata['original_headers']
    
    with open(temp_file_path, 'rb') as f:
        contents = f.read()
    
    name = filename.lower()
    
    header_param = None if first_row_is_data else 0
    
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
    
    final_column_names = []
    for idx in range(total_columns):
        if idx in user_mapping and user_mapping[idx].strip():
            final_column_names.append(user_mapping[idx].strip().lower().replace(' ', '_'))
        else:
            final_column_names.append(f'unnamed_{idx}')
    
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

    total_records = 0
    duplicate_records = 0
    seen_hashes = set()

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
            chunk.columns = final_column_names
            total_records += len(chunk)
            
            if ingestion_mode == 'normalized':
                chunk = normalize_dataframe(chunk)
            
            chunk = chunk.drop_duplicates()
            chunk["__hash"] = pd.util.hash_pandas_object(chunk, index=False).astype(str)
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
        
        df.columns = final_column_names
        total_records = len(df)
        
        if ingestion_mode == 'normalized':
            df = normalize_dataframe(df)
        
        df = df.drop_duplicates()
        df["__hash"] = df.astype(str).agg("|".join, axis=1)
        df = df.drop_duplicates(subset="__hash")
        duplicate_records = total_records - len(df)
        df = df.drop(columns=["__hash"])
        copy_cleaned_data(engine, upload_id, df)

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
        "ingestion_mode": ingestion_mode
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
    current_user: dict = Depends(get_current_user),
    created_by_user_id: int | None = None,
):
    where = []
    params = {}

    if current_user["role"] != "admin":
        where.append("u.created_by_user_id = :owner_id")
        params["owner_id"] = current_user["id"]

    if current_user["role"] == "admin" and created_by_user_id:
        where.append("u.created_by_user_id = :filter_uid")
        params["filter_uid"] = created_by_user_id

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
                       u.processing_status,
                       u.uploaded_at,
                       u.created_by_user_id,
                       usr.email AS uploaded_by,
                       c.name AS category,
                       u.category_id
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
                "processing_status": r.processing_status,
                "uploaded_at": str(r.uploaded_at),
                "category": r.category,
                "category_id": r.category_id,
                "created_by_user_id": r.created_by_user_id,
                "uploaded_by": r.uploaded_by
            }
            for r in rows
        ]

@app.get("/upload/{upload_id}/status")
def get_upload_status(
    upload_id: int,
    user: dict = Depends(get_current_user)
):
    """Lightweight endpoint frontend polls to check processing status."""
    with engine.begin() as conn:
        result = conn.execute(
            text("""
                SELECT processing_status, total_records, 
                       duplicate_records, status
                FROM upload_log
                WHERE upload_id = :uid
            """),
            {"uid": upload_id}
        ).fetchone()

    if not result:
        raise HTTPException(status_code=404, detail="Upload not found")

    return {
        "upload_id": upload_id,
        "processing_status": result.processing_status,
        "total_records": result.total_records,
        "duplicate_records": result.duplicate_records,
        "status": result.status
    }

@app.post("/admin/cleanup-temp-uploads")
def cleanup_abandoned_uploads(user: dict = Depends(admin_only)):
    import glob
    
    temp_dir = tempfile.gettempdir()
    pattern = os.path.join(temp_dir, "datavault_*")
    
    deleted_count = 0
    current_time = time.time()
    max_age = 3600
    
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
        assert_upload_access(conn, upload_id, user)

        total = conn.execute(
            text("SELECT COUNT(*) FROM cleaned_data WHERE upload_id=:uid"),
            {"uid": upload_id}
        ).scalar()

        order_by = "ORDER BY id"
        if sort_column:
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

@app.get("/admin/dashboard-stats")
def dashboard_stats(
    days: int = Query(30),
    current_user: dict = Depends(get_current_user)
):
    if current_user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin only")

    with engine.begin() as conn:

        # ── USERS ──────────────────────────────────────────────────────────
        user_stats = conn.execute(text("""
            SELECT
                COUNT(*)                                    AS total,
                COUNT(*) FILTER (WHERE is_active = true)   AS active,
                COUNT(*) FILTER (WHERE is_active = false)  AS disabled
            FROM users
        """)).fetchone()

        # ── FILES ──────────────────────────────────────────────────────────
        file_stats = conn.execute(text("""
            SELECT
                COUNT(*)                                                        AS total,
                COALESCE(SUM(total_records), 0)                                 AS total_records,
                COALESCE(AVG(total_records), 0)                                 AS avg_per_file,
                COUNT(*) FILTER (WHERE processing_status = 'processing')        AS processing,
                COUNT(*) FILTER (WHERE processing_status = 'failed'
                                    OR status = 'FAILED')                       AS failed
            FROM upload_log
        """)).fetchone()

        # ── STORAGE HEALTH — orphaned rows ─────────────────────────────────
        orphan_count = conn.execute(text("""
            SELECT COUNT(*)
            FROM cleaned_data cd
            LEFT JOIN upload_log ul ON ul.upload_id = cd.upload_id
            WHERE ul.upload_id IS NULL
        """)).scalar()

        # ── ACTIVITY OVER TIME ─────────────────────────────────────────────
        activity = conn.execute(text("""
            SELECT
                uploaded_at::date                   AS date,
                COUNT(*)                            AS files,
                COALESCE(SUM(total_records), 0)     AS records
            FROM upload_log
            WHERE uploaded_at >= NOW() - INTERVAL '1 day' * :days
            GROUP BY uploaded_at::date
            ORDER BY uploaded_at::date
        """), {"days": days}).fetchall()

        # ── USER BREAKDOWN ─────────────────────────────────────────────────
        user_breakdown = conn.execute(text("""
            SELECT
                u.email,
                COUNT(ul.upload_id)                                 AS files,
                COALESCE(SUM(ul.total_records), 0)                  AS records,
                COALESCE(SUM(ul.duplicate_records), 0)              AS duplicates,
                CASE
                    WHEN SUM(ul.total_records) > 0
                    THEN (SUM(ul.duplicate_records)::float
                          / SUM(ul.total_records) * 100)
                    ELSE 0
                END                                                 AS avg_dup_rate
            FROM users u
            LEFT JOIN upload_log ul ON ul.created_by_user_id = u.id
            WHERE u.role != 'admin'
            GROUP BY u.id, u.email
            ORDER BY files DESC
        """)).fetchall()

        # ── FILE TYPES ─────────────────────────────────────────────────────
        file_types = conn.execute(text("""
            SELECT
                LOWER(SUBSTRING(filename FROM '\.([^.]+)$')) AS ext,
                COUNT(*)                                      AS count
            FROM upload_log
            GROUP BY ext
            ORDER BY count DESC
        """)).fetchall()

        # ── PROCESSING STATUS ──────────────────────────────────────────────
        proc_status = conn.execute(text("""
            SELECT
                COALESCE(processing_status, 'ready') AS status,
                COUNT(*)                              AS count
            FROM upload_log
            GROUP BY processing_status
            ORDER BY count DESC
        """)).fetchall()

        # ── RECENT ACTIVITY FEED ───────────────────────────────────────────
        recent = conn.execute(text("""
            SELECT
                ul.upload_id,
                ul.filename,
                ul.total_records,
                ul.duplicate_records,
                ul.processing_status,
                ul.status,
                ul.uploaded_at,
                u.email   AS uploaded_by,
                c.name    AS category
            FROM upload_log ul
            JOIN users u      ON u.id  = ul.created_by_user_id
            JOIN categories c ON c.id  = ul.category_id
            ORDER BY ul.uploaded_at DESC
            LIMIT 10
        """)).fetchall()

    return {
        "users": {
            "total":    user_stats.total,
            "active":   user_stats.active,
            "disabled": user_stats.disabled,
            "breakdown": [
                {
                    "email":        r.email,
                    "files":        r.files,
                    "records":      r.records,
                    "duplicates":   r.duplicates,
                    "avg_dup_rate": round(float(r.avg_dup_rate), 2)
                }
                for r in user_breakdown
            ]
        },
        "files": {
            "total":      file_stats.total,
            "processing": file_stats.processing,
            "failed":     file_stats.failed
        },
        "records": {
            "total":        file_stats.total_records,
            "avg_per_file": float(file_stats.avg_per_file)
        },
        "health": {
            "orphaned_rows": orphan_count
        },
        "activity": [
            {
                "date":    str(r.date),
                "files":   r.files,
                "records": r.records
            }
            for r in activity
        ],
        "file_types": [
            {"ext": r.ext or "unknown", "count": r.count}
            for r in file_types
        ],
        "processing_status": [
            {"status": r.status, "count": r.count}
            for r in proc_status
        ],
        "recent_activity": [
            {
                "upload_id":          r.upload_id,
                "filename":           r.filename,
                "total_records":      r.total_records,
                "duplicate_records":  r.duplicate_records,
                "processing_status":  r.processing_status,
                "uploaded_at":        str(r.uploaded_at),
                "uploaded_by":        r.uploaded_by,
                "category":           r.category
            }
            for r in recent
        ]
    }

@app.get("/search")
def search_data(
    upload_id: int,
    query: str = Query(..., min_length=1),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    user: dict = Depends(get_current_user)
):
    offset = (page - 1) * page_size
    search_term = f"%{query.lower()}%"
    
    with engine.begin() as conn:
        assert_upload_access(conn, upload_id, user)
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
        
        search_conditions = " OR ".join([
            f"LOWER(CAST(row_data->>'{col}' AS TEXT)) LIKE :search"
            for col in columns
        ])
        
        total = conn.execute(
            text(f"""
                SELECT COUNT(*)
                FROM cleaned_data
                WHERE upload_id = :uid
                AND ({search_conditions})
            """),
            {"uid": upload_id, "search": search_term}
        ).scalar()
        
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
    with engine.begin() as conn:
        assert_upload_access(conn, upload_id, user)
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

@app.delete("/admin/users/{user_id}")
def delete_user(
    user_id: int,
    policy: str = Query(..., regex="^(delete_all|transfer)$"),
    current_user: dict = Depends(get_current_user)
):
    if current_user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin only")

    with engine.begin() as conn:
        target = conn.execute(
            text("SELECT id, email FROM users WHERE id = :uid"),
            {"uid": user_id}
        ).fetchone()

        if not target:
            raise HTTPException(status_code=404, detail="User not found")

        if user_id == current_user["id"]:
            raise HTTPException(
                status_code=400,
                detail="Cannot delete your own account"
            )

        if policy == "delete_all":
            conn.execute(text("""
                DELETE FROM cleaned_data
                WHERE upload_id IN (
                    SELECT upload_id FROM upload_log
                    WHERE created_by_user_id = :uid
                )
            """), {"uid": user_id})

            conn.execute(
                text("DELETE FROM upload_log WHERE created_by_user_id = :uid"),
                {"uid": user_id}
            )

            conn.execute(
                text("DELETE FROM categories WHERE created_by_user_id = :uid"),
                {"uid": user_id}
            )

        elif policy == "transfer":
            admin_id = current_user["id"]

            conn.execute(text("""
                UPDATE upload_log
                SET created_by_user_id = :admin_id
                WHERE created_by_user_id = :uid
            """), {"admin_id": admin_id, "uid": user_id})

            conn.execute(text("""
                UPDATE categories
                SET created_by_user_id = :admin_id
                WHERE created_by_user_id = :uid
            """), {"admin_id": admin_id, "uid": user_id})

        conn.execute(
            text("DELETE FROM users WHERE id = :uid"),
            {"uid": user_id}
        )

    return {"success": True, "policy": policy}

@app.get("/admin/users-with-stats")
def admin_users_with_stats(current_user: dict = Depends(get_current_user)):
    if current_user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin only")

    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT 
                u.id,
                u.email,
                u.role,
                u.is_active,
                COUNT(ul.upload_id) AS upload_count
            FROM users u
            LEFT JOIN upload_log ul ON ul.created_by_user_id = u.id
            GROUP BY u.id
            ORDER BY u.email
        """)).fetchall()

    return [
        {
            "id": r.id,
            "email": r.email,
            "role": r.role,
            "is_active": r.is_active,
            "upload_count": r.upload_count
        }
        for r in rows
    ]

# ---------------- EXPORT ----------------
@app.get("/export")
def export_data(
    upload_id: int,
    format: str = Query("csv", regex="^(csv|excel)$"),
    user: dict = Depends(get_current_user)
):
    with engine.begin() as conn:
        assert_upload_access(conn, upload_id, user)
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
def delete_upload(upload_id: int, user: dict = Depends(get_current_user)):
    with engine.begin() as conn:
        # Ownership check
        owner = conn.execute(
            text("SELECT created_by_user_id FROM upload_log WHERE upload_id = :uid"),
            {"uid": upload_id}
        ).scalar()

        if owner is None:
            raise HTTPException(status_code=404, detail="Upload not found")

        if not can_delete_upload(user, owner):
            raise HTTPException(status_code=403, detail="Not authorized")

        conn.execute(
            text("DELETE FROM cleaned_data WHERE upload_id = :uid"),
            {"uid": upload_id}
        )
        conn.execute(
            text("DELETE FROM upload_log WHERE upload_id = :uid"),
            {"uid": upload_id}
        )

    return {"success": True}

# ---------------- BULK DELETE ----------------
@app.delete("/uploads/bulk")
def bulk_delete_uploads(
    request: BulkDeleteRequest,
    user: dict = Depends(get_current_user)
):
    """
    Delete multiple uploads in one request.
    Each upload_id is validated for ownership before any deletion occurs.
    Either ALL succeed or NONE are deleted (full transaction).
    """
    if not request.upload_ids:
        raise HTTPException(status_code=400, detail="No upload IDs provided")

    if len(request.upload_ids) > 100:
        raise HTTPException(status_code=400, detail="Cannot delete more than 100 files at once")

    with engine.begin() as conn:
        # Validate ownership of every ID before touching anything
        for uid in request.upload_ids:
            owner = conn.execute(
                text("SELECT created_by_user_id FROM upload_log WHERE upload_id = :uid"),
                {"uid": uid}
            ).scalar()

            if owner is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Upload {uid} not found"
                )

            if not can_delete_upload(user, owner):
                raise HTTPException(
                    status_code=403,
                    detail=f"Not authorized to delete upload {uid}"
                )

        # All checks passed — delete everything in one transaction
        conn.execute(
            text("DELETE FROM cleaned_data WHERE upload_id = ANY(:ids)"),
            {"ids": request.upload_ids}
        )
        conn.execute(
            text("DELETE FROM upload_log WHERE upload_id = ANY(:ids)"),
            {"ids": request.upload_ids}
        )

    return {
        "success": True,
        "deleted_count": len(request.upload_ids)
    }

# ---------------- MOVE UPLOAD ----------------
@app.patch("/upload/{upload_id}/move")
def move_upload(
    upload_id: int,
    request: MoveUploadRequest,
    user: dict = Depends(get_current_user)
):
    """
    Move an upload to a different category.
    - Users can only move their own uploads to their own categories.
    - Admins can move any upload to any category owned by the upload's owner.
    """
    with engine.begin() as conn:
        # Fetch upload owner and current category
        upload_row = conn.execute(
            text("""
                SELECT created_by_user_id, category_id
                FROM upload_log
                WHERE upload_id = :uid
            """),
            {"uid": upload_id}
        ).fetchone()

        if upload_row is None:
            raise HTTPException(status_code=404, detail="Upload not found")

        upload_owner_id = upload_row.created_by_user_id

        if not can_delete_upload(user, upload_owner_id):
            raise HTTPException(status_code=403, detail="Not authorized")

        if upload_row.category_id == request.category_id:
            raise HTTPException(status_code=400, detail="Upload is already in this category")

        # Verify target category exists and belongs to the upload's owner
        # (admin moves stay within the original owner's categories)
        target_owner = conn.execute(
            text("SELECT created_by_user_id FROM categories WHERE id = :cid"),
            {"cid": request.category_id}
        ).scalar()

        if target_owner is None:
            raise HTTPException(status_code=404, detail="Target category not found")

        if target_owner != upload_owner_id:
            raise HTTPException(
                status_code=403,
                detail="Target category does not belong to the upload's owner"
            )

        # Execute the move
        conn.execute(
            text("UPDATE upload_log SET category_id = :cid WHERE upload_id = :uid"),
            {"cid": request.category_id, "uid": upload_id}
        )

    return {"success": True, "upload_id": upload_id, "new_category_id": request.category_id}

# ---------------- CATEGORIES ----------------
@app.post("/categories")
def create_category(
    name: str = Query(...),
    current_user: dict = Depends(get_current_user)
):
    if current_user["role"] == "admin":
        raise HTTPException(
            status_code=403,
            detail="Admins cannot create categories"
        )

    with engine.begin() as conn:
        try:
            conn.execute(
                text("""
                    INSERT INTO categories (name, created_by_user_id)
                    VALUES (:n, :uid)
                """),
                {"n": name.strip(), "uid": current_user["id"]}
            )
        except:
            raise HTTPException(
                status_code=400,
                detail="Category already exists"
            )

    return {"success": True}

@app.put("/categories/{category_id}")
def rename_category(
    category_id: int,
    name: str = Query(...),
    current_user: dict = Depends(get_current_user)
):
    if current_user["role"] == "admin":
        raise HTTPException(
            status_code=403,
            detail="Admins cannot rename categories"
        )

    with engine.begin() as conn:
        owner = conn.execute(
            text("SELECT created_by_user_id FROM categories WHERE id = :cid"),
            {"cid": category_id}
        ).scalar()

        if owner is None:
            raise HTTPException(status_code=404, detail="Category not found")

        if owner != current_user["id"]:
            raise HTTPException(status_code=403, detail="Not your category")

        conn.execute(
            text("""
                UPDATE categories SET name = :name WHERE id = :cid
            """),
            {"name": name.strip(), "cid": category_id}
        )

    return {"success": True}

@app.delete("/categories/{category_id}")
def delete_category(
    category_id: int,
    current_user: dict = Depends(get_current_user)
):
    if current_user["role"] == "admin":
        raise HTTPException(
            status_code=403,
            detail="Admins cannot delete categories"
        )

    with engine.begin() as conn:
        owner = conn.execute(
            text("SELECT created_by_user_id FROM categories WHERE id = :cid"),
            {"cid": category_id}
        ).scalar()

        if owner is None:
            raise HTTPException(status_code=404, detail="Category not found")

        if owner != current_user["id"]:
            raise HTTPException(status_code=403, detail="Not your category")

        count = conn.execute(
            text("SELECT COUNT(*) FROM upload_log WHERE category_id = :cid"),
            {"cid": category_id}
        ).scalar()

        if count > 0:
            raise HTTPException(
                status_code=400,
                detail="Category has uploads. Delete or move them first."
            )

        conn.execute(
            text("DELETE FROM categories WHERE id = :cid"),
            {"cid": category_id}
        )

    return {"success": True}

# ---------------- STATIC ----------------
@app.get("/")
def serve_upload():
    return FileResponse(os.path.join("..", "frontend", "upload.html"))

@app.get("/preview.html")
def serve_preview():
    return FileResponse(os.path.join("..", "frontend", "preview.html"))

@app.patch("/admin/users/{user_id}/toggle-status")
def toggle_user_status(
    user_id: int,
    current_user: dict = Depends(get_current_user)
):
    if current_user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin only")

    if user_id == current_user["id"]:
        raise HTTPException(status_code=400, detail="Cannot disable your own account")

    with engine.begin() as conn:
        user = conn.execute(
            text("SELECT id, is_active FROM users WHERE id = :uid"),
            {"uid": user_id}
        ).fetchone()

        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        new_status = not user.is_active

        conn.execute(
            text("UPDATE users SET is_active = :status WHERE id = :uid"),
            {"status": new_status, "uid": user_id}
        )

    return {
        "success": True,
        "user_id": user_id,
        "is_active": new_status
    }

@app.post("/admin/users")
def create_user(
    email: str = Query(...),
    password: str = Query(...),
    role: str = Query("user"),
    current_user: dict = Depends(get_current_user)
):
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

@app.get("/related.html")
def serve_related():
    return FileResponse(os.path.join("..", "frontend", "related.html"))

@app.get("/related-records")
def related_records(
    upload_id: int,
    row_id: int,
    user: dict = Depends(get_current_user)
):
    with engine.begin() as conn:
        assert_upload_access(conn, upload_id, user)
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
    offset = (page - 1) * page_size
    value = value.strip()
    normalized_phone = ''.join(c for c in value if c.isdigit())
    
    where_conditions = ["LOWER(TRIM(row_data->>'email')) = LOWER(:val)"]
    query_params = {"uid": upload_id, "val": value}
    
    if normalized_phone:
        where_conditions.append("REGEXP_REPLACE(COALESCE(row_data->>'phone', ''), '[^0-9]', '', 'g') = :norm_phone")
        query_params["norm_phone"] = normalized_phone
    
    where_sql = " OR ".join(where_conditions)
    
    with engine.begin() as conn:
        assert_upload_access(conn, upload_id, user)
        total = conn.execute(
            text(f"""
                SELECT COUNT(*)
                FROM cleaned_data
                WHERE upload_id = :uid
                AND ({where_sql})
            """),
            query_params
        ).scalar()
        
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

@app.delete("/admin/cleanup-orphaned-rows")
def cleanup_orphaned_rows(current_user: dict = Depends(get_current_user)):
    if current_user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin only")

    with engine.begin() as conn:
        result = conn.execute(text("""
            DELETE FROM cleaned_data
            WHERE upload_id NOT IN (
                SELECT upload_id FROM upload_log
            )
        """))
    return {
        "success": True,
        "deleted_rows": result.rowcount
    }

@app.get("/related-grouped")
def related_grouped(
    upload_id: int,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    match_type: str = Query("all", regex="^(all|email|phone|merged|both)$"),
    sort: str = Query("size-desc", regex="^(size-desc|size-asc|alpha)$"),
    user: dict = Depends(get_current_user)
):
    if match_type == 'both':
        match_type = 'merged'

    if sort == "size-desc":
        order_clause = "ORDER BY record_count DESC, group_key ASC"
    elif sort == "size-asc":
        order_clause = "ORDER BY record_count ASC, group_key ASC"
    elif sort == "alpha":
        order_clause = "ORDER BY group_key ASC"
    else:
        order_clause = "ORDER BY record_count DESC, group_key ASC"

    offset = (page - 1) * page_size  

    with engine.begin() as conn:
        assert_upload_access(conn, upload_id, user)
        query = text(f"""
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
            
            ranked_groups AS (
                SELECT 
                    *,
                    ROW_NUMBER() OVER ({order_clause}) AS row_num
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
            {order_clause};
        """)

        rows = conn.execute(
            query,
            {
                "uid": upload_id,
                "match_type": match_type,
                "offset": offset,
                "limit": page_size
            }
        ).fetchall()

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
    with engine.begin() as conn:
        assert_upload_access(conn, upload_id, user)
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
                
                email_groups AS (
                    SELECT 
                        email,
                        COUNT(*) AS record_count
                    FROM normalized
                    WHERE email IS NOT NULL AND email != ''
                    GROUP BY email
                    HAVING COUNT(*) > 1
                ),
                
                phone_groups AS (
                    SELECT 
                        phone,
                        COUNT(*) AS record_count
                    FROM normalized
                    WHERE phone IS NOT NULL AND phone != ''
                    GROUP BY phone
                    HAVING COUNT(*) > 1
                ),
                
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
    with engine.begin() as conn:
        assert_upload_access(conn, upload_id, user)
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

@app.get("/me/profile")
def get_my_profile(current_user: dict = Depends(get_current_user)):
    with engine.begin() as conn:
        # Get user details
        user = conn.execute(
            text("""
                SELECT id, email, role, is_active, created_at
                FROM users
                WHERE id = :uid
            """),
            {"uid": current_user["id"]}
        ).fetchone()

        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        # Get upload stats
        stats = conn.execute(
            text("""
                SELECT
                    COUNT(*)                        AS total_uploads,
                    COALESCE(SUM(total_records), 0) AS total_records
                FROM upload_log
                WHERE created_by_user_id = :uid
                  AND processing_status != 'processing'
            """),
            {"uid": current_user["id"]}
        ).fetchone()

    return {
        "email":          user.email,
        "role":           user.role,
        "is_active":      user.is_active,
        "created_at":     str(user.created_at),
        "total_uploads":  stats.total_uploads,
        "total_records":  int(stats.total_records),
    }


@app.post("/me/change-password")
def change_my_password(
    payload: dict,
    current_user: dict = Depends(get_current_user)
):
    new_password = payload.get("new_password", "").strip()

    if not new_password or len(new_password) < 6:
        raise HTTPException(
            status_code=400,
            detail="Password must be at least 6 characters"
        )

    # Hash the new password using your existing utility
    hashed = hash_password(new_password)

    with engine.begin() as conn:
        conn.execute(
            text("UPDATE users SET password_hash = :pw WHERE id = :uid"),
            {"pw": hashed, "uid": current_user["id"]}
        )

    return {"success": True}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000)