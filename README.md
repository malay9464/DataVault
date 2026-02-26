# 🗄️ Data Vault

A full-stack web application for uploading, managing, and detecting duplicate records across multiple CSV and Excel files. Built for organizations that need to find the same person across different datasets using email and phone matching.

---

## 📋 Table of Contents

- [Features](#features)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Environment Setup](#environment-setup)
- [Database Setup](#database-setup)
- [Running the App](#running-the-app)
- [User Roles](#user-roles)
- [How It Works](#how-it-works)
- [Phone Validation Rules](#phone-validation-rules)
- [API Overview](#api-overview)

---

## ✨ Features

- **File Upload** — Upload CSV and Excel (`.xlsx`, `.xls`) files with real-time background processing
- **Duplicate Detection** — Detects duplicates within a file using row hashing
- **Related Records** — Groups records by matching email, phone, or both within a single file
- **Cross-file Matching** — Finds the same person across different files using a shared cache (`related_groups_cache`)
- **Smart Header Detection** — Automatically detects missing or suspicious headers and prompts the user to resolve them
- **Phone Validation** — 8-rule system to filter out fake, junk, and malformed phone numbers
- **Admin Dashboard** — Full visibility into all users, files, records, duplicate rates, and activity feed
- **User Management** — Create, disable, reset passwords, delete users, and transfer their data
- **Category System** — Users organise their files into categories
- **Export** — Download cleaned data as CSV or Excel
- **Progress Streaming** — Real-time upload progress via Server-Sent Events (SSE)
- **Role-based Access** — Admins see everything, users see only their own data

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|------------|
| Backend | FastAPI (Python) |
| Database | PostgreSQL |
| ORM / DB Driver | SQLAlchemy + psycopg2 |
| Data Processing | Pandas, NumPy |
| Excel Reading | python-calamine, openpyxl, xlrd |
| Authentication | JWT via PyJWT |
| Password Hashing | passlib (pbkdf2_sha256) |
| PDF Generation | ReportLab |
| Fast JSON | orjson |
| Frontend | Vanilla JS, HTML, CSS |
| OS | Windows |

---

## ✅ Prerequisites

- **Python 3.11+**
- **PostgreSQL 14+**
- **pip**
- Windows OS

---

## 🚀 Installation

**1. Clone the repository:**
```bash
git clone <your-repo-url>
cd DataVault
```

**2. Create and activate a virtual environment:**
```bash
python -m venv venv
venv\Scripts\activate
```

**3. Install dependencies:**
```bash
cd backend
pip install -r requirements.txt
```

---

## 🔐 Environment Setup

Create a `.env` file inside the `backend/` folder with the following:

```env
DATABASE_URL=postgresql://postgres:YOUR_PASSWORD@localhost:5432/YOUR_DB_NAME
SECRET_KEY=your_jwt_secret_key_here
ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_MINUTES=480
```

> ⚠️ Never commit your `.env` file. Add it to `.gitignore`.

---

## 🗃️ Database Setup

**1. Create the PostgreSQL database:**
```sql
CREATE DATABASE 50_data;
```

**2. Create all required tables:**
```sql
-- Users table
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    role TEXT DEFAULT 'user',
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Categories table
CREATE TABLE categories (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    created_by_user_id INTEGER REFERENCES users(id),
    UNIQUE(name, created_by_user_id)
);

-- Upload log table
CREATE TABLE upload_log (
    upload_id BIGINT PRIMARY KEY,
    category_id INTEGER REFERENCES categories(id),
    filename TEXT,
    total_records INTEGER DEFAULT 0,
    duplicate_records INTEGER DEFAULT 0,
    failed_records INTEGER DEFAULT 0,
    status TEXT DEFAULT 'PROCESSING',
    processing_status TEXT DEFAULT 'processing',
    created_by_user_id INTEGER REFERENCES users(id),
    uploaded_at TIMESTAMPTZ DEFAULT NOW(),
    header_status TEXT,
    original_headers JSONB,
    final_headers JSONB,
    header_resolution_type TEXT,
    first_row_is_data BOOLEAN DEFAULT false
);

-- Cleaned data table (stores every row as JSONB)
CREATE TABLE cleaned_data (
    id BIGSERIAL PRIMARY KEY,
    upload_id BIGINT REFERENCES upload_log(upload_id),
    row_data JSONB
);

CREATE INDEX idx_cleaned_data_upload_id ON cleaned_data(upload_id);

-- Related groups cache (for fast cross-file duplicate lookup)
CREATE TABLE related_groups_cache (
    id BIGSERIAL PRIMARY KEY,
    upload_id BIGINT,
    group_key TEXT,
    match_type TEXT,
    record_count INTEGER,
    file_count INTEGER,
    upload_ids BIGINT[]
);

CREATE INDEX idx_rgc_upload_id ON related_groups_cache(upload_id);
CREATE INDEX idx_rgc_group_key ON related_groups_cache(group_key);
CREATE INDEX idx_rgc_match_type ON related_groups_cache(match_type);
```

**3. Create your first admin user:**

Open a Python shell inside `backend/`:
```python
from security import hash_password
print(hash_password("your_admin_password"))
```

Then run this SQL with the output:
```sql
INSERT INTO users (email, password_hash, role, is_active)
VALUES ('admin@yourdomain.com', '<paste_hash_here>', 'admin', true);
```

---

## ▶️ Running the App

```bash
cd backend
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

Then open your browser at:
```
http://127.0.0.1:8000
```

---

## 👥 User Roles

| Permission | Admin | User |
|------------|-------|------|
| Upload files | ❌ | ✅ |
| View own files | ✅ | ✅ |
| View all users' files | ✅ | ❌ |
| Create categories | ❌ | ✅ |
| Delete own files | ✅ | ✅ |
| Delete any file | ✅ | ❌ |
| Manage users | ✅ | ❌ |
| View admin dashboard | ✅ | ❌ |
| View related records | ✅ | ✅ |

---

## ⚙️ How It Works

### File Upload Flow
1. User uploads a CSV or Excel file
2. System detects headers — if missing or suspicious, user is prompted to resolve them
3. File is saved to a temp queue and the API responds immediately (non-blocking)
4. Background thread reads the file, normalizes it via `normalize_dataframe()`, deduplicates rows by hash, and saves to `cleaned_data`
5. After saving, `_build_cache_for_upload()` builds phone/email/merged groups in `related_groups_cache`

### Duplicate Detection
- **Within a file** — rows are hashed and exact duplicates are dropped at upload time
- **Within a file (related records)** — records sharing the same email or phone are grouped
- **Across files (cross-file)** — `related_groups_cache` is queried to find group keys that appear in more than one file

### Data Normalization
Every uploaded file goes through `normalize_dataframe()` which:
- Standardizes column names (lowercase, underscores)
- Detects and extracts email, phone, and name columns
- Cleans email (strip, lowercase)
- Validates and extracts phone using `extract_best_phone()`
- Handles compound phone values like `9858543575/8568523147` by picking the first valid number

---

## 📱 Phone Validation Rules

All phone numbers are validated by `is_valid_phone()` before being stored or indexed:

| Rule | Description | Example Rejected |
|------|-------------|-----------------|
| 1 | Length must be 6–25 digits | `221`, `0` |
| 2 | Not all same digit | `9999999999` |
| 3 | Not all zeros | `0000000000` |
| 4 | Not sequential | `1234567890` |
| 5 | Not in fake number blacklist | `8888888888` |
| 6 | Must have at least 4 unique digits | `2211111`, `229999` |
| 7 | No single digit can exceed 60% frequency | `2211111111` |
| 8 | Must not start with `00` | `00220000001` |

---

## 🔌 API Overview

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/login` | Get JWT token |
| GET | `/me` | Current user info |
| POST | `/upload` | Upload a file |
| GET | `/uploads` | List files |
| DELETE | `/upload/{id}` | Delete a file |
| DELETE | `/uploads/bulk` | Bulk delete files |
| GET | `/preview` | Preview file data |
| GET | `/search` | Search within a file |
| GET | `/export` | Download file as CSV or Excel |
| GET | `/categories` | List categories |
| POST | `/categories` | Create category |
| GET | `/related-grouped` | Duplicates within one file |
| GET | `/related-grouped-all` | Cross-file duplicate groups |
| GET | `/related-all-stats` | Cross-file duplicate stats |
| GET | `/related-group-records` | Records for a specific group |
| GET | `/admin/dashboard-stats` | Admin dashboard data |
| GET | `/admin/users` | List all users |
| POST | `/admin/users` | Create user |
| DELETE | `/admin/users/{id}` | Delete user |
| PATCH | `/admin/users/{id}/toggle-status` | Enable/disable user |

---

## 🔒 Security Notes

- Passwords are hashed using **pbkdf2_sha256** via passlib
- All endpoints require a valid **JWT Bearer token**
- Admins can access all data; users are scoped to their own uploads
- Temp upload files are automatically cleaned up after processing
- No raw SQL string interpolation for user input — all queries use bound parameters

---

## 📝 License

Internal use only.