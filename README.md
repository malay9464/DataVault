# 🗄️ Data Vault

An internal data management system for uploading, deduplicating, and analysing CSV and Excel files. Built with FastAPI + PostgreSQL + vanilla JS.

---

## What It Does

- Upload CSV, XLS, and XLSX files organised into categories
- Automatically detects and counts duplicate records within each file
- Finds contacts that appear across **multiple files** (cross-file deduplication by email & phone)
- Admin dashboard with charts — top users, file types, duplicate rates, upload activity
- Role-based access: **Admin** manages users and views all data; **Users** manage their own files
- Export cleaned data as CSV or Excel

---

## Tech Stack

| Layer | Technology |
|---|---|
| Backend | Python · FastAPI · Uvicorn |
| Database | PostgreSQL · SQLAlchemy |
| Data Processing | pandas · numpy · python-calamine |
| Auth | JWT (PyJWT) · passlib pbkdf2_sha256 |
| Frontend | Vanilla HTML · CSS · JavaScript |
| Charts | Chart.js |

---

## Prerequisites

Before running, make sure you have:

- **Python 3.11+** — [python.org/downloads](https://www.python.org/downloads/)
- **PostgreSQL** — [postgresql.org/download](https://www.postgresql.org/download/)
- **Git** — [git-scm.com](https://git-scm.com/)

---

## Setup & Installation

### 1. Clone the repository

```bash
git clone https://github.com/your-username/data-vault.git
cd data-vault
```

### 2. Create a virtual environment

```bash
# Windows
python -m venv venv
venv\Scripts\activate

# macOS / Linux
python -m venv venv
source venv/bin/activate
```

### 3. Install dependencies

```bash
cd backend
pip install -r requirements.txt
```

### 4. Set up the PostgreSQL database

Open pgAdmin or psql and create a new database:

```sql
CREATE DATABASE 50_data;
```

Then create the required tables (run this in your database):

```sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    role TEXT DEFAULT 'user',
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW(),
    last_login_at TIMESTAMP
);

CREATE TABLE categories (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    created_by_user_id INT REFERENCES users(id)
);

CREATE TABLE upload_log (
    upload_id BIGINT PRIMARY KEY,
    category_id INT REFERENCES categories(id),
    filename TEXT,
    total_records INT DEFAULT 0,
    duplicate_records INT DEFAULT 0,
    failed_records INT DEFAULT 0,
    status TEXT DEFAULT 'PROCESSING',
    processing_status TEXT DEFAULT 'processing',
    created_by_user_id INT REFERENCES users(id),
    header_status TEXT DEFAULT 'no_issue',
    original_headers JSONB,
    final_headers JSONB,
    header_resolution_type TEXT DEFAULT 'original',
    first_row_is_data BOOLEAN DEFAULT FALSE,
    uploaded_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE cleaned_data (
    id BIGSERIAL PRIMARY KEY,
    upload_id BIGINT REFERENCES upload_log(upload_id),
    row_data JSONB
);

CREATE INDEX idx_cleaned_data_upload_id ON cleaned_data(upload_id);

CREATE TABLE related_groups_cache (
    id BIGSERIAL PRIMARY KEY,
    upload_id BIGINT REFERENCES upload_log(upload_id),
    match_type TEXT,
    group_key TEXT,
    record_count INT,
    file_count INT DEFAULT 1,
    upload_ids BIGINT[],
    created_at TIMESTAMP DEFAULT NOW()
);
```

### 5. Create your first admin user

Run this in psql or pgAdmin (replace the password hash with your own):

```sql
-- First, run this Python snippet to generate a hash for your password:
-- from passlib.context import CryptContext
-- ctx = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")
-- print(ctx.hash("your_password_here"))

INSERT INTO users (email, password_hash, role, is_active)
VALUES ('admin@yourcompany.com', '<paste_hash_here>', 'admin', true);
```

### 6. Configure the database connection

Open `backend/db.py` and update the connection string with your PostgreSQL credentials:

```python
DATABASE_URL = "postgresql://YOUR_USER:YOUR_PASSWORD@localhost:5432/50_data"
```

### 7. Run the application

```bash
# Make sure you are in the backend folder
cd backend
uvicorn main:app --host 0.0.0.0 --port 8000
```

### 8. Open in browser

```
http://localhost:8000
```

Log in with the admin credentials you created in Step 5.

---

## Project Structure

```
data-vault/
├── backend/
│   ├── main.py           ← All API endpoints
│   ├── db.py             ← Database connection & bulk insert
│   ├── auth.py           ← JWT authentication
│   ├── permissions.py    ← Role-based access control
│   ├── security.py       ← Password hashing
│   ├── users.py          ← Users router
│   ├── header.py         ← File header detection
│   ├── logger.py         ← Upload activity logger
│   └── requirements.txt
└── frontend/
    ├── upload.html        ← Main page
    ├── login.html         ← Login page
    ├── preview.html       ← File record viewer
    ├── related.html       ← Single-file duplicates
    ├── related-all.html   ← Cross-file duplicates
    ├── users.html         ← User management (admin)
    ├── header.html        ← Header resolution
    ├── css/
    └── js/
```

---

## Default Ports

| Service | Port |
|---|---|
| FastAPI server | 8000 |
| PostgreSQL | 5432 |

---

## Notes

- Admins **cannot** upload files — only regular users can
- Admins **cannot** create or manage categories
- The `related_groups_cache` table is built automatically after every upload. If you migrate data manually, rebuild it by calling: `GET /admin/rebuild-phone-cache`
- Upload progress is streamed via Server-Sent Events (SSE) — works in all modern browsers
- Large files (CSV) are processed in 500,000-row chunks to avoid memory issues

---

## License

Internal use only.