# 🗄️ Data Vault

A **strictly multi-tenant data ingestion and analysis platform** built with FastAPI and PostgreSQL. Upload messy real-world CSV/Excel files, clean and normalize them, detect duplicates, and analyze related records — all with complete isolation between users.

---

## 📸 Overview

Data Vault lets multiple isolated users:
- Upload large tabular datasets (CSV, XLS, XLSX)
- Auto-normalize and clean messy column data
- Detect and remove duplicate records
- Preview, search, and sort cleaned data
- Find related records grouped by email or phone
- Export cleaned data as CSV or Excel
- Manage files in user-owned categories

Admins get a supervisory layer — view all users and their uploads, manage accounts, reset passwords, and delete users with full data control.

---

## 🏗️ Tech Stack

| Layer | Technology |
|-------|-----------|
| Backend | FastAPI (Python) |
| Database | PostgreSQL with JSONB storage |
| Data Processing | Pandas |
| Authentication | JWT (PyJWT), 8-hour expiry |
| Password Hashing | bcrypt |
| Bulk Insert | PostgreSQL COPY via psycopg2 |
| Frontend | Vanilla HTML + CSS + JavaScript |
| Icons | Feather Icons |

---

## 📁 Project Structure

```
backend/
├── main.py                  # All API routes and endpoint logic
├── auth.py                  # JWT creation, validation, get_current_user
├── db.py                    # SQLAlchemy engine, bulk insert via COPY
├── permissions.py           # Role checks and ownership validation
├── security.py              # bcrypt hash/verify
├── users.py                 # /users router (list, create, status, role)
├── logger.py                # CSV upload event logging
├── header_resolution.py     # Header detection and normalization
└── requirements.txt

frontend/
├── upload.html/js/css       # Main dashboard — upload + file list
├── preview.html/js/css      # Data preview with pagination and sort
├── related.html/js/css      # Related records viewer
├── users.html/js/css        # Admin user management
├── header.html/js           # Header review and correction flow
└── login.html/js/css        # Login page
```

---

## 🗃️ Database Schema

```sql
users
  id, email (unique), password_hash, role, is_active, created_at, last_login_at

categories
  id, name, created_by_user_id
  UNIQUE (name, created_by_user_id)   -- per-user uniqueness

upload_log
  upload_id (bigint), category_id, filename, created_by_user_id,
  total_records, duplicate_records, status, uploaded_at,
  header_status, original_headers (jsonb), final_headers (jsonb),
  header_resolution_type, first_row_is_data
  UNIQUE (filename, created_by_user_id)  -- per-user uniqueness

cleaned_data
  id, upload_id, row_data (jsonb)
  INDEX on (upload_id)
```

---

## 🚀 Setup & Installation

### 1. Clone and install dependencies

```bash
cd backend
pip install -r requirements.txt
```

### 2. Configure database

Edit `db.py`:
```python
DATABASE_URL = "postgresql://postgres:YOUR_PASSWORD@localhost:5432/YOUR_DB"
```

### 3. Run database migrations

```sql
-- Multi-tenant constraints
ALTER TABLE categories ADD COLUMN IF NOT EXISTS
  created_by_user_id INTEGER REFERENCES users(id);

ALTER TABLE categories DROP CONSTRAINT IF EXISTS categories_name_key;
ALTER TABLE categories ADD CONSTRAINT categories_name_user_unique
  UNIQUE (name, created_by_user_id);

ALTER TABLE upload_log DROP CONSTRAINT IF EXISTS upload_log_filename_key;
ALTER TABLE upload_log ADD CONSTRAINT upload_log_filename_user_unique
  UNIQUE (filename, created_by_user_id);

-- Assign existing categories to their owners
UPDATE categories c
SET created_by_user_id = (
    SELECT ul.created_by_user_id FROM upload_log ul
    WHERE ul.category_id = c.id
    GROUP BY ul.created_by_user_id
    ORDER BY COUNT(*) DESC LIMIT 1
) WHERE c.created_by_user_id IS NULL;

UPDATE categories SET created_by_user_id = 1
WHERE created_by_user_id IS NULL;
```

### 4. Start the server

```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

Open `http://localhost:8000` in your browser.

---

## 👥 Role System

### User
- Create, rename, delete own categories (delete only if empty)
- Upload CSV/XLS/XLSX files into own categories
- Preview, search, export, delete own uploads
- View related/duplicate records within own uploads
- **Cannot** see other users or their data

### Admin
- View all uploads across all users
- Delete any upload
- Create, delete, disable users
- Reset passwords
- View uploads filtered by user and category
- Transfer or purge user data on account deletion
- **Cannot** upload files
- **Cannot** create or modify categories

---

## ✨ Features

### Data Ingestion
- **Streaming CSV ingestion** — 100k row chunks, UTF-8 with latin1 fallback
- **Excel support** — XLS and XLSX
- **Header detection** — auto-detects missing or suspicious headers
- **Header resolution flow** — user reviews and corrects columns before ingestion
- **Column normalization** — maps common variants to canonical `email`, `phone`, `name`
- **Deduplication** — row-hash based exact duplicate removal

### Data Management
- **Categories** — user-owned, per-user unique names
- **File filtering** — by category, filename, record count, duplicate count, date range
- **Pagination** — 10 files per page with page navigation
- **Advanced filters** — record range, duplicate range, date range

### Preview & Search
- **Paginated preview** — 50 rows per page
- **Column sort** — click any header, asc/desc toggle
- **Server-side search** — full-text across all columns instantly

### Related Records
- **By row** — find all records sharing same email or phone as a given row
- **By value** — search any email or phone across the entire upload
- **Grouped view** — duplicate groups sorted by size or alphabetically
- **Filter by type** — email duplicates, phone duplicates, or both
- **Stats bar** — counts of email groups, phone groups, merged groups
- **Resizable columns** — drag column borders to resize

### Export
- **CSV export** — cleaned data as downloadable CSV
- **Excel export** — cleaned data as XLSX

### Admin Panel
- **User sidebar** — lists all users with upload counts
- **Click-to-filter** — click any user to see their uploads
- **Category dropdown** — filter within a user's uploads by category
- **User management** — create, disable, change role, reset password
- **User deletion** — delete all data or transfer to admin

### UX
- **Keyboard shortcuts** — `/` search, `N` new file, `?` shortcuts, `Esc` close
- **Drag and drop** upload
- **Toast notifications** — success, error, warning
- **Empty state messages** — contextual (no data vs no filter results)
- **Upload progress bar** — animated during file processing

---

## 🔒 Security

- All authorization enforced **server-side** — frontend checks are UI only
- Every upload access validates **ownership**, not just delete
- Role checks centralized in `permissions.py`
- bcrypt password hashing
- JWT tokens with 8-hour expiry
- Admin cannot access user data endpoints through privilege escalation

---

## 🔌 Key API Endpoints

```
POST   /login                          Auth
GET    /me                             Current user info

GET    /categories                     List own categories
POST   /categories                     Create category
PUT    /categories/{id}                Rename category
DELETE /categories/{id}                Delete empty category

POST   /upload                         Upload file
GET    /uploads                        List uploads (filtered)
DELETE /upload/{id}                    Delete upload
GET    /export                         Export as CSV or Excel

GET    /preview                        Paginated data preview
GET    /search                         Full-text search

GET    /related-grouped                Grouped duplicate records
GET    /related-search                 Search by email/phone
GET    /related-grouped-stats          Duplicate statistics

GET    /users                          Admin: list users
POST   /users                          Admin: create user
DELETE /admin/users/{id}               Admin: delete user (policy=delete_all|transfer)
GET    /admin/users-with-stats         Admin: users with upload counts
```

---