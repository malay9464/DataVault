
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('admin', 'user')),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login_at TIMESTAMP
);

CREATE TABLE audit_logs (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL,
    action TEXT NOT NULL,
    target_type TEXT,
    target_id BIGINT,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_audit_user
        FOREIGN KEY (user_id)
        REFERENCES users(id)
);

CREATE INDEX idx_audit_logs_user_id ON audit_logs(user_id);
CREATE INDEX idx_audit_logs_created_at ON audit_logs(created_at);

ALTER TABLE upload_log
ADD COLUMN created_by_user_id INT;

ALTER TABLE upload_log
ADD CONSTRAINT fk_upload_user
FOREIGN KEY (created_by_user_id)
REFERENCES users(id);

INSERT INTO users (email, password_hash, role)
VALUES (
    'admin@system.local',
    'TEMP_HASH_REPLACE_LATER',
    'admin'
);

UPDATE upload_log
SET created_by_user_id = (
    SELECT id FROM users WHERE email = 'admin@system.local'
)
WHERE created_by_user_id IS NULL;

ALTER TABLE upload_log
ALTER COLUMN created_by_user_id SET NOT NULL;

select * from cleaned_data;
select * from users;

UPDATE users
SET password_hash = '$2b$12$DuhQsKNr7KgOv0saUiwbdebZaZgt1D1jTWy6RqS9yrJYJEhYCmCCK'
WHERE email = 'admin@system.local';

SELECT email, role, is_active FROM users;

UPDATE users
SET password_hash = '$pbkdf2-sha256$29000$DyEEwBhjjBHCuPeek3IOoQ$MevS1din.eSXulZnvWdWlsWbS2jCNbqdz2FQGFYCkoI'
WHERE email = 'admin@system.local';

INSERT INTO users (email, password_hash, role, is_active)
VALUES (
  'user2@example.com',
  '$pbkdf2-sha256$29000$FCIk5Fxr7f0/h/Ces/Z.Lw$59v.qfsBM0f9xXcoS080Aj2B9P6oLdlYFB3qYsrPSKY',
  'user',
  true
);

select * from users;

-- Run these SQL commands in your PostgreSQL database to create indexes
-- This will make queries 100x faster on large datasets

-- 1. Index on upload_id (already exists, but ensure it's there)
CREATE INDEX IF NOT EXISTS idx_cleaned_data_upload_id 
ON cleaned_data(upload_id);

-- 2. Index on email field (extracted from JSONB)
CREATE INDEX IF NOT EXISTS idx_cleaned_data_email 
ON cleaned_data(LOWER(TRIM(row_data->>'email')));

-- 3. Index on phone field (extracted and normalized from JSONB)
CREATE INDEX IF NOT EXISTS idx_cleaned_data_phone 
ON cleaned_data(REGEXP_REPLACE(COALESCE(row_data->>'phone', ''), '[^0-9]', '', 'g'));

-- 4. Composite index for faster duplicate detection
CREATE INDEX IF NOT EXISTS idx_cleaned_data_upload_email 
ON cleaned_data(upload_id, LOWER(TRIM(row_data->>'email'))) 
WHERE LOWER(TRIM(row_data->>'email')) IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_cleaned_data_upload_phone 
ON cleaned_data(upload_id, REGEXP_REPLACE(COALESCE(row_data->>'phone', ''), '[^0-9]', '', 'g'))
WHERE REGEXP_REPLACE(COALESCE(row_data->>'phone', ''), '[^0-9]', '', 'g') != '';

-- Analyze tables to update statistics
ANALYZE cleaned_data;

select * from users