CREATE TABLE upload_log (
    id SERIAL PRIMARY KEY,
    filename TEXT,
    total_records INT,
    duplicate_records INT,
    failed_records INT,
    status TEXT,
    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE cleaned_data (
    id SERIAL PRIMARY KEY,
    upload_id BIGINT,
    row_data JSONB
);

CREATE INDEX idx_cleaned_data_upload_id
ON cleaned_data(upload_id);

select * from upload_log order by uploaded_at desc;

select * from cleaned_data;

ALTER TABLE upload_log
ADD COLUMN upload_id BIGINT;

DELETE FROM upload_log
WHERE upload_id IS NULL;

DELETE FROM cleaned_data
WHERE upload_id NOT IN (
    SELECT upload_id FROM upload_log
);

VACUUM ANALYZE upload_log;

VACUUM ANALYZE cleaned_data;

ALTER TABLE upload_log
ALTER COLUMN upload_id SET NOT NULL;

CREATE TABLE categories (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

ALTER TABLE upload_log
ADD COLUMN category_id INT;

CREATE INDEX idx_upload_log_category_id
ON upload_log(category_id);

table (if not already created)
CREATE TABLE IF NOT EXISTS categories (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);