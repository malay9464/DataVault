
CREATE TABLE IF NOT EXISTS categories (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS upload_log (
    id SERIAL PRIMARY KEY,
    upload_id BIGINT NOT NULL UNIQUE,
    category_id INT NOT NULL,
    filename TEXT NOT NULL,
    total_records INT NOT NULL,
    duplicate_records INT NOT NULL,
    failed_records INT NOT NULL,
    status TEXT NOT NULL,
    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_upload_category
        FOREIGN KEY (category_id)
        REFERENCES categories(id)
);

CREATE INDEX IF NOT EXISTS idx_upload_log_category_id
ON upload_log(category_id);

CREATE INDEX IF NOT EXISTS idx_upload_log_uploaded_at
ON upload_log(uploaded_at);

CREATE TABLE IF NOT EXISTS cleaned_data (
    id SERIAL PRIMARY KEY,
    upload_id BIGINT NOT NULL,
    row_data JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_cleaned_data_upload_id
ON cleaned_data(upload_id);

INSERT INTO categories (name)
VALUES ('Uncategorized')
ON CONFLICT (name) DO NOTHING;

UPDATE upload_log
SET category_id = (
    SELECT id FROM categories WHERE name = 'Uncategorized'
)
WHERE category_id IS NULL;

ALTER TABLE upload_log
ALTER COLUMN category_id SET NOT NULL;

-- Categories
SELECT * FROM categories;

-- Uploads with category names
SELECT u.upload_id, u.filename, c.name
FROM upload_log u
JOIN categories c ON c.id = u.category_id;

-- Orphan cleaned data (should be ZERO)
SELECT COUNT(*)
FROM cleaned_data cd
LEFT JOIN upload_log ul
ON cd.upload_id = ul.upload_id
WHERE ul.upload_id IS NULL;
