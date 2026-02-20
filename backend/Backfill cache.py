# backfill_cache.py
# Run once from your backend/ directory:
#   python backfill_cache.py
#
# Builds related_groups_cache for ALL existing uploads.
# No re-uploading needed. Safe to re-run (deletes and rebuilds each upload's cache).

import sys
import time
sys.path.insert(0, '.')

from db import engine
from sqlalchemy import text


def build_cache_for_upload(conn, upload_id: int):
    # Wipe stale cache for this upload
    conn.execute(
        text("DELETE FROM related_groups_cache WHERE upload_id = :uid"),
        {"uid": upload_id}
    )

    # EMAIL groups
    conn.execute(text("""
        INSERT INTO related_groups_cache
            (upload_id, group_key, match_type, record_count, file_count, upload_ids)
        SELECT
            :uid,
            NULLIF(LOWER(TRIM(row_data->>'email')), 'nan') AS group_key,
            'email',
            COUNT(*),
            1,
            ARRAY[:uid]
        FROM cleaned_data
        WHERE upload_id = :uid
          AND NULLIF(LOWER(TRIM(row_data->>'email')), 'nan') IS NOT NULL
        GROUP BY 2
        HAVING COUNT(*) > 1
    """), {"uid": upload_id})

    # PHONE groups
    conn.execute(text("""
        INSERT INTO related_groups_cache
            (upload_id, group_key, match_type, record_count, file_count, upload_ids)
        SELECT
            :uid,
            NULLIF(REGEXP_REPLACE(
                COALESCE(row_data->>'phone',''), '[^0-9]','','g'), '') AS group_key,
            'phone',
            COUNT(*),
            1,
            ARRAY[:uid]
        FROM cleaned_data
        WHERE upload_id = :uid
          AND NULLIF(REGEXP_REPLACE(
                COALESCE(row_data->>'phone',''), '[^0-9]','','g'), '') IS NOT NULL
        GROUP BY 2
        HAVING COUNT(*) > 1
    """), {"uid": upload_id})

    # MERGED groups
    conn.execute(text("""
        INSERT INTO related_groups_cache
            (upload_id, group_key, match_type, record_count, file_count, upload_ids)
        SELECT
            :uid,
            NULLIF(LOWER(TRIM(row_data->>'email')), 'nan')
                || '__' ||
            NULLIF(REGEXP_REPLACE(
                COALESCE(row_data->>'phone',''), '[^0-9]','','g'), '') AS group_key,
            'merged',
            COUNT(*),
            1,
            ARRAY[:uid]
        FROM cleaned_data
        WHERE upload_id = :uid
          AND NULLIF(LOWER(TRIM(row_data->>'email')), 'nan') IS NOT NULL
          AND NULLIF(REGEXP_REPLACE(
                COALESCE(row_data->>'phone',''), '[^0-9]','','g'), '') IS NOT NULL
        GROUP BY 2
        HAVING COUNT(*) > 1
    """), {"uid": upload_id})

    conn.commit()


def main():
    # First make sure the cache table and indexes exist
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS related_groups_cache (
                id              SERIAL PRIMARY KEY,
                upload_id       INT NOT NULL,
                group_key       TEXT NOT NULL,
                match_type      TEXT NOT NULL,
                record_count    INT NOT NULL,
                file_count      INT NOT NULL DEFAULT 1,
                upload_ids      INT[] NOT NULL,
                created_at      TIMESTAMP DEFAULT NOW()
            )
        """))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_rgc_upload_id    ON related_groups_cache (upload_id)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_rgc_match_type   ON related_groups_cache (match_type)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_rgc_group_key    ON related_groups_cache (group_key)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_rgc_record_count ON related_groups_cache (record_count DESC)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_cd_upload_id     ON cleaned_data (upload_id)"))
        conn.commit()
        print("✓ Tables and indexes ready")

        # Fetch all ready uploads
        uploads = conn.execute(text("""
            SELECT upload_id, filename, total_records
            FROM upload_log
            WHERE processing_status = 'ready'
            ORDER BY uploaded_at ASC
        """)).fetchall()

    total = len(uploads)
    print(f"Found {total} uploads to backfill...\n")

    success = 0
    failed  = 0
    t_start = time.time()

    for i, u in enumerate(uploads, 1):
        t0 = time.time()
        try:
            with engine.connect() as conn:
                build_cache_for_upload(conn, u.upload_id)
            elapsed = time.time() - t0
            print(f"  [{i:>3}/{total}] ✓  {u.filename:<50} {u.total_records:>8,} records  {elapsed:.1f}s")
            success += 1
        except Exception as e:
            print(f"  [{i:>3}/{total}] ✗  {u.filename:<50} ERROR: {e}")
            failed += 1

    total_time = time.time() - t_start
    print(f"\n{'─'*60}")
    print(f"Done in {total_time:.1f}s — {success} succeeded, {failed} failed")
    print("You can now use /related-grouped-all — it will read from the cache.")


if __name__ == "__main__":
    main()