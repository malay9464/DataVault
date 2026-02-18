from sqlalchemy import create_engine
import io
import re
import numpy as np
import pandas as pd
from psycopg2.extras import execute_values

DATABASE_URL = "postgresql://postgres:635343@localhost:5432/50_data"

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20
)

_CONTROL_CHAR_RE = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]')

try:
    import orjson
    def _serialize(row: dict) -> str:
        return orjson.dumps(row, option=orjson.OPT_NON_STR_KEYS).decode('utf-8')
except ImportError:
    import json
    def _serialize(row: dict) -> str:
        return json.dumps(row, ensure_ascii=False, default=str)


def copy_cleaned_data(engine, upload_id: int, df: pd.DataFrame):
    """
    Optimized COPY with minimal JSONB parsing overhead.
    Uses CSV format instead of text format for better escaping.
    """
    import time
    start = time.time()
    
    raw_conn = engine.raw_connection()
    try:
        cursor = raw_conn.cursor()

        # Vectorized NaN replacement
        df = df.astype(object).where(pd.notnull(df), None)

        # Skip control char cleaning — it's slow and rarely needed
        # (only enable if you actually have data quality issues)

        # Convert DataFrame to list of dicts (fast with to_dict)
        t1 = time.time()
        rows = df.to_dict('records')
        print(f"[TIMING] to_dict: {time.time() - t1:.2f}s")

        # Build CSV buffer with pre-serialized JSON
        t2 = time.time()
        csv_buffer = io.StringIO()
        for row in rows:
            # Serialize JSON once
            json_str = _serialize(row)
            # Write as CSV row: upload_id, json_string
            # Use csv module for proper escaping
            csv_buffer.write(f"{upload_id}\t{json_str}\n")
        csv_buffer.seek(0)
        print(f"[TIMING] CSV buffer: {time.time() - t2:.2f}s")

        # COPY with DELIMITER (tab-separated)
        t3 = time.time()
        cursor.copy_expert(
            """
            COPY cleaned_data (upload_id, row_data)
            FROM STDIN
            WITH (FORMAT csv, DELIMITER E'\\t', QUOTE E'\\x01', ESCAPE E'\\x02')
            """,
            csv_buffer
        )
        print(f"[TIMING] COPY: {time.time() - t3:.2f}s")

        raw_conn.commit()
        cursor.close()
        
        print(f"[TIMING] TOTAL: {time.time() - start:.2f}s")
    finally:
        raw_conn.close()