from sqlalchemy import create_engine
import psycopg2
import io
import json
import re
import numpy as np
import pandas as pd

DATABASE_URL = "postgresql://postgres:635343@localhost:5432/50_data"

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20
)

def clean_string(v):
    """Remove control characters that break JSON in PostgreSQL."""
    if isinstance(v, str):
        # Strip ASCII control chars (0x00-0x1F) except tab(\x09) and newline(\x0a)
        v = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', v)
    return v

def copy_cleaned_data(engine, upload_id: int, df: pd.DataFrame):
    """High-speed bulk insert using PostgreSQL COPY CSV format."""
    raw_conn = engine.raw_connection()
    try:
        raw_conn.set_session(autocommit=True)
        cursor = raw_conn.cursor()

        df = df.replace({np.nan: None})

        buffer = io.StringIO()
        for row_data in df.to_dict(orient="records"):
            # Clean control characters from string values
            cleaned = {k: clean_string(v) for k, v in row_data.items()}

            # Serialize to JSON — json.dumps handles all backslash escaping correctly
            json_str = json.dumps(cleaned, ensure_ascii=False, default=str)

            # Use CSV format: wrap the JSON in double quotes,
            # escape any double quotes inside by doubling them
            # This is the standard CSV quoting rule — no backslash interpretation
            json_csv = '"' + json_str.replace('"', '""') + '"'

            buffer.write(f"{upload_id}\t{json_csv}\n")

        buffer.seek(0)

        # FORMAT CSV with tab delimiter — backslashes in data are safe
        cursor.copy_expert(
            "COPY cleaned_data (upload_id, row_data) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', QUOTE '\"')",
            buffer
        )

        cursor.close()
    finally:
        raw_conn.close()