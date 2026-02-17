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
    Uses execute_values — inserts in batches of 2000 rows per SQL statement.
    Faster than COPY for JSONB because avoids CSV quote-escaping overhead.
    """
    raw_conn = engine.raw_connection()
    try:
        cursor = raw_conn.cursor()

        # Vectorized NaN replacement
        df = df.astype(object).where(pd.notnull(df), None)

        # Vectorized control char cleaning
        for col in df.select_dtypes(include="object").columns:
            df[col] = df[col].str.replace(
                _CONTROL_CHAR_RE, '', regex=True
            ).where(df[col].notna(), None)

        # Serialize entire dataframe to JSON records in one C-level call
        json_str = df.to_json(
            orient='records',
            force_ascii=False,
            default_handler=str
        )

        # Parse JSON array with orjson (Rust, very fast)
        try:
            import orjson as _oj
            rows = _oj.loads(json_str)
        except ImportError:
            import json as _j
            rows = _j.loads(json_str)

        data = [
            (upload_id, _serialize(row))
            for row in rows
        ]

        # Single batched INSERT — 2000 rows per statement
        execute_values(
            cursor,
            "INSERT INTO cleaned_data (upload_id, row_data) VALUES %s",
            data,
            template="(%s, %s::jsonb)",
            page_size=2000
        )

        raw_conn.commit()
        cursor.close()
    finally:
        raw_conn.close()