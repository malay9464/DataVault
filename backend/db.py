from sqlalchemy import create_engine
import psycopg2

DATABASE_URL = "postgresql://postgres:635343@localhost:5432/50_data"

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True
)

import io
import json
import pandas as pd
import csv
import numpy as np

def copy_cleaned_data(engine, upload_id: int, df: pd.DataFrame):
    raw_conn = engine.raw_connection()
    raw_conn.set_session(autocommit=True)
    cursor = raw_conn.cursor()

    buffer = io.StringIO()

    df = df.replace({np.nan: None})

    df["upload_id"] = upload_id

    # Safe JSON serialization (no NaN possible now)
    df["row_data"] = df.drop(columns=["upload_id"]).apply(
        lambda r: json.dumps(r.to_dict(), allow_nan=False, default=str),
        axis=1
    )

    export_df = df[["upload_id", "row_data"]]

    export_df.to_csv(
        buffer,
        index=False,
        header=False,
        quoting=csv.QUOTE_MINIMAL
    )

    buffer.seek(0)

    cursor.copy_expert(
        "COPY cleaned_data (upload_id, row_data) FROM STDIN WITH (FORMAT CSV)",
        buffer
    )

    cursor.close()
    raw_conn.close()
