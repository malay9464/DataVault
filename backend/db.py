from sqlalchemy import create_engine

DATABASE_URL = "postgresql://postgres:635343@localhost:5432/50_data"

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True
)
