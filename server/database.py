import os
from sqlalchemy.ext.asyncio import create_async_engine

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://user:password@localhost:5432/testdb")

# Create async engine
# echo=True to see SQL queries in logs (useful for debugging)
engine = create_async_engine(DATABASE_URL, echo=True)
