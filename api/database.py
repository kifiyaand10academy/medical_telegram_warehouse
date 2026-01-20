# api/database.py
"""
Database connection setup for FastAPI.

Why this matters:
- FastAPI needs a way to talk to your PostgreSQL (Neon) warehouse
- We use SQLAlchemy ORM for clean, safe queries
- Connection is loaded from .env for security
"""

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os
from dotenv import load_dotenv

# Load environment variables from .env (DB credentials, etc.)
load_dotenv()

# Build the database URL dynamically from .env
# Format: postgresql://user:password@host:port/dbname
DATABASE_URL = (
    f"postgresql://{os.getenv('postgres_user')}:{os.getenv('postgres_password')}"
    f"@{os.getenv('postgres_host')}:{os.getenv('postgres_port')}/{os.getenv('postgres_db')}"
)

# Neon.tech requires SSL — add it if needed
if "neon.tech" in os.getenv('postgres_host', ''):
    DATABASE_URL += "?sslmode=require"

# Create SQLAlchemy engine (core interface to the database)
engine = create_engine(DATABASE_URL)

# SessionLocal: factory for database sessions (used per request)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base class for ORM models (not used here since we query raw SQL)
Base = declarative_base()

def get_db():
    """
    FastAPI dependency: provides a database session for each request.
    
    How it works:
    - Called automatically by FastAPI when an endpoint needs DB access
    - Yields a session → used in endpoint → then closed (even if error occurs)
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()