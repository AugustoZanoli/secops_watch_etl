import os

from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:admin@localhost:5432/security_watch",
)

engine = create_engine(DATABASE_URL)
