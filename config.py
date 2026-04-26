import logging
import os

from dotenv import load_dotenv

load_dotenv()

# Пути
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "cian")
USER_DATA_DIR = os.path.join(BASE_DIR, "cian/cian_profile")
AVITO_USER_DATA_DIR = os.path.join(BASE_DIR, "avito/avito_profile")

# PostgreSQL — SQLAlchemy async URL (для FastAPI и Alembic)
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+asyncpg://domq:domq_dev@localhost:5432/domq",
)

# asyncpg native URL (без +asyncpg — для DatabaseManager)
DATABASE_DSN = os.getenv(
    "DATABASE_DSN",
    "postgresql://domq:domq_dev@localhost:5432/domq",
)

os.makedirs(DATA_DIR, exist_ok=True)


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(os.path.join(DATA_DIR, "bot.log"), encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
