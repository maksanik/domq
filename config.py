import logging
import os

# Пути
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "cian")
DB_PATH = os.path.join("db/cian_data.db")
USER_DATA_DIR = os.path.join(BASE_DIR, "cian/cian_profile")

# Создаем папку, если ее нет
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
