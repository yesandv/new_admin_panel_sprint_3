import os

from dotenv import load_dotenv

load_dotenv()

PG_DSL = {
    "dbname": os.getenv("PG_DB"),
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASSWORD"),
    "host": os.getenv("PG_HOST", "127.0.0.1"),
    "port": os.getenv("PG_PORT", 5432),
}
PG_SCHEMA = os.getenv("PG_SCHEMA", "public")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = os.getenv("REDIS_PORT", 6379)
REDIS_KEY = os.getenv("REDIS_KEY")

ES_URL = os.getenv("ES_URL", "http://localhost:9200")
ES_INDEX = os.getenv("ES_INDEX")

RELATED_PG_TABLES = ["person", "genre"]

CHUNK_SIZE = 100
