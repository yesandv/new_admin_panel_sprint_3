import logging
import time

import redis
from dotenv import load_dotenv

from etl.postgres_to_es.config.settings import (
    PG_DSL,
    ES_URL,
    ES_INDEX,
    REDIS_HOST,
    REDIS_PORT,
    REDIS_KEY,
)
from etl.postgres_to_es.es_loader import ElasticsearchLoader
from etl.postgres_to_es.pg_extractor import PostgresExtractor
from etl.postgres_to_es.redis_storage import RedisStorage

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def etl_process():
    redis_storage = RedisStorage(
        redis.Redis(host=REDIS_HOST, port=REDIS_PORT), REDIS_KEY
    )
    pg_extractor = PostgresExtractor(PG_DSL)
    es_loader = ElasticsearchLoader(ES_URL, ES_INDEX)

    last_modified = redis_storage.retrieve_state()
    while True:
        data, latest_modified = pg_extractor.extract_data(last_modified)
        if not data:
            logging.info("No data to process. ETL process completed")
            time.sleep(20)
            continue
        transformed_data = pg_extractor.transform_data(data)
        es_loader.load_data(transformed_data)
        if latest_modified > last_modified:
            redis_storage.save_state(latest_modified)
            last_modified = latest_modified


if __name__ == "__main__":
    etl_process()
