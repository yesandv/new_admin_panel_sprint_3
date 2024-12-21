import datetime
import time

import redis

from etl.postgres_to_es.config.logger import logger
from etl.postgres_to_es.config.settings import etl_settings
from etl.postgres_to_es.data_transformer import DataTransformer
from etl.postgres_to_es.db.pg_connector import PostgresConnector
from etl.postgres_to_es.db.pg_extractor import initialize_extractors
from etl.postgres_to_es.es_loader import ElasticsearchLoader
from etl.postgres_to_es.redis_storage import RedisStorage


def etl_process():
    redis_storage = RedisStorage(
        redis.Redis(
            host=etl_settings.redis_host, port=etl_settings.redis_port
        ),
        etl_settings.redis_key,
    )
    db_connector = PostgresConnector(etl_settings.pg_dsl)
    filmwork_extractor, genre_extractor, person_extractor = (
        initialize_extractors(db_connector, etl_settings.pg_schema)
    )
    es_loader = ElasticsearchLoader(etl_settings.es_url)

    last_modified = redis_storage.retrieve_state()
    while True:
        genre_modified_records = genre_extractor.get_modified_records(
            last_modified
        )
        genre_data = genre_extractor.get_data(genre_modified_records)
        person_modified_records = person_extractor.get_modified_records(
            last_modified
        )
        person_data = person_extractor.get_data(person_modified_records)
        filmwork_modified_records = set(
            filmwork_extractor.get_modified_records(last_modified)
        )
        for table, records in zip(
                etl_settings.related_pg_tables,
                (genre_modified_records, person_modified_records),
        ):
            filmwork_modified_records.update(
                filmwork_extractor.get_related_to_film_work_ids(table, records)
            )
        filmwork_data = filmwork_extractor.get_data(
            list(filmwork_modified_records)
        )
        if not all([genre_data, person_data, filmwork_data]):
            logger.info("No data to process. ETL process completed")
            time.sleep(20)
            continue
        for index, data in zip(
                etl_settings.es_indexes,
                (filmwork_data, genre_data, person_data),
        ):
            transformed_data = DataTransformer.transform(data, index)
            es_loader.load_data(transformed_data, index)
        last_modified = datetime.datetime.now(datetime.UTC)
        redis_storage.save_state(last_modified)


if __name__ == "__main__":
    etl_process()
