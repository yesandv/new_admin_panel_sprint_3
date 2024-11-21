import logging

import backoff
import elastic_transport
from elasticsearch import Elasticsearch, helpers

from etl.postgres_to_es.config.settings import etl_settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ElasticsearchLoader:

    def __init__(self, host: str):
        self.es = Elasticsearch([host])

    @backoff.on_exception(
        backoff.expo,
        (
                helpers.BulkIndexError,
                elastic_transport.ConnectionError,
                elastic_transport.ConnectionTimeout,
        ),
        max_tries=5,
        jitter=backoff.full_jitter,
    )
    def load_data(
            self,
            transformed_data: list[dict],
            index_name: str,
            chunk_size: int = etl_settings.chunk_size,
    ):
        if not self.es.indices.exists(index=index_name):
            self.es.indices.create(
                index=index_name, body=etl_settings.es_schema_map[index_name]
            )
        for i in range(0, len(transformed_data), chunk_size):
            chunk = transformed_data[i: i + chunk_size]
            helpers.bulk(self.es, chunk)
            logging.info("Data loaded to Elasticsearch successfully")
