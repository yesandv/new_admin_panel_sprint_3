import logging

import backoff
import elastic_transport
from elasticsearch import Elasticsearch, helpers

from etl.postgres_to_es.config.es_schema import index_schema

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ElasticsearchLoader:

    def __init__(self, host: str, index_name: str):
        self.es = Elasticsearch([host])
        self.index_name = index_name

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
    def load_data(self, transformed_data: list[dict], chunk_size: int = 100):
        if not self.es.indices.exists(index=self.index_name):
            self.es.indices.create(index=self.index_name, body=index_schema)
        for i in range(0, len(transformed_data), chunk_size):
            chunk = transformed_data[i:i + chunk_size]
            helpers.bulk(self.es, chunk)
            logging.info("Data loaded to Elasticsearch successfully")
