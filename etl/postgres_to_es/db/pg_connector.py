import backoff
import psycopg
from psycopg import InterfaceError
from psycopg.rows import Row

from etl.postgres_to_es.config.logger import logger
from etl.postgres_to_es.config.settings import etl_settings
from etl.postgres_to_es.db.base import BaseConnector


class PostgresConnector(BaseConnector):

    def __init__(
            self, dsl: dict, sequence_size: int = etl_settings.chunk_size
    ):
        self.config = dsl
        self.connection = self.connect()
        self.sequence_size = sequence_size

    @backoff.on_exception(
        backoff.expo,
        psycopg.OperationalError,
        max_tries=5,
        jitter=backoff.full_jitter,
    )
    def connect(self):
        return psycopg.connect(**self.config)

    def fetch_data(self, query: str, params: tuple) -> list[Row]:
        cursor = self.connection.cursor()
        try:
            rows = []
            cursor.execute(query, params)
            while True:
                _rows = cursor.fetchmany(size=self.sequence_size)
                if not _rows:
                    break
                rows.extend(_rows)
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in rows]
        except InterfaceError:
            logger.exception(
                "Error occurred while extracting data from the PG DB"
            )
        finally:
            cursor.close()
